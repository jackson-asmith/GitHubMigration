#!/usr/bin/env pwsh
#Requires -Version 7.0

# ==========================================================================
# Configuration
# ==========================================================================

[CmdletBinding()]
param(
    # Output CSV path. Env var CSV_OUT is honoured for CI compatibility.
    [string]$CsvOut     = ($env:CSV_OUT     ?? 'migration_validation.csv'),

    # Concurrency cap. LFS repos each perform a full clone + lfs fetch --all
    # against GitHub. At Parallelism=4, assume up to 4 simultaneous clones.
    # Reduce if GitHub rate limits or network saturation is observed.
    [ValidateRange(1, 64)]
    [int]$Parallelism   = ($env:PARALLELISM  ?? 4),

    # Repo list — one repo slug per line.
    [ValidateScript({ Test-Path $_ -PathType Leaf })]
    [string]$ReposFile  = ($env:REPOS_FILE  ?? 'repos.txt')
)

# Warn thresholds
[long]$WarnRepoSizeKiB  = 1843200       # ~1.8 GiB; GitHub hard limit is 2 GiB
[int]$WarnBranchCount   = 100
[long]$WarnLfsSizeBytes = 1073741824    # 1 GiB

# ==========================================================================
# Serial setup — must complete before workers are spawned
# ==========================================================================

# Initialise CSV header. Kept outside $InvokeRepoValidation so parallel
# workers cannot race on header creation.
if (-not (Test-Path $CsvOut)) {
    'repo,status,errors,warnings,notes' | Set-Content $CsvOut -Encoding UTF8
}

# ==========================================================================
# $InvokeRepoValidation
# ==========================================================================

# Defined as a scriptblock so ForEach-Object -Parallel can receive it via
# $using:. PowerShell runspaces do not inherit the caller's function table,
# but they can receive and invoke a scriptblock reference.

$InvokeRepoValidation = {
    param(
        [string]$Repo,
        [string]$CsvOut,
        [long]$WarnRepoSizeKiB,
        [int]$WarnBranchCount,
        [long]$WarnLfsSizeBytes
    )

    # State shared between helpers. A hashtable is used rather than plain
    # ints so that mutations inside closures (child scopes) are visible here.
    $state = @{
        Errors   = 0
        Warnings = 0
        Notes    = [System.Collections.Generic.List[string]]::new()
    }

    # --- Helpers -----------------------------------------------------------
    # .GetNewClosure() captures $state and $Repo by reference so mutations
    # inside each helper are reflected in the calling scope.

    $fail = {
        param([string]$Msg)
        Write-Host "  FAIL [$Repo]: $Msg"
        $state.Notes.Add("FAIL: $Msg")
        $state.Errors++
    }.GetNewClosure()

    $warn = {
        param([string]$Msg)
        Write-Host "  WARN [$Repo]: $Msg"
        $state.Notes.Add("WARN: $Msg")
        $state.Warnings++
    }.GetNewClosure()

    # do/while($false) lets any check call `break` to skip remaining checks
    # while still falling through to the CSV write below. This ensures every
    # repo always gets a row in the output, even on early failures.
    do {

        # --- Sync mirror ---------------------------------------------------
        # Fetch and prune before validating so stale, deleted, and renamed
        # refs are reflected in the local mirror prior to any diff.
        git --git-dir="$Repo.git" fetch --prune github
        if ($LASTEXITCODE -ne 0) { & $fail 'FETCH FAILED'; break }

        # --- Ref integrity -------------------------------------------------
        # Diff every ref/SHA pair between Bitbucket and GitHub. A clean diff
        # guarantees identical commit graphs.
        $bbRefs = git ls-remote "ssh://bitbucket/$Repo" | Sort-Object
        if ($LASTEXITCODE -ne 0) { & $fail 'LS-REMOTE FAILED: bitbucket'; $bbRefs = @() }
        $ghRefs = git ls-remote "git@github.com:org/$Repo.git" | Sort-Object
        if ($LASTEXITCODE -ne 0) { & $fail 'LS-REMOTE FAILED: github'; $ghRefs = @() }

        if ($bbRefs -and $ghRefs -and (Compare-Object $bbRefs $ghRefs)) {
            & $fail 'REF MISMATCH'
        }

        # Belt-and-suspenders: confirm every Bitbucket SHA exists as an object
        # in the local mirror. Catches partial pushes where refs were written
        # but objects were not fully transferred.
        if ($bbRefs) {
            $bbRefs | ForEach-Object {
                $parts = $_ -split '\s+', 2
                $sha   = $parts[0]
                $ref   = if ($parts.Count -gt 1) { $parts[1] } else { '' }
                git --git-dir="$Repo.git" cat-file -e $sha 2>$null
                if ($LASTEXITCODE -ne 0) {
                    & $fail "MISSING OBJECT: $sha $ref"
                }
            }
        }

        # --- Default branch ------------------------------------------------
        # Use --symref to extract Bitbucket's HEAD symref reliably.
        $bbDefault = git ls-remote --symref "ssh://bitbucket/$Repo" HEAD |
            Where-Object { $_ -match '^ref:' } |
            ForEach-Object { ($_ -split '\s+')[1] } |
            Select-Object -First 1
        if ($LASTEXITCODE -ne 0) { & $fail 'LS-REMOTE --SYMREF FAILED: bitbucket'; $bbDefault = $null }

        $ghDefault = gh api "/repos/org/$Repo" --jq '.default_branch'
        if ($LASTEXITCODE -ne 0) { & $fail 'GH API FAILED: default_branch'; $ghDefault = $null }

        # Name and SHA are checked independently for specific error messages.
        if ($bbDefault -and $ghDefault -and $bbDefault -ne "refs/heads/$ghDefault") {
            & $fail "DEFAULT BRANCH NAME MISMATCH: bb=$bbDefault gh=$ghDefault"
        }

        $bbHead = git ls-remote "ssh://bitbucket/$Repo" HEAD |
            Where-Object { $_ -match '\sHEAD$' } |
            ForEach-Object { ($_ -split '\s+')[0] } |
            Select-Object -First 1
        if ($LASTEXITCODE -ne 0) { & $fail 'LS-REMOTE HEAD FAILED: bitbucket'; $bbHead = $null }

        $ghHead = git ls-remote "git@github.com:org/$Repo.git" HEAD |
            Where-Object { $_ -match '\sHEAD$' } |
            ForEach-Object { ($_ -split '\s+')[0] } |
            Select-Object -First 1
        if ($LASTEXITCODE -ne 0) { & $fail 'LS-REMOTE HEAD FAILED: github'; $ghHead = $null }

        if ($bbHead -and $ghHead -and $bbHead -ne $ghHead) {
            & $fail "HEAD SHA MISMATCH: bb=$bbHead gh=$ghHead"
        }

        # --- WARN: repo size -----------------------------------------------
        # count-objects -v emits raw KiB; keep values arithmetic-safe.
        $repoSizeKiB = git --git-dir="$Repo.git" count-objects -v |
            Where-Object { $_ -match '^size-pack:\s+(\d+)' } |
            ForEach-Object { [long]$Matches[1] } |
            Select-Object -First 1

        if ($LASTEXITCODE -eq 0 -and $repoSizeKiB -gt $WarnRepoSizeKiB) {
            & $warn "REPO SIZE NEAR 2 GiB LIMIT (${repoSizeKiB} KiB)"
        }

        # --- WARN: branch count --------------------------------------------
        $branchCount = (
            git --git-dir="$Repo.git" branch -r | Measure-Object -Line
        ).Lines

        if ($LASTEXITCODE -eq 0 -and $branchCount -gt $WarnBranchCount) {
            & $warn "HIGH BRANCH COUNT ($branchCount)"
        }

        # --- LFS -----------------------------------------------------------
        $gitAttributes = git --git-dir="$Repo.git" show 'HEAD:.gitattributes' 2>$null
        if ($LASTEXITCODE -eq 0 -and $gitAttributes -match 'filter=lfs') {

            # WARN: LFS volume
            # git lfs ls-files -s outputs human-readable sizes, e.g.:
            #   abc123def * path/to/file (1.5 GB)
            # The last two tokens inside the parens are <number> <unit>, so we
            # parse both and convert to bytes for an accurate sum.
            # NOTE: the original bash script used awk '{sum += $NF}' which always
            # produced 0 because $NF captures the unit suffix ("GB)", "MB)", etc.),
            # not a number — meaning the LARGE LFS VOLUME warning never fired.
            $lfsSizeLines = git --git-dir="$Repo.git" lfs ls-files -s 2>$null
            $lfsSize = if ($LASTEXITCODE -eq 0 -and $lfsSizeLines) {
                $multipliers = @{ B = 1L; KB = 1KB; MB = 1MB; GB = 1GB; TB = 1TB }
                ($lfsSizeLines | ForEach-Object {
                    if ($_ -match '\((\d+(?:\.\d+)?)\s+(B|KB|MB|GB|TB)\)') {
                        [long]([double]$Matches[1] * $multipliers[$Matches[2]])
                    } else { 0L }
                } | Measure-Object -Sum).Sum
            } else { 0L }

            if ($lfsSize -gt $WarnLfsSizeBytes) {
                & $warn "LARGE LFS VOLUME (${lfsSize} bytes)"
            }

            # Validate pointer file structure against the local mirror.
            # --pointers checks pointer hashes are well-formed but does NOT
            # verify that LFS objects exist on GitHub or that historical
            # objects on all refs are present.
            git --git-dir="$Repo.git" lfs fsck --pointers
            if ($LASTEXITCODE -ne 0) {
                & $fail 'LFS POINTER INTEGRITY FAILURE'
            }

            # Verify LFS objects exist on GitHub across all historical refs.
            # lfs fsck (no --pointers) forces pointer->object resolution
            # against the remote. --git-dir is insufficient for LFS object
            # resolution so a real clone is required.
            $lfsTmp = New-Item @{
                ItemType = 'Directory'
                Path     = [System.IO.Path]::GetTempPath()
                Name     = [System.IO.Path]::GetRandomFileName()
                Force    = $true
            }

            try {
                git clone --no-checkout "git@github.com:org/$Repo.git" $lfsTmp.FullName
                if ($LASTEXITCODE -eq 0) {
                    git -C $lfsTmp.FullName lfs fetch --all
                    if ($LASTEXITCODE -eq 0) {
                        git -C $lfsTmp.FullName lfs fsck
                    }
                }
                if ($LASTEXITCODE -ne 0) {
                    & $fail 'LFS OBJECT MISSING ON GITHUB'
                }
            } finally {
                Remove-Item -Recurse -Force $lfsTmp.FullName -ErrorAction SilentlyContinue
            }
        }

    } while ($false)

    # --- Graded result -----------------------------------------------------
    $status = if ($state.Errors -gt 0) { 'FAIL' }
              elseif ($state.Warnings -gt 0) { 'WARN' }
              else { 'OK' }

    Write-Host "${status}: $Repo (errors=$($state.Errors) warnings=$($state.Warnings))"

    # --- CSV output --------------------------------------------------------
    # Each worker writes to a per-repo temp file rather than appending
    # directly to CsvOut. Parallel Add-Content calls are not atomic and will
    # corrupt rows. The main process merges temp files after
    # ForEach-Object -Parallel completes.
    #
    # Forward slashes in the repo slug are replaced so the name is
    # filesystem-safe. Embedded double quotes are stripped from notes to
    # prevent CSV parser breakage. Notes are machine-generated so this
    # should be a no-op.
    $notesJoined = ($state.Notes -join ';') -replace '"', ''
    $safeRepo    = $Repo -replace '/', '_'
    "$Repo,$status,$($state.Errors),$($state.Warnings),`"$notesJoined`"" |
        Set-Content "${CsvOut}.${safeRepo}.tmp" -Encoding UTF8

    # WARNs are non-blocking; only FAILs produce a non-zero exit signal.
    return ($state.Errors -gt 0)
}

# ==========================================================================
# Parallel execution
# ==========================================================================

Write-Verbose "Starting validation (parallelism=$Parallelism) ..."

$repos = Get-Content $ReposFile | Where-Object { $_.Trim() -ne '' }

# ForEach-Object -Parallel spawns up to ThrottleLimit runspaces concurrently.
# $using: is required to pass outer-scope values into each runspace.
# The scriptblock body is passed by reference to avoid duplicating it inline.
$results = $repos | ForEach-Object -Parallel {
    $fn     = $using:InvokeRepoValidation
    $params = @{
        Repo             = $_
        CsvOut           = $using:CsvOut
        WarnRepoSizeKiB  = $using:WarnRepoSizeKiB
        WarnBranchCount  = $using:WarnBranchCount
        WarnLfsSizeBytes = $using:WarnLfsSizeBytes
    }
    & $fn @params
} -ThrottleLimit $Parallelism

$anyFailed = $results -contains $true

# ==========================================================================
# Post-run CSV merge
# ==========================================================================

# Merge per-repo temp files into the output CSV in sorted order, then
# clean up. Sorting here gives stable output regardless of execution order.
$csvDir  = Split-Path $CsvOut -Parent
$csvName = Split-Path $CsvOut -Leaf
if (-not $csvDir) { $csvDir = '.' }

Get-ChildItem -Path $csvDir -Filter "${csvName}.*.tmp" -ErrorAction SilentlyContinue |
    Sort-Object Name |
    ForEach-Object {
        Get-Content $_.FullName | Add-Content $CsvOut -Encoding UTF8
        Remove-Item $_.FullName
    }

Write-Verbose "Done. Results written to $CsvOut"

# Surface exit code so callers and CI pipelines see failures.
# Use the CSV to count and triage individual failures.
exit ($anyFailed ? 1 : 0)
