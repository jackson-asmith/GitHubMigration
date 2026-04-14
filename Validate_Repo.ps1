#!/usr/bin/env pwsh
#Requires -Version 7.0

# ==========================================================================
# Configuration
# ==========================================================================

[CmdletBinding()]
param(
    # Output CSV path. Env var CSV_OUT is honoured for CI compatibility.
    [string]$CsvOut           = ($env:CSV_OUT            ?? 'migration_validation.csv'),

    # Concurrency cap. LFS repos each perform a full clone + lfs fetch --all
    # against GitHub. At Parallelism=4, assume up to 4 simultaneous clones.
    # Reduce if GitHub rate limits or network saturation is observed.
    [ValidateRange(1, 64)]
    [int]$Parallelism         = ($env:PARALLELISM         ?? 4),

    # Repo list — one repo slug per line.
    [ValidateScript({ Test-Path $_ -PathType Leaf })]
    [string]$ReposFile        = ($env:REPOS_FILE         ?? 'repos.txt'),

    [string]$GitHubOrg        = ($env:GITHUB_ORG         ?? 'org'),
    [string]$GitHubHost       = ($env:GITHUB_HOST        ?? 'github.com'),
    [string]$GitHubRemoteName = ($env:GITHUB_REMOTE_NAME ?? 'github'),
    [string]$BitbucketBase    = ($env:BITBUCKET_BASE     ?? 'ssh://bitbucket'),

    # Pass -SkipSshTest to omit the SSH connectivity smoke test in preflight.
    [switch]$SkipSshTest
)

# Warn thresholds
[long]$WarnRepoSizeKiB  = 1843200       # ~1.8 GiB; GitHub hard limit is 2 GiB
[int]$WarnBranchCount   = 100
[long]$WarnLfsSizeBytes = 1073741824    # 1 GiB

# ==========================================================================
# Preflight
# ==========================================================================

function Test-Preflight {
    [CmdletBinding()]
    param(
        [string[]]$Repos,
        [string]$GitHubOrg,
        [string]$GitHubHost,
        [string]$GitHubRemoteName,
        [string]$BitbucketBase,
        [switch]$SkipSshTest
    )

    $ok = $true

    # --- Required tools -------------------------------------------------------
    $gitFound = [bool](Get-Command 'git' -ErrorAction SilentlyContinue)
    $ghFound  = [bool](Get-Command 'gh'  -ErrorAction SilentlyContinue)

    if (-not $gitFound) { Write-Error 'git not found in PATH'; $ok = $false }
    if (-not $ghFound)  { Write-Error 'gh not found in PATH';  $ok = $false }

    # Gate the lfs check on git being present; without git the command would
    # throw a terminating command-not-found error and skip all further checks.
    if ($gitFound) {
        git lfs version 2>&1 | Out-Null
        if ($LASTEXITCODE -ne 0) {
            Write-Error 'git-lfs not available — install it or ensure it is on PATH'
            $ok = $false
        }
    }

    # --- Repos list -----------------------------------------------------------
    # Slugs may be bare names (my-repo) or single-depth paths (team/my-repo).
    # A slug containing / implies a subdirectory layout for local mirrors
    # (e.g. team/my-repo.git). Ensure your mirror root matches this structure.
    if ($Repos.Count -eq 0) {
        Write-Error 'Repos file is empty — nothing to validate'
        $ok = $false
    }

    # --- Per-repo local checks ------------------------------------------------
    foreach ($repo in $Repos) {
        if (-not (Test-Path "$repo.git" -PathType Container)) {
            Write-Error "Local mirror not found: $repo.git"
            $ok = $false
            continue
        }
        git --git-dir="$repo.git" remote get-url $GitHubRemoteName 2>&1 | Out-Null
        if ($LASTEXITCODE -ne 0) {
            Write-Error "Remote '$GitHubRemoteName' not configured in $repo.git"
            $ok = $false
        }
    }

    # --- GitHub auth ----------------------------------------------------------
    # --hostname targets the configured host explicitly; without it gh defaults
    # to github.com regardless of $GitHubHost.
    gh auth status --hostname $GitHubHost 2>&1 | Out-Null
    if ($LASTEXITCODE -ne 0) {
        Write-Error "gh is not authenticated for $GitHubHost — run 'gh auth login --hostname $GitHubHost'"
        $ok = $false
    } else {
        # Cheap API call confirms the token is valid and the host is reachable.
        gh api --hostname $GitHubHost '/user' 2>&1 | Out-Null
        if ($LASTEXITCODE -ne 0) {
            Write-Error "gh API call failed for $GitHubHost — check token scope and network connectivity"
            $ok = $false
        }
    }

    # --- Bitbucket connectivity -----------------------------------------------
    # Smoke test against the first repo to catch SSH key or host reachability
    # issues before spawning parallel workers.
    if ($Repos.Count -gt 0) {
        $bbTestUrl = "$BitbucketBase/$($Repos[0])"
        Write-Verbose "Bitbucket connectivity test: $bbTestUrl"
        git ls-remote $bbTestUrl HEAD 2>&1 | Out-Null
        if ($LASTEXITCODE -ne 0) {
            Write-Error "Cannot reach Bitbucket at $bbTestUrl — check SSH key and access"
            $ok = $false
        }
    }

    # --- Optional GitHub SSH smoke test ---------------------------------------
    # Runs against the first repo; if SSH works for one it works for all.
    if (-not $SkipSshTest -and $Repos.Count -gt 0) {
        $testUrl = "git@${GitHubHost}:$GitHubOrg/$($Repos[0]).git"
        Write-Verbose "GitHub SSH smoke test: $testUrl"
        git ls-remote $testUrl HEAD 2>&1 | Out-Null
        if ($LASTEXITCODE -ne 0) {
            Write-Error "SSH smoke test failed for $testUrl — check SSH key and repo access"
            $ok = $false
        }
    }

    return $ok
}

# ==========================================================================
# Serial setup — must complete before workers are spawned
# ==========================================================================

# Write-Information (stream 6) is buffered per-runspace in
# ForEach-Object -Parallel, so per-repo output appears as a clean block
# rather than interleaved across workers. PS7 parallel runspaces inherit
# preference variables from the calling scope, so this setting propagates.
$InformationPreference = 'Continue'

$repos = Get-Content $ReposFile | ForEach-Object { $_.Trim() } | Where-Object { $_ }

Write-Verbose 'Running preflight checks ...'
$preflightParams = @{
    Repos            = $repos
    GitHubOrg        = $GitHubOrg
    GitHubHost       = $GitHubHost
    GitHubRemoteName = $GitHubRemoteName
    BitbucketBase    = $BitbucketBase
    SkipSshTest      = $SkipSshTest
}
if (-not (Test-Preflight @preflightParams)) {
    Write-Error 'Preflight failed — resolve the above errors before running validation.'
    exit 1
}

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
        [string]$GitHubOrg,
        [string]$GitHubHost,
        [string]$GitHubRemoteName,
        [string]$BitbucketBase,
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
        Write-Information "  FAIL [$Repo]: $Msg"
        $state.Notes.Add("FAIL: $Msg")
        $state.Errors++
    }.GetNewClosure()

    $warn = {
        param([string]$Msg)
        Write-Information "  WARN [$Repo]: $Msg"
        $state.Notes.Add("WARN: $Msg")
        $state.Warnings++
    }.GetNewClosure()

    # Shared process helper — captures stdout and stderr into separate strings
    # via async reads on both streams. The async pattern is required to prevent
    # deadlocks: if we read one stream synchronously while the process is
    # blocked waiting for the other stream's buffer to drain, we hang.
    # ProcessStartInfo.ArgumentList (available in .NET 5+ / PS7) handles
    # argument quoting correctly without manual escaping.
    $invokeNative = {
        param([string]$Exe, [string[]]$Arguments)
        $psi = [System.Diagnostics.ProcessStartInfo]@{
            FileName               = $Exe
            RedirectStandardOutput = $true
            RedirectStandardError  = $true
            UseShellExecute        = $false
        }
        foreach ($a in $Arguments) { $psi.ArgumentList.Add($a) }
        $p       = [System.Diagnostics.Process]::Start($psi)
        $outTask = $p.StandardOutput.ReadToEndAsync()
        $errTask = $p.StandardError.ReadToEndAsync()
        $p.WaitForExit()
        [PSCustomObject]@{
            Output   = @($outTask.Result -split '\r?\n' | Where-Object { $_ })
            Stderr   = $errTask.Result.Trim()
            ExitCode = $p.ExitCode
        }
    }

    # $git and $gh are thin closures over $invokeNative. $gh also captures
    # $GitHubHost to inject --hostname on every call, ensuring we always
    # target the configured host rather than gh's default of github.com.
    $git = { & $invokeNative 'git' $args }.GetNewClosure()
    $gh  = { & $invokeNative 'gh' (@('--hostname', $GitHubHost) + $args) }.GetNewClosure()

    # Convenience URL locals — avoids repeating construction throughout.
    $bbUrl = "$BitbucketBase/$Repo"
    $ghUrl = "git@${GitHubHost}:$GitHubOrg/$Repo.git"

    # do/while($false) lets any check call `break` to skip remaining checks
    # while still falling through to the CSV write below. This ensures every
    # repo always gets a row in the output, even on early failures.
    do {

        # --- Sync mirror ---------------------------------------------------
        # Fetch and prune before validating so stale, deleted, and renamed
        # refs are reflected in the local mirror prior to any diff.
        $r = & $git --git-dir "$Repo.git" fetch --prune $GitHubRemoteName
        if ($r.ExitCode -ne 0) { & $fail "FETCH FAILED$(if ($r.Stderr) { ': ' + $r.Stderr })"; break }

        # --- Ref integrity -------------------------------------------------
        # Diff every ref/SHA pair between Bitbucket and GitHub. A clean diff
        # guarantees identical commit graphs.
        $r = & $git ls-remote $bbUrl
        if ($r.ExitCode -ne 0) {
            & $fail "LS-REMOTE FAILED: bitbucket$(if ($r.Stderr) { ': ' + $r.Stderr })"
            $bbRefs = @()
        } else { $bbRefs = $r.Output | ForEach-Object { ($_ -replace '\s+', ' ').Trim() } | Where-Object { $_ } | Sort-Object }

        $r = & $git ls-remote $ghUrl
        if ($r.ExitCode -ne 0) {
            & $fail "LS-REMOTE FAILED: github$(if ($r.Stderr) { ': ' + $r.Stderr })"
            $ghRefs = @()
        } else { $ghRefs = $r.Output | ForEach-Object { ($_ -replace '\s+', ' ').Trim() } | Where-Object { $_ } | Sort-Object }

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
                $r = & $git --git-dir "$Repo.git" cat-file -e $sha
                if ($r.ExitCode -ne 0) { & $fail "MISSING OBJECT: $sha $ref" }
            }
        }

        # --- Default branch ------------------------------------------------
        # --symref returns both the symref line (for branch name) and the SHA
        # line (for HEAD comparison) in a single call, saving a round trip vs.
        # a separate ls-remote for bbHead.
        $r = & $git ls-remote --symref $bbUrl HEAD
        if ($r.ExitCode -ne 0) {
            & $fail "LS-REMOTE --SYMREF FAILED: bitbucket$(if ($r.Stderr) { ': ' + $r.Stderr })"
            $bbDefault = $null
            $bbHead    = $null
        } else {
            $bbDefault = $r.Output |
                Where-Object { $_ -match '^ref:' } |
                ForEach-Object { ($_ -split '\s+')[1] } |
                Select-Object -First 1
            # Reuse the --symref output for HEAD SHA — no separate ls-remote needed.
            $bbHead = $r.Output |
                Where-Object { $_ -match '\sHEAD$' -and $_ -notmatch '^ref:' } |
                ForEach-Object { ($_ -split '\s+')[0] } |
                Select-Object -First 1
        }

        $r = & $gh api "/repos/$GitHubOrg/$Repo" --jq '.default_branch'
        if ($r.ExitCode -ne 0) {
            & $fail "GH API FAILED: default_branch$(if ($r.Stderr) { ': ' + $r.Stderr })"
            $ghDefault = $null
        } else { $ghDefault = $r.Output | Select-Object -First 1 }

        # Name and SHA are checked independently for specific error messages.
        if ($bbDefault -and $ghDefault -and $bbDefault -ne "refs/heads/$ghDefault") {
            & $fail "DEFAULT BRANCH NAME MISMATCH: bb=$bbDefault gh=$ghDefault"
        }

        # GitHub HEAD SHA extracted from the already-fetched $ghRefs —
        # no additional ls-remote call needed.
        $ghHead = $ghRefs |
            Where-Object { $_ -match '\sHEAD$' } |
            ForEach-Object { ($_ -split '\s+')[0] } |
            Select-Object -First 1

        if ($bbHead -and $ghHead -and $bbHead -ne $ghHead) {
            & $fail "HEAD SHA MISMATCH: bb=$bbHead gh=$ghHead"
        }

        # --- WARN: repo size -----------------------------------------------
        # count-objects -v emits raw KiB; keep values arithmetic-safe.
        $r = & $git --git-dir "$Repo.git" count-objects -v
        if ($r.ExitCode -eq 0) {
            $repoSizeKiB = $r.Output |
                Where-Object { $_ -match '^size-pack:\s+(\d+)' } |
                ForEach-Object { [long]$Matches[1] } |
                Select-Object -First 1
            if ($repoSizeKiB -gt $WarnRepoSizeKiB) {
                & $warn "REPO SIZE NEAR 2 GiB LIMIT (${repoSizeKiB} KiB)"
            }
        }

        # --- WARN: branch count --------------------------------------------
        $r = & $git --git-dir "$Repo.git" branch -r
        if ($r.ExitCode -eq 0) {
            $branchCount = ($r.Output |
                Where-Object { $_ -notmatch '->' } |
                Measure-Object -Line).Lines
            if ($branchCount -gt $WarnBranchCount) {
                & $warn "HIGH BRANCH COUNT ($branchCount)"
            }
        }

        # --- LFS -----------------------------------------------------------
        $r = & $git --git-dir "$Repo.git" show HEAD:.gitattributes
        if ($r.ExitCode -eq 0 -and ($r.Output -join "`n") -match 'filter=lfs') {

            # WARN: LFS volume.
            # git lfs ls-files -s outputs human-readable sizes, e.g.:
            #   abc123def * path/to/file (1.5 GB)
            # The last two tokens inside the parens are <number> <unit>, so
            # we parse both and convert to bytes for an accurate sum.
            # NOTE: the original bash script used awk '{sum += $NF}' which
            # always produced 0 because $NF captures the unit suffix
            # ("GB)", "MB)", etc.) — meaning the warning never fired.
            $r = & $git --git-dir "$Repo.git" lfs ls-files -s
            $lfsSize = if ($r.ExitCode -eq 0 -and $r.Output) {
                $multipliers = @{ B = 1L; KB = 1KB; MB = 1MB; GB = 1GB; TB = 1TB }
                ($r.Output | ForEach-Object {
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
            $r = & $git --git-dir "$Repo.git" lfs fsck --pointers
            if ($r.ExitCode -ne 0) { & $fail 'LFS POINTER INTEGRITY FAILURE' }

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
                $r = & $git clone --no-checkout $ghUrl $lfsTmp.FullName
                if ($r.ExitCode -eq 0) {
                    $r = & $git -C $lfsTmp.FullName lfs fetch --all
                    if ($r.ExitCode -eq 0) {
                        $r = & $git -C $lfsTmp.FullName lfs fsck
                    }
                }
                if ($r.ExitCode -ne 0) { & $fail 'LFS OBJECT MISSING ON GITHUB' }
            } finally {
                Remove-Item -Recurse -Force $lfsTmp.FullName -ErrorAction SilentlyContinue
            }
        }

    } while ($false)

    # --- Graded result -----------------------------------------------------
    $status = if ($state.Errors -gt 0) { 'FAIL' }
              elseif ($state.Warnings -gt 0) { 'WARN' }
              else { 'OK' }

    Write-Information "${status}: $Repo (errors=$($state.Errors) warnings=$($state.Warnings))"

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

# ForEach-Object -Parallel spawns up to ThrottleLimit runspaces concurrently.
# $using: is required to pass outer-scope values into each runspace.
# The scriptblock body is passed by reference to avoid duplicating it inline.
$results = $repos | ForEach-Object -Parallel {
    $fn     = $using:InvokeRepoValidation
    $params = @{
        Repo              = $_
        CsvOut            = $using:CsvOut
        GitHubOrg         = $using:GitHubOrg
        GitHubHost        = $using:GitHubHost
        GitHubRemoteName  = $using:GitHubRemoteName
        BitbucketBase     = $using:BitbucketBase
        WarnRepoSizeKiB   = $using:WarnRepoSizeKiB
        WarnBranchCount   = $using:WarnBranchCount
        WarnLfsSizeBytes  = $using:WarnLfsSizeBytes
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
