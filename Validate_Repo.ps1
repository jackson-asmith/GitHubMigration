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
    # Gated on $gitFound — without git these calls would throw command-not-found
    # errors that look like real failures rather than a missing-tool error.
    if ($gitFound) {
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
    }

    # --- GitHub auth ----------------------------------------------------------
    # --hostname targets the configured host explicitly; without it gh defaults
    # to github.com regardless of $GitHubHost.
    # Gated on $ghFound for the same reason as the git block above.
    if ($ghFound) {
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
    }

    # --- Bitbucket connectivity -----------------------------------------------
    # Smoke test against the first repo to catch SSH key or host reachability
    # issues before spawning parallel workers.
    if ($gitFound -and $Repos.Count -gt 0) {
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
    if ($gitFound -and -not $SkipSshTest -and $Repos.Count -gt 0) {
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
# workers cannot race on header creation. A prototype object drives the
# header so the column names stay in sync with the row-writing code below.
if (-not (Test-Path $CsvOut)) {
    [PSCustomObject]@{ repo = ''; status = ''; errors = 0; warnings = 0; notes = '' } |
        ConvertTo-Csv -UseQuotes AsNeeded |
        Select-Object -First 1 |
        Set-Content $CsvOut -Encoding UTF8
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
    #
    # NOTE: WorkingDirectory is not configurable here. Use -C for git or
    # equivalent flags to set the working directory per-call.
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
        # WaitForExit() with no timeout argument waits for both the process and
        # any redirected streams to fully drain before returning. We then call
        # .GetAwaiter().GetResult() rather than .Result so that task exceptions
        # are unwrapped from AggregateException and surface cleanly.
        $p.WaitForExit()
        $rawOut = $outTask.GetAwaiter().GetResult()
        $rawErr = $errTask.GetAwaiter().GetResult()
        [PSCustomObject]@{
            # Normalise whitespace centrally so all callers receive consistent
            # lines regardless of platform line endings or tab-vs-space
            # variance (e.g. the SHA<TAB>ref layout in git ls-remote output).
            Output   = @($rawOut -split '\r?\n' |
                         ForEach-Object { ($_ -replace '\s+', ' ').Trim() } |
                         Where-Object   { $_ })
            Stderr   = $rawErr.Trim()
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
        } else { $bbRefs = $r.Output | Sort-Object }

        $r = & $git ls-remote $ghUrl
        if ($r.ExitCode -ne 0) {
            & $fail "LS-REMOTE FAILED: github$(if ($r.Stderr) { ': ' + $r.Stderr })"
            $ghRefs = @()
        } else { $ghRefs = $r.Output | Sort-Object }

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

        # --- LFS detection (tiered) ----------------------------------------
        #
        # Tier A — pointer presence (definitive).
        # git lfs ls-files --all scans every ref tip in the mirror for LFS
        # pointer objects. Any output means LFS content definitely exists and
        # requires full validation. This is more reliable than inspecting
        # .gitattributes because:
        #   - .gitattributes may not exist on HEAD (only on other branches)
        #   - LFS pointers can outlive .gitattributes if tracking was removed
        #     without scrubbing historical objects
        #   - Global LFS rules (~/.gitattributes_global) aren't in the repo
        $r = & $git --git-dir "$Repo.git" lfs ls-files --all
        $hasLfsObjects = $r.ExitCode -eq 0 -and $r.Output.Count -gt 0

        # Ref list is shared by Tier B and Tier C — compute once so
        # for-each-ref is not called redundantly.
        $allBranchTagRefs = @()
        if (-not $hasLfsObjects) {
            $r = & $git --git-dir "$Repo.git" for-each-ref '--format=%(refname)' 'refs/heads' 'refs/tags'
            if ($r.ExitCode -eq 0) { $allBranchTagRefs = $r.Output }
        }

        # Tier B — config-only scan (catches "configured but empty" repos).
        # Only reached when Tier A finds nothing. Scans .gitattributes at
        # every branch and tag tip; skips refs/remotes since those duplicate
        # refs/heads content after a mirror fetch.
        # Result drives a WARN only — with no objects there is nothing to
        # validate against GitHub.
        $hasLfsConfig = $false
        if (-not $hasLfsObjects -and $allBranchTagRefs.Count -gt 0) {
            foreach ($refName in $allBranchTagRefs) {
                $ra = & $git --git-dir "$Repo.git" show "${refName}:.gitattributes"
                if ($ra.ExitCode -eq 0 -and ($ra.Output -join "`n") -match 'filter=lfs') {
                    $hasLfsConfig = $true
                    break
                }
            }
        }

        # Tier C — pointer signature scan (ref tips only, not full history).
        # Reached only when Tiers A and B both find no LFS evidence. Searches
        # blob content at every branch/tag tip for the git-lfs pointer header
        # string. Catches repos where pointer objects exist but git-lfs does
        # not recognise them — e.g. the mirror was fetched without LFS support,
        # .gitattributes was removed after pointers were committed, or there
        # is a git-lfs version mismatch.
        #
        # git grep deduplicates blobs across refs internally, so a blob
        # present on 50 branches is scanned once. -q exits on the first match
        # rather than enumerating all occurrences, bounding the cost further.
        #
        # Intentionally NOT run against full history. For a manual full-history
        # sweep of a specific repo (e.g. investigating a mysterious failure):
        #   git --git-dir=repo.git rev-list --objects --all |
        #     git cat-file --batch-check='%(objecttype) %(objectsize) %(objectname)' |
        #     awk '$1=="blob" && $2<200 {print $3}' |
        #     git --git-dir=repo.git cat-file --batch |
        #     grep 'version https://git-lfs.github.com/spec/v1'
        $hasLfsPointers = $false
        if (-not $hasLfsObjects -and -not $hasLfsConfig -and $allBranchTagRefs.Count -gt 0) {
            $grepArgs = @('--git-dir', "$Repo.git", 'grep', '-q', '--fixed-strings',
                          'version https://git-lfs.github.com/spec/v1') + $allBranchTagRefs
            $rg = & $git @grepArgs
            $hasLfsPointers = $rg.ExitCode -eq 0
        }

        if ($hasLfsObjects) {

            # WARN: LFS volume.
            # Sized against HEAD only to avoid double-counting objects that
            # appear across multiple branches. git lfs ls-files -s outputs
            # human-readable sizes, e.g.:  abc123def * path/to/file (1.5 GB)
            # The last two tokens inside the parens are <number> <unit>.
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
            #
            # Fixed parent dir groups all LFS temp clones under one path.
            # If the process is killed before 'finally' runs, leftovers are
            # easy to find and sweep with:
            #   Remove-Item -Recurse -Force "$env:TEMP/validate-repo-lfs"
            # -Force on New-Item is safe under parallel workers: creates the
            # directory if absent, silently succeeds if it already exists.
            $lfsWorkDir = [System.IO.Path]::Combine(
                [System.IO.Path]::GetTempPath(), 'validate-repo-lfs')
            New-Item -ItemType Directory -Path $lfsWorkDir -Force | Out-Null
            $lfsTmpPath = [System.IO.Path]::Combine(
                $lfsWorkDir, [System.IO.Path]::GetRandomFileName())
            New-Item -ItemType Directory -Path $lfsTmpPath -Force | Out-Null
            try {
                # --filter=blob:none: blobless clone — transfers commits and
                # trees only; regular blobs are omitted because we only need
                # to verify LFS objects, which git lfs fetch --all handles
                # separately. Dramatically reduces data transfer for large
                # repos regardless of non-LFS content volume.
                #
                # -c flags reduce noise and improve CI stability:
                #   advice.detachedHead=false — suppresses the detached HEAD warning
                #   gc.auto=0                 — disables automatic GC during clone
                #   core.longpaths=true       — safe handling of deep paths on Windows
                $cloneArgs = @(
                    '-c', 'advice.detachedHead=false',
                    '-c', 'gc.auto=0',
                    '-c', 'core.longpaths=true',
                    'clone', '--no-checkout', '--filter=blob:none',
                    $ghUrl, $lfsTmpPath
                )
                $r = & $git @cloneArgs
                if ($r.ExitCode -eq 0) {
                    # --all fetches every LFS object reachable from every ref.
                    # This is intentionally the heaviest fetch scope: migration
                    # validation must confirm GitHub holds all objects across
                    # all branches and tags, not just the default branch.
                    # To verify only the default branch (lighter, less complete):
                    #   git lfs fetch origin <default-branch>
                    $r = & $git -C $lfsTmpPath lfs fetch --all
                    if ($r.ExitCode -eq 0) {
                        $r = & $git -C $lfsTmpPath lfs fsck
                    }
                }
                if ($r.ExitCode -ne 0) { & $fail 'LFS OBJECT MISSING ON GITHUB' }
            } finally {
                Remove-Item -Recurse -Force $lfsTmpPath -ErrorAction SilentlyContinue
            }

        } elseif ($hasLfsConfig) {
            # LFS rules exist in .gitattributes on at least one branch or tag
            # but no pointer objects were found in the mirror. This is unusual
            # — either no files have been tracked yet or all objects were
            # removed from history. Nothing to validate, but worth surfacing.
            & $warn 'LFS CONFIGURED (filter=lfs in .gitattributes) BUT NO LFS OBJECTS FOUND — verify intentional'

        } elseif ($hasLfsPointers) {
            # Pointer signatures found in blob content at ref tips, but
            # git lfs ls-files --all returned nothing (Tier A) and no
            # filter=lfs rule exists in .gitattributes on any branch or tag
            # (Tier B). Likely causes:
            #   - Mirror was cloned/fetched without git-lfs installed
            #   - .gitattributes was deleted after pointer objects were committed
            #   - git-lfs version mismatch preventing pointer recognition
            # Standard LFS validation cannot proceed reliably; manual audit
            # required. See the Tier C comment above for the full history sweep.
            & $warn 'UNTRACKED LFS POINTER SIGNATURES FOUND IN BLOB CONTENT — git-lfs does not recognise these objects; manual LFS audit required'
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
    # ConvertTo-Csv handles all quoting correctly per RFC 4180: fields that
    # contain commas, double-quotes, or newlines are wrapped in double-quotes,
    # and any embedded double-quotes are escaped by doubling them. This
    # replaces the previous manual string construction which stripped quotes
    # rather than escaping them, silently destroying data.
    #
    # -UseQuotes AsNeeded (PS 7+) quotes only fields that require it, keeping
    # the output readable while remaining fully spec-compliant.
    # Select-Object -Skip 1 drops the header row — the header was written
    # once to CsvOut before workers were spawned.
    #
    # Forward slashes in the repo slug are replaced so the temp file name is
    # filesystem-safe; this does not affect the repo field written to the CSV.
    $safeRepo = $Repo -replace '[/\\]', '_'
    [PSCustomObject]@{
        repo     = $Repo
        status   = $status
        errors   = $state.Errors
        warnings = $state.Warnings
        notes    = $state.Notes -join ';'
    } | ConvertTo-Csv -UseQuotes AsNeeded |
        Select-Object -Skip 1 |
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
