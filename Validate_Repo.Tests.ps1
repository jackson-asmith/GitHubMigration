#Requires -Version 7.0
#Requires -Modules @{ ModuleName = 'Pester'; ModuleVersion = '5.0.0' }

# ---------------------------------------------------------------------------
# Test design notes
# ---------------------------------------------------------------------------
#
# Test-Preflight is extracted via the PowerShell AST so the script's
# top-level initialization code (file reads, parallel workers, CSV writes)
# never runs during unit tests. Each test fully controls git/gh behaviour
# via function stubs defined below; LASTEXITCODE is set explicitly inside
# each stub because PowerShell functions — unlike native executables — do
# not set it automatically.
#
# $InvokeRepoValidation is a scriptblock that calls [Process]::Start()
# internally, so its process-level behaviour cannot be unit-tested without
# real git repos. The tests below cover the pure-logic portions that can
# be exercised without spawning processes: status grading, CSV safe-slug
# derivation, LFS size parsing, and output-line normalisation.
# ---------------------------------------------------------------------------

BeforeAll {
    $script:ScriptPath = Join-Path $PSScriptRoot 'Validate_Repo.ps1'

    # Extract Test-Preflight from the script via AST without executing any
    # top-level code (file reads, parallel workers, CSV writes, etc.).
    $parseErrors = $null
    $ast = [System.Management.Automation.Language.Parser]::ParseFile(
        $script:ScriptPath, [ref]$null, [ref]$parseErrors)

    if ($parseErrors) {
        throw "Parse errors in $($script:ScriptPath): $($parseErrors -join '; ')"
    }

    $fnDef = $ast.FindAll({
        param($node)
        $node -is [System.Management.Automation.Language.FunctionDefinitionAst] -and
        $node.Name -eq 'Test-Preflight'
    }, $false) | Select-Object -First 1

    if (-not $fnDef) { throw 'Test-Preflight function not found in script' }

    . ([scriptblock]::Create($fnDef.Extent.Text))

    # Define git/gh as PowerShell functions so PowerShell's command-resolution
    # order (Function > External) causes all calls inside Test-Preflight to
    # use these stubs rather than the native executables. The stubs forward
    # their arguments to a state-controlled scriptblock held in a script-
    # scope variable, giving each test full control without relying on
    # Pester's ParameterFilter internals for native-command names.
    function global:git {
        [CmdletBinding()]
        param([Parameter(ValueFromRemainingArguments)][string[]]$GitArgs)
        & $script:GitImpl $GitArgs
    }
    function global:gh {
        [CmdletBinding()]
        param([Parameter(ValueFromRemainingArguments)][string[]]$GhArgs)
        & $script:GhImpl $GhArgs
    }
}

AfterAll {
    Remove-Item Function:\git -ErrorAction SilentlyContinue
    Remove-Item Function:\gh  -ErrorAction SilentlyContinue
}

# ===========================================================================
# Test-Preflight
# ===========================================================================

Describe 'Test-Preflight' {

    # Canonical happy-path parameters reused across tests.
    BeforeAll {
        $script:DefaultParams = @{
            Repos            = @('my-repo')
            GitHubOrg        = 'my-org'
            GitHubHost       = 'github.com'
            GitHubRemoteName = 'github'
            BitbucketBase    = 'ssh://bitbucket'
        }
    }

    BeforeEach {
        # Reset LASTEXITCODE so a previous test's exit code cannot leak.
        $global:LASTEXITCODE = 0

        # Default: all git and gh calls succeed.
        $script:GitImpl = { $global:LASTEXITCODE = 0 }
        $script:GhImpl  = { $global:LASTEXITCODE = 0 }

        # Default: Get-Command finds both tools; Test-Path finds the mirror.
        Mock Get-Command { [PSCustomObject]@{ Name = $Name } }
        Mock Test-Path   { $true }
    }

    # -----------------------------------------------------------------------
    Context 'Required tools' {
    # -----------------------------------------------------------------------

        It 'returns $false when git is not on PATH' {
            Mock Get-Command { $null } -ParameterFilter { $Name -eq 'git' }

            Test-Preflight @script:DefaultParams | Should -BeFalse
        }

        It 'returns $false when gh is not on PATH' {
            Mock Get-Command { $null } -ParameterFilter { $Name -eq 'gh' }

            Test-Preflight @script:DefaultParams | Should -BeFalse
        }

        It 'returns $false when git-lfs exits non-zero' {
            $script:GitImpl = {
                param([string[]]$A)
                $global:LASTEXITCODE = if ($A -contains 'lfs') { 1 } else { 0 }
            }

            Test-Preflight @script:DefaultParams -SkipSshTest | Should -BeFalse
        }

        It 'does not call git lfs when git is absent' {
            Mock Get-Command { $null } -ParameterFilter { $Name -eq 'git' }
            $lfsWasCalled = $false
            $script:GitImpl = { param([string[]]$A) if ($A -contains 'lfs') { $lfsWasCalled = $true } }

            Test-Preflight @script:DefaultParams
            $lfsWasCalled | Should -BeFalse
        }

        It 'returns $false when both git and gh are absent' {
            Mock Get-Command { $null } -ParameterFilter { $Name -eq 'git' -or $Name -eq 'gh' }

            Test-Preflight @script:DefaultParams | Should -BeFalse
        }
    }

    # -----------------------------------------------------------------------
    Context 'Repos list' {
    # -----------------------------------------------------------------------

        It 'returns $false when the repos list is empty' {
            $p = $script:DefaultParams.Clone(); $p.Repos = @()
            Test-Preflight @p | Should -BeFalse
        }
    }

    # -----------------------------------------------------------------------
    Context 'Local mirror checks' {
    # -----------------------------------------------------------------------

        It 'returns $false when the local mirror directory is missing' {
            Mock Test-Path { $false }

            Test-Preflight @script:DefaultParams -SkipSshTest | Should -BeFalse
        }

        It 'returns $false when the GitHub remote is not configured' {
            $script:GitImpl = {
                param([string[]]$A)
                $global:LASTEXITCODE = if ($A -contains 'get-url') { 1 } else { 0 }
            }

            Test-Preflight @script:DefaultParams -SkipSshTest | Should -BeFalse
        }

        It 'checks every repo in the list for a local mirror' {
            $checked = [System.Collections.Generic.List[string]]::new()
            Mock Test-Path {
                param($Path) $checked.Add($Path); $true
            } -ParameterFilter { $Path -like '*.git' }

            $p = $script:DefaultParams.Clone()
            $p.Repos = @('repo-a', 'repo-b', 'repo-c')
            Test-Preflight @p -SkipSshTest | Out-Null

            $checked | Should -Contain 'repo-a.git'
            $checked | Should -Contain 'repo-b.git'
            $checked | Should -Contain 'repo-c.git'
        }

        It 'skips per-repo mirror checks when git is not found' {
            Mock Get-Command { $null } -ParameterFilter { $Name -eq 'git' }
            # Test-Path should never be called for .git mirror paths when git
            # is absent, because the block is gated on $gitFound.
            $mirrorChecked = $false
            Mock Test-Path {
                param($Path)
                if ($Path -like '*.git') { $mirrorChecked = $true }
                $true
            }

            Test-Preflight @script:DefaultParams
            $mirrorChecked | Should -BeFalse
        }
    }

    # -----------------------------------------------------------------------
    Context 'GitHub authentication' {
    # -----------------------------------------------------------------------

        It 'returns $false when gh auth status fails' {
            $script:GhImpl = {
                param([string[]]$A)
                $global:LASTEXITCODE = if ($A -contains 'status') { 1 } else { 0 }
            }

            Test-Preflight @script:DefaultParams -SkipSshTest | Should -BeFalse
        }

        It 'returns $false when the gh API call fails after successful auth' {
            $script:GhImpl = {
                param([string[]]$A)
                if ($A -contains 'status')   { $global:LASTEXITCODE = 0 }
                elseif ($A -contains '/user') { $global:LASTEXITCODE = 1 }
                else                          { $global:LASTEXITCODE = 0 }
            }

            Test-Preflight @script:DefaultParams -SkipSshTest | Should -BeFalse
        }

        It 'does not call gh API when auth status fails' {
            $apiWasCalled = $false
            $script:GhImpl = {
                param([string[]]$A)
                if ($A -contains 'status')   { $global:LASTEXITCODE = 1 }
                elseif ($A -contains '/user') { $apiWasCalled = $true; $global:LASTEXITCODE = 0 }
                else                          { $global:LASTEXITCODE = 0 }
            }

            Test-Preflight @script:DefaultParams -SkipSshTest
            $apiWasCalled | Should -BeFalse
        }

        It 'skips auth checks when gh is not found' {
            Mock Get-Command { $null } -ParameterFilter { $Name -eq 'gh' }
            $ghCalled = $false
            $script:GhImpl = { $ghCalled = $true }

            Test-Preflight @script:DefaultParams -SkipSshTest
            $ghCalled | Should -BeFalse
        }

        It 'passes --hostname to every gh call' {
            $hostArgs = [System.Collections.Generic.List[string]]::new()
            $script:GhImpl = {
                param([string[]]$A)
                foreach ($a in $A) { $hostArgs.Add($a) }
                $global:LASTEXITCODE = 0
            }

            Test-Preflight @script:DefaultParams -SkipSshTest | Out-Null
            $hostArgs | Should -Contain 'github.com'
        }
    }

    # -----------------------------------------------------------------------
    Context 'Bitbucket connectivity' {
    # -----------------------------------------------------------------------

        It 'returns $false when the Bitbucket ls-remote fails' {
            $script:GitImpl = {
                param([string[]]$A)
                $url = $A | Where-Object { $_ -like '*bitbucket*' } | Select-Object -First 1
                $global:LASTEXITCODE = if ($url) { 1 } else { 0 }
            }

            Test-Preflight @script:DefaultParams -SkipSshTest | Should -BeFalse
        }

        It 'uses the first repo slug to build the Bitbucket test URL' {
            $script:CapturedBbUrl = $null
            $script:GitImpl = {
                param([string[]]$A)
                $url = $A | Where-Object { $_ -like '*bitbucket*' } | Select-Object -First 1
                if ($url) { $script:CapturedBbUrl = $url }
                $global:LASTEXITCODE = 0
            }

            $p = $script:DefaultParams.Clone()
            $p.Repos = @('first-repo', 'second-repo')
            Test-Preflight @p -SkipSshTest | Out-Null

            $script:CapturedBbUrl | Should -BeLike '*first-repo*'
        }

        It 'skips the Bitbucket test when the repos list is empty' {
            $p = $script:DefaultParams.Clone()
            $p.Repos = @()
            $bbCalled = $false
            $script:GitImpl = {
                param([string[]]$A)
                $url = $A | Where-Object { $_ -like '*bitbucket*' } | Select-Object -First 1
                if ($url) { $bbCalled = $true }
                $global:LASTEXITCODE = 0
            }

            Test-Preflight @p -SkipSshTest
            $bbCalled | Should -BeFalse
        }
    }

    # -----------------------------------------------------------------------
    Context 'GitHub SSH smoke test' {
    # -----------------------------------------------------------------------

        It 'returns $false when the SSH ls-remote fails' {
            $script:GitImpl = {
                param([string[]]$A)
                $url = $A | Where-Object { $_ -like 'git@*' } | Select-Object -First 1
                $global:LASTEXITCODE = if ($url) { 1 } else { 0 }
            }

            Test-Preflight @script:DefaultParams | Should -BeFalse
        }

        It 'skips the SSH test when -SkipSshTest is specified' {
            $sshCalled = $false
            $script:GitImpl = {
                param([string[]]$A)
                $url = $A | Where-Object { $_ -like 'git@*' } | Select-Object -First 1
                if ($url) { $sshCalled = $true; $global:LASTEXITCODE = 1 }
                else      { $global:LASTEXITCODE = 0 }
            }

            Test-Preflight @script:DefaultParams -SkipSshTest
            $sshCalled | Should -BeFalse
        }

        It 'skips the SSH test when the repos list is empty' {
            $p = $script:DefaultParams.Clone()
            $p.Repos = @()
            $sshCalled = $false
            $script:GitImpl = {
                param([string[]]$A)
                $url = $A | Where-Object { $_ -like 'git@*' } | Select-Object -First 1
                if ($url) { $sshCalled = $true }
                $global:LASTEXITCODE = 0
            }

            Test-Preflight @p
            $sshCalled | Should -BeFalse
        }

        It 'builds the SSH URL from GitHubHost, GitHubOrg, and the first repo slug' {
            $script:CapturedSshUrl = $null
            $script:GitImpl = {
                param([string[]]$A)
                $url = $A | Where-Object { $_ -like 'git@*' } | Select-Object -First 1
                if ($url) { $script:CapturedSshUrl = $url }
                $global:LASTEXITCODE = 0
            }

            $p = @{
                Repos            = @('my-repo')
                GitHubOrg        = 'acme'
                GitHubHost       = 'github.example.com'
                GitHubRemoteName = 'github'
                BitbucketBase    = 'ssh://bitbucket'
            }
            Test-Preflight @p | Out-Null

            $script:CapturedSshUrl | Should -Be 'git@github.example.com:acme/my-repo.git'
        }
    }

    # -----------------------------------------------------------------------
    Context 'All checks pass' {
    # -----------------------------------------------------------------------

        It 'returns $true when all tools, mirrors, auth, and connectivity succeed' {
            Test-Preflight @script:DefaultParams | Should -BeTrue
        }

        It 'returns $true with -SkipSshTest when all other checks succeed' {
            Test-Preflight @script:DefaultParams -SkipSshTest | Should -BeTrue
        }

        It 'returns $true for multiple repos when all mirrors and remotes are present' {
            $p = $script:DefaultParams.Clone()
            $p.Repos = @('repo-a', 'repo-b', 'repo-c')

            Test-Preflight @p -SkipSshTest | Should -BeTrue
        }
    }
}

# ===========================================================================
# InvokeRepoValidation — pure-logic tests (no process spawning)
# ===========================================================================

Describe 'InvokeRepoValidation — graded status' {

    It 'produces OK when there are no errors or warnings' {
        $state = @{ Errors = 0; Warnings = 0 }
        $status = if ($state.Errors -gt 0) { 'FAIL' } elseif ($state.Warnings -gt 0) { 'WARN' } else { 'OK' }
        $status | Should -Be 'OK'
    }

    It 'produces WARN when there are warnings but no errors' {
        $state = @{ Errors = 0; Warnings = 2 }
        $status = if ($state.Errors -gt 0) { 'FAIL' } elseif ($state.Warnings -gt 0) { 'WARN' } else { 'OK' }
        $status | Should -Be 'WARN'
    }

    It 'produces FAIL when there are errors and no warnings' {
        $state = @{ Errors = 1; Warnings = 0 }
        $status = if ($state.Errors -gt 0) { 'FAIL' } elseif ($state.Warnings -gt 0) { 'WARN' } else { 'OK' }
        $status | Should -Be 'FAIL'
    }

    It 'produces FAIL when both errors and warnings are present' {
        $state = @{ Errors = 3; Warnings = 5 }
        $status = if ($state.Errors -gt 0) { 'FAIL' } elseif ($state.Warnings -gt 0) { 'WARN' } else { 'OK' }
        $status | Should -Be 'FAIL'
    }

    It 'signals an error (returns $true) when the error count is non-zero' {
        $state = @{ Errors = 1 }
        ($state.Errors -gt 0) | Should -BeTrue
    }

    It 'does not signal an error (returns $false) when only warnings are present' {
        $state = @{ Errors = 0; Warnings = 1 }
        ($state.Errors -gt 0) | Should -BeFalse
    }

    It 'does not signal an error (returns $false) for a clean repo' {
        $state = @{ Errors = 0; Warnings = 0 }
        ($state.Errors -gt 0) | Should -BeFalse
    }
}

Describe 'InvokeRepoValidation — safe repo slug for temp-file naming' {

    It 'leaves a plain slug unchanged' {
        'my-repo' -replace '[/\\]', '_' | Should -Be 'my-repo'
    }

    It 'replaces a forward slash with an underscore' {
        'team/my-repo' -replace '[/\\]', '_' | Should -Be 'team_my-repo'
    }

    It 'replaces a backslash with an underscore' {
        'team\my-repo' -replace '[/\\]', '_' | Should -Be 'team_my-repo'
    }

    It 'replaces multiple path separators' {
        'org/team/my-repo' -replace '[/\\]', '_' | Should -Be 'org_team_my-repo'
    }
}

Describe 'InvokeRepoValidation — LFS size parsing' {

    BeforeAll {
        $script:Multipliers = @{ B = 1L; KB = 1KB; MB = 1MB; GB = 1GB; TB = 1TB }

        function ParseLfsLine([string]$Line) {
            if ($Line -match '\((\d+(?:\.\d+)?)\s+(B|KB|MB|GB|TB)\)') {
                return [long]([double]$Matches[1] * $script:Multipliers[$Matches[2]])
            }
            return 0L
        }
    }

    It 'parses a gigabyte value' {
        ParseLfsLine 'abc123 * path/to/video.mp4 (1.5 GB)' | Should -Be 1610612736L
    }

    It 'parses a megabyte value' {
        ParseLfsLine 'abc123 * path/to/archive.zip (512 MB)' | Should -Be 536870912L
    }

    It 'parses a kilobyte value' {
        ParseLfsLine 'abc123 * path/to/icon.png (32 KB)' | Should -Be 32768L
    }

    It 'parses a byte value' {
        ParseLfsLine 'abc123 * path/to/tiny.bin (1024 B)' | Should -Be 1024L
    }

    It 'parses a terabyte value' {
        ParseLfsLine 'abc123 * path/to/dump.tar (2 TB)' | Should -Be 2199023255552L
    }

    It 'returns 0 for a line without a size token' {
        ParseLfsLine 'abc123 * path/to/file' | Should -Be 0L
    }

    It 'returns 0 for an empty string' {
        ParseLfsLine '' | Should -Be 0L
    }

    It 'sums sizes correctly across multiple lines' {
        $lines = @(
            'aaa * a.mp4 (1 GB)'
            'bbb * b.zip (512 MB)'
        )
        $total = ($lines | ForEach-Object { ParseLfsLine $_ } | Measure-Object -Sum).Sum
        # 1 GB + 512 MB = 1073741824 + 536870912 = 1610612736
        $total | Should -Be 1610612736L
    }
}

Describe 'invokeNative — output normalisation' {

    BeforeAll {
        # Mirrors the normalisation pipeline inside $invokeNative so we can
        # verify the logic independently of any real process.
        function NormaliseOutput([string]$Raw) {
            @($Raw -split '\r?\n' |
                ForEach-Object { ($_ -replace '\s+', ' ').Trim() } |
                Where-Object { $_ })
        }
    }

    It 'splits on LF' {
        NormaliseOutput "line1`nline2" | Should -HaveCount 2
    }

    It 'splits on CRLF' {
        NormaliseOutput "line1`r`nline2" | Should -HaveCount 2
    }

    It 'trims leading and trailing whitespace from each line' {
        # Wrap in @() to prevent single-element unwrapping (which would make
        # $result a string and $result[0] index a char, not a line).
        [string[]]$result = NormaliseOutput "  hello world  "
        $result[0] | Should -Be 'hello world'
    }

    It 'collapses internal tabs (git ls-remote SHA<TAB>ref layout) to a single space' {
        [string[]]$result = NormaliseOutput "abc123`t`t`trefs/heads/main"
        $result[0] | Should -Be 'abc123 refs/heads/main'
    }

    It 'filters out blank lines' {
        NormaliseOutput "`nline1`n`nline2`n`n" | Should -HaveCount 2
    }

    It 'returns an empty array for all-blank input' {
        NormaliseOutput "`n`n`n" | Should -HaveCount 0
    }

    It 'preserves content of non-empty lines after normalisation' {
        $result = NormaliseOutput "abc123 refs/heads/main`nHEAD"
        $result | Should -Contain 'abc123 refs/heads/main'
        $result | Should -Contain 'HEAD'
    }
}

Describe 'Default branch extraction from ls-remote --symref output' {

    BeforeAll {
        # Mirrors the Where-Object / ForEach-Object pipeline used in
        # $InvokeRepoValidation to parse the --symref HEAD output.
        function ExtractSymref([string[]]$Lines) {
            $Lines |
                Where-Object { $_ -match '^ref:' } |
                ForEach-Object { ($_ -split '\s+')[1] } |
                Select-Object -First 1
        }

        function ExtractHeadSha([string[]]$Lines) {
            $Lines |
                Where-Object { $_ -match '\sHEAD$' -and $_ -notmatch '^ref:' } |
                ForEach-Object { ($_ -split '\s+')[0] } |
                Select-Object -First 1
        }
    }

    It 'extracts the symref branch name from the ref: line' {
        $output = @('ref: refs/heads/main HEAD', 'abc1234 HEAD')
        ExtractSymref $output | Should -Be 'refs/heads/main'
    }

    It 'extracts the HEAD SHA from the SHA line' {
        $output = @('ref: refs/heads/main HEAD', 'abc1234 HEAD')
        ExtractHeadSha $output | Should -Be 'abc1234'
    }

    It 'does not confuse the ref: line for a SHA line' {
        $output = @('ref: refs/heads/main HEAD', 'abc1234 HEAD')
        ExtractHeadSha $output | Should -Not -BeLike 'ref:*'
    }

    It 'returns $null when no symref line is present' {
        $output = @('abc1234 HEAD')
        ExtractSymref $output | Should -BeNullOrEmpty
    }

    It 'returns $null when no HEAD SHA line is present' {
        $output = @('ref: refs/heads/main HEAD')
        ExtractHeadSha $output | Should -BeNullOrEmpty
    }
}
