# GitHubMigration — Repo Validation

Validates that repositories have been migrated correctly from Bitbucket to
GitHub. For each repo the scripts verify ref integrity, default branch
alignment, HEAD SHA parity, and LFS object presence, then write a graded
CSV report (`OK` / `WARN` / `FAIL`) suitable for CI artefact collection or
manual triage.

Two implementations are provided targeting different environments:

| Script | Platform | Shell |
|---|---|---|
| `Validate_Repo.ps1` | Cross-platform | PowerShell 7+ |
| `Validate_Repo.sh` | Linux / macOS | Bash |

`Validate_Repo.ps1` is the primary script: it includes preflight checks,
configurable parameters, richer LFS detection, and a Pester test suite.
`Validate_Repo.sh` is a simpler Bash equivalent for environments where
PowerShell is unavailable.

---

## Prerequisites

| Tool | Minimum version | Purpose |
|---|---|---|
| `git` | 2.38+ | All git operations |
| `git-lfs` | 3.0+ | LFS detection and validation |
| `gh` (GitHub CLI) | 2.0+ | API calls and auth checks |
| PowerShell | 7.0+ | `.ps1` script only |
| Pester | 5.0+ | Running the test suite |

Authenticate the GitHub CLI before running:

```sh
gh auth login --hostname <your-github-host>
```

---

## Setup

### 1. Repo list

Create a `repos.txt` file with one repo slug per line. Slugs may be bare
names or single-depth `team/repo` paths:

```
my-service
platform/auth-api
platform/billing-service
```

### 2. Local mirrors

Each repo must have a bare local mirror alongside the script, named
`<slug>.git`. The mirror must have a remote named `github` (configurable)
pointing at the GitHub destination:

```sh
# Example: create a mirror from Bitbucket and add the GitHub remote
git clone --mirror ssh://bitbucket/my-service my-service.git
git -C my-service.git remote add github git@github.com:<org>/my-service.git
```

---

## Usage

### PowerShell

```powershell
./Validate_Repo.ps1
```

By default this writes a timestamped CSV (`migration_validation_<timestamp>.csv`)
and runs 4 repos in parallel. All defaults can be overridden with parameters
or environment variables:

```powershell
./Validate_Repo.ps1 `
    -ReposFile    repos.txt `
    -CsvOut       results.csv `
    -Parallelism  8 `
    -GitHubOrg    my-org `
    -GitHubHost   github.example.com `
    -GitHubRemoteName github `
    -BitbucketBase    ssh://git@bitbucket.example.com `
    -SkipSshTest
```

All parameters can also be set via environment variables:

| Parameter | Environment variable | Default |
|---|---|---|
| `-ReposFile` | `REPOS_FILE` | `repos.txt` |
| `-CsvOut` | `CSV_OUT` | `migration_validation_<timestamp>.csv` |
| `-Parallelism` | `PARALLELISM` | `4` |
| `-GitHubOrg` | `GITHUB_ORG` | `org` |
| `-GitHubHost` | `GITHUB_HOST` | `github.com` |
| `-GitHubRemoteName` | `GITHUB_REMOTE_NAME` | `github` |
| `-BitbucketBase` | `BITBUCKET_BASE` | `ssh://bitbucket` |

### Bash

```sh
./Validate_Repo.sh
```

Configuration is via environment variables only:

| Variable | Default |
|---|---|
| `REPOS_FILE` | `repos.txt` |
| `CSV_OUT` | `migration_validation.csv` |
| `PARALLELISM` | `4` |

> **Note:** The Bash script has hardcoded values for the GitHub org (`org`),
> host (`github.com`), and Bitbucket base URL (`ssh://bitbucket`). Edit the
> top of the script to match your environment before running.

---

## What is checked

### Preflight (PowerShell only)

Before spawning any workers the script verifies:

- `git`, `git-lfs`, and `gh` are on PATH
- Every repo in `repos.txt` has a local mirror directory
- Each mirror has the expected GitHub remote configured
- `gh` is authenticated for the target host
- Bitbucket is reachable via the first repo slug
- GitHub SSH access works (skippable with `-SkipSshTest`)

### Per-repo checks

| Check | Outcome on failure |
|---|---|
| Fetch mirror from GitHub | `FAIL` — remaining checks skipped |
| Ref list matches between Bitbucket and GitHub | `FAIL` |
| Every Bitbucket SHA present as a local object | `FAIL` |
| Default branch name matches | `FAIL` |
| HEAD SHA matches | `FAIL` |
| Repo size approaching GitHub 2 GiB limit (>1.8 GiB) | `WARN` |
| High branch count (>100) | `WARN` |
| LFS pointer integrity (local mirror) | `FAIL` |
| LFS objects present on GitHub across all refs | `FAIL` |
| LFS configured but no objects found | `WARN` |
| Unrecognised LFS pointer signatures in blob content | `WARN` |

### LFS detection (PowerShell)

Detection is tiered to handle repos where git-lfs state is inconsistent:

1. **Tier A** — `git lfs ls-files --all` finds pointer objects (definitive; triggers full LFS validation)
2. **Tier B** — `.gitattributes` contains `filter=lfs` on any branch or tag (warns; nothing to validate against GitHub)
3. **Tier C** — LFS pointer header strings found in blob content via `git grep` (warns; manual audit required)

---

## Output

### CSV columns

| Column | Description |
|---|---|
| `repo` | Repo slug from `repos.txt` |
| `status` | `OK`, `WARN`, or `FAIL` |
| `errors` | Count of `FAIL` findings |
| `warnings` | Count of `WARN` findings |
| `notes` | JSON array of individual findings (PowerShell) or semicolon-delimited string (Bash) |

Every repo always produces a row, even if validation fails partway through.

### Exit codes

| Code | Meaning |
|---|---|
| `0` | All repos passed (OK or WARN) |
| `1` | One or more repos produced a FAIL |

---

## Tests (PowerShell)

The Pester suite covers `Test-Preflight` end-to-end and the pure-logic
portions of `$InvokeRepoValidation` (status grading, CSV slug sanitisation,
LFS size parsing, output normalisation, default branch extraction).
Process-spawning paths are not unit-tested.

```powershell
Invoke-Pester ./Validate_Repo.Tests.ps1
```
