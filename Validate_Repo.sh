#!/usr/bin/env bash
set -euo pipefail

# ==========================================================================
# Configuration
# ==========================================================================

# Output CSV path. Override via environment if needed.
CSV_OUT="${CSV_OUT:-migration_validation.csv}"

# Concurrency cap. LFS repos each perform a full clone + lfs fetch --all
# against GitHub. At PARALLELISM=4, assume up to 4 simultaneous clones.
# Reduce if GitHub rate limits or network saturation is observed.
readonly PARALLELISM="${PARALLELISM:-4}"

# Repo list — one repo slug per line.
readonly REPOS_FILE="${REPOS_FILE:-repos.txt}"

# Warn thresholds
readonly WARN_REPO_SIZE_KIB=1843200    # ~1.8 GiB; GitHub hard limit is 2 GiB
readonly WARN_BRANCH_COUNT=100
readonly WARN_LFS_SIZE_BYTES=1073741824 # 1 GiB

# ==========================================================================
# Serial setup — must complete before workers are spawned
# ==========================================================================

# Initialise CSV header. Kept outside validate_repo so parallel workers
# cannot race on header creation.
if [[ ! -f "$CSV_OUT" ]]; then
  echo "repo,status,errors,warnings,notes" > "$CSV_OUT"
fi

# ==========================================================================
# validate_repo
# ==========================================================================

validate_repo() {
  local repo=$1
  local errors=0
  local warnings=0
  local -a notes=()

  # --- Helpers -----------------------------------------------------------
  # Defined inside the function so they close over local variables without
  # polluting the global namespace or racing under parallel execution.

  fail() {
    echo "  FAIL [$repo]: $1"
    notes+=( "FAIL: $1" )
    (( errors++ )) || true
  }

  warn() {
    echo "  WARN [$repo]: $1"
    notes+=( "WARN: $1" )
    (( warnings++ )) || true
  }

  # --- Sync mirror -------------------------------------------------------
  # Fetch and prune before validating so stale, deleted, and renamed refs
  # are reflected in the local mirror prior to any diff.
  git --git-dir=$repo.git fetch --prune github \
    || { echo "FETCH FAILED: $repo"; return 1; }

  # --- Ref integrity -----------------------------------------------------
  # Diff every ref/SHA pair between Bitbucket and GitHub. A clean diff
  # guarantees identical commit graphs.
  diff \
    <(git ls-remote ssh://bitbucket/$repo | sort) \
    <(git ls-remote git@github.com:org/$repo.git | sort) \
    || fail "REF MISMATCH"

  # Belt-and-suspenders: confirm every Bitbucket SHA exists as an object
  # in the local mirror. Catches partial pushes where refs were written
  # but objects were not fully transferred.
  while read -r sha ref; do
    git --git-dir=$repo.git cat-file -e "$sha" \
      || fail "MISSING OBJECT: $sha $ref"
  done < <(git ls-remote ssh://bitbucket/$repo)

  # --- Default branch ----------------------------------------------------
  # Use --symref to extract Bitbucket's HEAD symref reliably.
  local bb_default
  bb_default=$(git ls-remote --symref ssh://bitbucket/$repo HEAD \
    | awk '/^ref:/ {print $2}')

  local gh_default
  gh_default=$(gh api /repos/org/$repo --jq '.default_branch')

  # Name and SHA are checked independently for specific error messages.
  [[ "$bb_default" == "refs/heads/$gh_default" ]] \
    || fail "DEFAULT BRANCH NAME MISMATCH: bb=$bb_default gh=$gh_default"

  local bb_head gh_head
  bb_head=$(git ls-remote ssh://bitbucket/$repo HEAD | awk '{print $1}')
  gh_head=$(git ls-remote git@github.com:org/$repo.git HEAD | awk '{print $1}')

  [[ "$bb_head" == "$gh_head" ]] \
    || fail "HEAD SHA MISMATCH: bb=$bb_head gh=$gh_head"

  # --- WARN: repo size ---------------------------------------------------
  # count-objects -v emits raw KiB; drop -H to keep values arithmetic-safe.
  local repo_size
  repo_size=$(git --git-dir=$repo.git count-objects -v \
    | awk '/^size-pack:/ {print $2}')
  (( repo_size > WARN_REPO_SIZE_KIB )) \
    && warn "REPO SIZE NEAR 2 GiB LIMIT (${repo_size} KiB)" || true

  # --- WARN: branch count ------------------------------------------------
  local branch_count
  branch_count=$(git --git-dir=$repo.git branch -r | wc -l)
  (( branch_count > WARN_BRANCH_COUNT )) \
    && warn "HIGH BRANCH COUNT ($branch_count)" || true

  # --- LFS ---------------------------------------------------------------
  if git --git-dir=$repo.git show HEAD:.gitattributes 2>/dev/null \
       | grep -q 'filter=lfs'; then

    # WARN: LFS volume
    # git lfs ls-files -s outputs human-readable sizes, e.g.:
    #   abc123def * path/to/file (1.5 GB)
    # $(NF-1) is the numeric part; $NF is the unit with a closing paren.
    # Strip the paren from the unit, then convert to bytes before summing.
    local lfs_size
    lfs_size=$(git --git-dir=$repo.git lfs ls-files -s 2>/dev/null \
      | awk '{
          v = $(NF-1); sub(/^\(/, "", v); v = v + 0
          u = $NF;     sub(/\)$/, "", u)
          mult = (u == "TB") ? 1099511627776 : \
                 (u == "GB") ? 1073741824    : \
                 (u == "MB") ? 1048576       : \
                 (u == "KB") ? 1024          : 1
          sum += v * mult
        } END { print int(sum + 0) }')
    (( lfs_size > WARN_LFS_SIZE_BYTES )) \
      && warn "LARGE LFS VOLUME (${lfs_size} bytes)" || true

    # Validate pointer file structure against the local mirror.
    # --pointers checks pointer hashes are well-formed but does NOT verify
    # that LFS objects exist on GitHub or that historical objects on all
    # refs are present.
    git --git-dir=$repo.git lfs fsck --pointers \
      || fail "LFS POINTER INTEGRITY FAILURE"

    # Verify LFS objects exist on GitHub across all historical refs.
    # lfs fsck (no --pointers) forces pointer->object resolution against
    # the remote. --git-dir is insufficient for LFS object resolution so
    # a real clone is required. lfs_tmp is assigned unconditionally before
    # the subshell so rm -rf is always safe regardless of clone outcome.
    local lfs_tmp
    lfs_tmp=$(mktemp -d)
    (
      git clone --no-checkout git@github.com:org/$repo.git "$lfs_tmp" \
        && git -C "$lfs_tmp" lfs fetch --all \
        && git -C "$lfs_tmp" lfs fsck
    ) || fail "LFS OBJECT MISSING ON GITHUB"
    rm -rf "$lfs_tmp"

  fi

  # --- Graded result -----------------------------------------------------
  local status
  if (( errors > 0 )); then
    status="FAIL"
  elif (( warnings > 0 )); then
    status="WARN"
  else
    status="OK"
  fi

  echo "$status: $repo (errors=$errors warnings=$warnings)"

  # --- CSV output --------------------------------------------------------
  # Each worker writes to a per-repo temp file rather than appending
  # directly to CSV_OUT. Parallel >> appends are not atomic and will
  # corrupt rows. The main process merges temp files after xargs completes.
  #
  # Repo slug is used as the temp file suffix; forward slashes are replaced
  # so the name is filesystem-safe.
  #
  # Embedded double quotes are stripped from notes to prevent CSV parser
  # breakage. Notes are machine-generated so this should be a no-op.
  local notes_csv
  notes_csv=$(IFS=';'; echo "${notes[*]//\"/}")
  echo "$repo,$status,$errors,$warnings,\"$notes_csv\"" \
    > "${CSV_OUT}.${repo//\//_}.tmp"

  # WARNs are non-blocking; only FAILs produce a non-zero exit code.
  (( errors > 0 )) && return 1 || return 0
}

# Export function and constants so xargs child processes inherit them.
export -f validate_repo
export CSV_OUT WARN_REPO_SIZE_KIB WARN_BRANCH_COUNT WARN_LFS_SIZE_BYTES

# ==========================================================================
# Parallel execution
# ==========================================================================

echo "Starting validation (parallelism=$PARALLELISM) ..."

# Null-delimited xargs guards against repo names with spaces or special
# characters. -n1 dispatches one repo per worker. -P caps concurrency.
# xargs exits non-zero if any child process exits non-zero.
tr '\n' '\0' < "$REPOS_FILE" \
  | xargs -0 -n1 -P "$PARALLELISM" -I{} bash -c 'validate_repo "$@"' _ {}
xargs_exit=$?

# ==========================================================================
# Post-run CSV merge
# ==========================================================================

# Merge per-repo temp files into the output CSV in sorted order, then
# clean up. Sorting here gives a stable output regardless of execution
# order under parallelism.
for tmp in $(ls "${CSV_OUT}".*.tmp 2>/dev/null | sort); do
  cat "$tmp" >> "$CSV_OUT"
  rm -f "$tmp"
done

echo "Done. Results written to $CSV_OUT"

# Surface xargs exit code so callers and CI pipelines see failures.
# xargs exits 1 if any worker failed, 0 if all succeeded.
# Use the CSV to count and triage individual failures.
exit $xargs_exit