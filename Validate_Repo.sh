validate_repo() {
  local repo=$1
  local errors=0

  # --- Sync mirror -----------------------------------------------------
  # Fetch and prune before validating so stale, deleted, and renamed
  # refs are reflected in the local mirror prior to any diff.
  git --git-dir=$repo.git fetch --prune github \
    || { echo "FETCH FAILED: $repo"; return 1; }

  # --- Ref integrity ---------------------------------------------------
  # Diff every ref/SHA pair between Bitbucket and GitHub.
  # A clean diff guarantees identical commit graphs; SHA collision is
  # not a realistic threat model here.
  diff \
    <(git ls-remote ssh://bitbucket/$repo | sort) \
    <(git ls-remote git@github.com:org/$repo.git | sort) \
    || { echo "REF MISMATCH: $repo"; (( errors++ )); }

  # Belt-and-suspenders: confirm every Bitbucket SHA exists as an object
  # in the local mirror. Catches partial pushes where refs were written
  # but objects were not fully transferred.
  while read -r sha ref; do
    git --git-dir=$repo.git cat-file -e "$sha" \
      || { echo "MISSING OBJECT: $sha $ref"; (( errors++ )); }
  done < <(git ls-remote ssh://bitbucket/$repo)

  # --- Default branch --------------------------------------------------
  # Extract Bitbucket's HEAD symref; more reliable than parsing raw HEAD.
  local bb_default
  bb_default=$(git ls-remote --symref ssh://bitbucket/$repo HEAD \
    | awk '/^ref:/ {print $2}')

  local gh_default
  gh_default=$(gh api /repos/org/$repo --jq '.default_branch')

  # Validate branch name parity.
  [[ "$bb_default" == "refs/heads/$gh_default" ]] \
    || { echo "DEFAULT BRANCH NAME MISMATCH: bb=$bb_default gh=$gh_default"; (( errors++ )); }

  # Validate HEAD SHA parity. Name and SHA are checked independently so
  # each produces a specific error rather than a single generic failure.
  local bb_head gh_head
  bb_head=$(git ls-remote ssh://bitbucket/$repo HEAD | awk '{print $1}')
  gh_head=$(git ls-remote git@github.com:org/$repo.git HEAD | awk '{print $1}')

  [[ "$bb_head" == "$gh_head" ]] \
    || { echo "HEAD SHA MISMATCH: bb=$bb_head gh=$gh_head"; (( errors++ )); }

  # --- LFS -------------------------------------------------------------
  # Only run LFS checks if this repo actually uses LFS.
  if git --git-dir=$repo.git show HEAD:.gitattributes 2>/dev/null \
       | grep -q 'filter=lfs'; then

    # Validate pointer file structure against the local mirror.
    # --pointers checks pointer hashes are well-formed but does NOT
    # verify that LFS objects exist on GitHub or that historical objects
    # on all refs are present.
    git --git-dir=$repo.git lfs fsck --pointers \
      || { echo "LFS POINTER INTEGRITY FAILURE: $repo"; (( errors++ )); }

    # Verify LFS objects exist on GitHub across all historical refs.
    # lfs fsck (no --pointers) forces pointer->object resolution against
    # the remote. --git-dir is insufficient for LFS object resolution so
    # a real clone is required. lfs_tmp is assigned unconditionally before
    # the clone block so rm -rf is always safe regardless of clone outcome.
    local lfs_tmp
    lfs_tmp=$(mktemp -d)
    (
      git clone --no-checkout git@github.com:org/$repo.git "$lfs_tmp" \
        && git -C "$lfs_tmp" lfs fetch --all \
        && git -C "$lfs_tmp" lfs fsck
    ) || { echo "LFS OBJECT MISSING ON GITHUB: $repo"; (( errors++ )); }
    rm -rf "$lfs_tmp"

  fi

  # --- Result ----------------------------------------------------------
  if (( errors > 0 )); then
    echo "FAIL: $repo ($errors error(s))"
    return 1
  fi

  echo "OK: $repo"
  return 0
}