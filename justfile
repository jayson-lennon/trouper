test:
    cargo nextest run

check:
    cargo check

build:
    cargo build

# Bump version (major/minor/patch), commit, and tag — git adaptation of the
# fossil bump flow. Moves BOTH crates to the same number in one shot:
# trouper's `version`, trouper_macros' `version`, and the workspace
# dependency requirement, so the two published crates never drift apart.
bump LEVEL:
    #!/usr/bin/env bash
    set -euo pipefail

    # --- Validate input ---
    case "{{LEVEL}}" in
        major|minor|patch) ;;
        *) echo "Usage: just bump <major|minor|patch>" >&2; exit 1 ;;
    esac

    # --- Pre-flight: must be on main ---
    BRANCH=$(git rev-parse --abbrev-ref HEAD)
    if [ "$BRANCH" != "main" ]; then
        echo "Error: must be on main (currently on '$BRANCH')" >&2
        exit 1
    fi

    # --- Pre-flight: working tree must be clean ---
    if [ -n "$(git status --porcelain)" ]; then
        echo "Error: working tree has uncommitted changes" >&2
        exit 1
    fi

    # --- Compute the new version (plain semver arithmetic) ---
    CURRENT=$(sed -n 's/^version = "\(.*\)"/\1/p' Cargo.toml)
    IFS='.' read -r MAJOR MINOR PATCH <<< "$CURRENT"
    case "{{LEVEL}}" in
        major) NEW="$((MAJOR + 1)).0.0" ;;
        minor) NEW="${MAJOR}.$((MINOR + 1)).0" ;;
        patch) NEW="${MAJOR}.${MINOR}.$((PATCH + 1))" ;;
    esac

    # --- Resolve tag collisions (walk forward by patch) ---
    CANDIDATE="$NEW"
    ATTEMPTS=0
    while git tag --list "v${CANDIDATE}" | grep -qx "v${CANDIDATE}"; do
        echo "Tag v${CANDIDATE} already exists, skipping..."
        ATTEMPTS=$((ATTEMPTS + 1))
        if [ "$ATTEMPTS" -ge 100 ]; then
            echo "Error: too many tag collisions, aborting" >&2
            exit 1
        fi
        PATCH=$((PATCH + 1))
        CANDIDATE="${MAJOR}.${MINOR}.${PATCH}"
    done

    # --- Update both manifests + the dependency requirement, in lockstep ---
    sed -i "s/^version = \".*\"/version = \"${CANDIDATE}\"/" Cargo.toml trouper_macros/Cargo.toml
    sed -i "s/trouper_macros = { path = \"trouper_macros\", version = \"[^\"]*\" }/trouper_macros = { path = \"trouper_macros\", version = \"${CANDIDATE}\" }/" Cargo.toml

    # --- Regenerate Cargo.lock for the new workspace versions ---
    cargo update --workspace

    # --- Sanity: both crates must now carry exactly the new version ---
    A=$(sed -n 's/^version = "\(.*\)"/\1/p' Cargo.toml)
    B=$(sed -n 's/^version = "\(.*\)"/\1/p' trouper_macros/Cargo.toml)
    if [ "$A" != "$CANDIDATE" ] || [ "$B" != "$CANDIDATE" ]; then
        echo "Error: version mismatch after bump (trouper=$A, trouper_macros=$B, wanted $CANDIDATE)" >&2
        exit 1
    fi

    # --- Commit + tag (annotated, so `git push --follow-tags` carries it) ---
    git add Cargo.toml Cargo.lock trouper_macros/Cargo.toml
    git commit -m "Bump version to ${CANDIDATE}"
    git tag -a "v${CANDIDATE}" -m "trouper v${CANDIDATE}"

    echo "Bumped to ${CANDIDATE}, committed and tagged as v${CANDIDATE}"
    echo "Next: just publish"

publish:
    #!/usr/bin/env bash
    set -euo pipefail

    # --- Pre-flight: main + clean, so the tag lands on the released tree ---
    BRANCH=$(git rev-parse --abbrev-ref HEAD)
    if [ "$BRANCH" != "main" ]; then
        echo "Error: must be on main (currently on '$BRANCH')" >&2
        exit 1
    fi
    if [ -n "$(git status --porcelain)" ]; then
        echo "Error: working tree has uncommitted changes" >&2
        exit 1
    fi

    # --- Push main with its release tag first: crates.io artifacts should
    # always correspond to a pushed, tagged commit ---
    echo '==> Pushing main (with tags) to origin'
    git push --follow-tags

    # --- Publish. Order matters: the proc-macro crate first — trouper
    # depends on it with a version requirement, so the new version must
    # exist upstream before `cargo publish` verifies the manifest. ---
    (cd trouper_macros && cargo publish)
    cargo publish
