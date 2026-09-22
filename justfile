test:
    cargo nextest run

check:
    cargo check

build:
    cargo build

publish:
    # Order matters: the proc-macro crate first — trouper depends on it
    # with a version requirement, so the new version must exist upstream
    # before `cargo publish` verifies the manifest.
    cd trouper_macros && cargo publish
    cargo publish
