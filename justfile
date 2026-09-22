test:
    cargo nextest run

check:
    cargo check

build:
    cargo build

publish:
    cargo publish
    cd trouper-macros && cargo-publish
