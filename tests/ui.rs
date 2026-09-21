//! Compile-fail UI tests for the Event/Command schema derives.
//!
//! Each file in `tests/ui/` must fail to compile; the `.stderr` pins hold
//! the expected diagnostics (rustc-version-sensitive — re-pin with
//! `TRYBUILD=overwrite` after toolchain upgrades).

#[test]
fn ui() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/ui/*.rs");
}
