#!/usr/bin/env python3
"""Folded-stack accounting for the competitors profile rig.

Reduces perf-captured, stackcollapse'd folded stacks (one file per leg)
into:

1. a bucket table per leg — self-time shares partitioning ~100% of
   samples, with an approximate ns/msg column (share x measured ns/msg);
2. an annotated stack tree per leg — the dominant root-to-leaf path
   family with inclusive % per frame and bucket tags inline;
3. a side-by-side trouper-vs-ractor comparison over the union of
   buckets, zero-filled.

Self attribution: a sample's deepest (leaf) frame determines its bucket
in the table. Inclusive attribution: a frame's share counts every stack
that passes through it (used by the tree).

Bucket precedence (leaf frame, first match wins — evaluated top-down):

  harness         the driver's kanal ops + waits + sleeps (outside the
                  per-message work: prime/start/done signal plumbing)
  tokio-runtime   tokio scheduler/worker/park/task frames, mio
  sync-prims      parking_lot, std::sys::sync, futex, arc_swap debts,
                  std::sync
  alloc           malloc/free/tcache/realloc and Rust alloc machinery
  syscalls-clock  vDSO clock, clock_gettime, getrandom, raw syscall
  trouper         any trouper:: frame
  ractor          any ractor:: frame
  channel-ops     kanal:: frames (in-actor done channel / door)
  unwound-libc    residual [libc]/[unknown]/binary-name frames

Frames that merely name containers (Box<dyn FnOnce> stubs, thread_start,
Rust shim frames like __rust_begin_short_backtrace) are transparent for
matching but never themselves terminal buckets except as unwound-libc.

Usage:
  python3 tools/fold_accounting.py TROUPER.folded RACTOR.folded \
      --trouper-ns 800 --ractor-ns 255
"""

from __future__ import annotations

import argparse
import re
import sys
from collections import Counter

# ── bucket precedence ─────────────────────────────────────────────────────

PATTERNS = [
    ("harness", re.compile(
        r"^profile_competitors::(main|messages|wait_prime|timed_run)"
        r"|std::thread::sleep")),
    ("tokio-runtime", re.compile(
        r"^tokio::(runtime|task|sync::notify|sync::batch_semaphore"
        r"|sync::task|sync::mpsc|loom)"
        r"|^futures_(util|core)::|mio$|^tokio$")),
    ("sync-prims", re.compile(
        r"^(parking_lot|lock_api|arc_swap|std::sys::sync|std::sync|"
        r"core::sync::atomic|std::sys::thread_local|std::sys::pal)")),
    ("alloc", re.compile(
        r"^(malloc|free|cfree|calloc|realloc|tcache|int_malloc|"
        r"_int_|checked_request2size|sysmalloc|__libc_calloc|"
        r"__rustc::__rdl_|__rust_alloc|__rust_dealloc|alloc::|"
        r"core::ptr::drop_glue|<alloc::|core::mem::|"
        r"std::alloc|<.+ as core::alloc::global::GlobalAlloc>)"
        r"|__GI___libc_(malloc|free|realloc)")),
    ("syscalls-clock", re.compile(
        r"^(__vdso|vdso|clock_gettime|getrandom|syscall|__syscall|"
        r"std::sys::pal::unix::time::Timespec|"
        r"<i32 as std::sys::pal::unix::IsMinusOne>)")),
    ("trouper", re.compile(r"^(trouper|<trouper|profile_competitors::"
                           r"(Producer|Sink|Prime|Tick|trouper_leg))")),
    ("ractor", re.compile(r"^(ractor|<ractor|profile_competitors::"
                          r"ractor_leg)")),
    ("channel-ops", re.compile(r"^(kanal|<kanal)")),
    ("unwound-libc", re.compile(r".")),  # catch-all: libc/unknown/binaries
]

# Self-time-only refinement: when the leaf is an anonymous [libc.so.6]
# frame, the real work is inside libc (memcpy, futex, malloc internals…).
# That is reported as unwound-libc (libc) and, when the frame above it is
# malloc-family, as alloc — handled by re-inspecting the parent in
# classify_leaf().

TRANSPARENT = re.compile(
    r"^(\[libc\.so\.6\]|\[ld-linux|\[libgcc|"
    r"<std::sys::thread::unix::Thread>::new::thread_start|"
    r"<alloc::boxed::Box<dyn core::ops::function::FnOnce<|"
    r"std::sys::backtrace::__rust_begin_short_backtrace|"
    r"std::thread::lifecycle::spawn_unchecked|"
    r"std::sys::thread::unix|"
    r"<std::thread::|std::thread::|"
    r"core::ops::function::FnOnce|"
    r"core::future::future::Future|"
    r"core::pin::|<core::pin::Pin<|"
    r"core::ptr::non_null|"
    r"<core::option::Option|core::result::Result|"
    r"<core::cell::|core::cell::|"
    r"std::sys::backtrace|"
    r"core::mem::maybe_uninit|"
    r"core::ptr::read|core::ptr::write|"
    r"core::hint::|core::mem::forget|"
    r"_[a-z_]*$)"  # trailing partial token from stackcollapse line-wrap
)


_ANON = re.compile(r"^\[|\[__libc_start_main\]|^__libc_start_main")


def parse(path: str):
    """Parse a folded file into {thread: {stack_tuple: count}}."""
    threads: dict[str, Counter] = {}
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.rstrip("\n")
            if not line:
                continue
            stack_str, _, cnt_str = line.rpartition(" ")
            if not stack_str or not cnt_str.isdigit():
                continue
            frames = tuple(stack_str.split(";"))
            thread = frames[0] if frames else "?"
            threads.setdefault(thread, Counter())[frames] += int(cnt_str)
    return threads


def bucket_of(frame: str) -> str:
    # Generic impl frames arrive with a leading '<' (<arc_swap::…,
    # <lock_api::…); match on the stripped form so container-generic
    # prefixes don't dodge the pattern.
    stripped = frame.lstrip("<")
    for name, pat in PATTERNS:
        if pat.search(frame) or pat.search(stripped):
            return name
    return "unwound-libc"


def classify_leaf(frames: tuple[str, ...]) -> str:
    """Bucket for a stack's leaf (self time), with precedence rules.

    Anonymous [libc.so.6] / [unknown] leaves carry no symbol, so the
    sample is attributed to the nearest resolved ancestor frame's
    bucket — fp unwinding is healthy here (0% [unknown] anywhere), and
    an anonymous libc frame under a trouper frame is time libc spent on
    trouper's behalf (memcpy, futex, malloc internals). When even the
    resolved ancestor is itself another anonymous frame, the sample
    stays unwound-libc.
    """
    n = len(frames)
    leaf = frames[n - 1]

    if not _ANON.match(leaf):
        return bucket_of(leaf)

    # Anonymous leaf: walk up to the nearest resolved frame.
    for i in range(n - 2, -1, -1):
        frame = frames[i]
        if _ANON.match(frame) or TRANSPARENT.match(frame):
            continue
        return bucket_of(frame)
    return "unwound-libc"


def self_buckets(parsed) -> Counter:
    out: Counter = Counter()
    for _thread, stacks in parsed.items():
        for frames, cnt in stacks.items():
            out[classify_leaf(frames)] += cnt
    return out


def frame_inclusive(parsed, needle: str) -> tuple[int, int]:
    """Total samples and thread-span for stacks containing `needle`."""
    total = 0
    for _thread, stacks in parsed.items():
        for frames, cnt in stacks.items():
            if any(needle in f for f in frames):
                total += cnt
    return total, 0


# ── stack tree ────────────────────────────────────────────────────────────


def build_tree(parsed, thread_filter: str | None):
    """Merge stacks into a prefix tree: frame -> (samples, children)."""
    tree: dict = {}
    total = 0
    for thread, stacks in parsed.items():
        if thread_filter and thread != thread_filter:
            continue
        for frames, cnt in stacks.items():
            total += cnt
            node = tree
            node["#"] = node.get("#", 0) + cnt
            for f in frames:
                child = node.setdefault(f, {})
                child["#"] = child.get("#", 0) + cnt
                node = child
    return tree, total


def render_tree(node, total, min_pct: float, depth: int = 0,
                out: list[str] | None = None, prefix: str = "",
                budget: dict | None = None):
    """Print the dominant path; branch at the heaviest child; collapse
    siblings under min_pct into one line.

    Transparent frames (thread stubs, Box shims, thread_local plumbing)
    are elided from the output but still descended — without this the
    tokio runtime scaffolding alone eats the whole line budget before
    the interesting frames appear."""
    if out is None:
        out = []
    if budget is None:
        budget = {"lines": 110}
    if depth > 80 or budget["lines"] <= 0 or "#" not in node:
        return out
    children = [(f, ch) for f, ch in node.items() if f != "#"]
    children.sort(key=lambda kv: kv[1].get("#", 0), reverse=True)
    shown = 0
    for f, ch in children:
        c = ch.get("#", 0)
        pct = 100.0 * c / total
        if pct < min_pct:
            continue
        transparent = bool(TRANSPARENT.match(f))
        if not transparent:
            tag = bucket_of(f)
            out.append(f"{prefix}{pct:5.1f}%  {f[:110]} [{tag}]")
            budget["lines"] -= 1
        shown += 1
        render_tree(ch, total, min_pct, depth + 1, out,
                    prefix if transparent else prefix + "  ", budget)
        if budget["lines"] <= 0:
            break
    if not shown:
        return out
    return out


# ── output ────────────────────────────────────────────────────────────────

BUCKET_ORDER = [
    "trouper", "ractor", "tokio-runtime", "sync-prims", "alloc",
    "syscalls-clock", "channel-ops", "harness", "unwound-libc",
]


def bucket_table(name: str, buckets: Counter, total: int, ns: float):
    rows = []
    for b in BUCKET_ORDER:
        c = buckets.get(b, 0)
        pct = 100.0 * c / total
        rows.append((b, c, pct, pct / 100.0 * ns))
    print(f"\n### {name} — bucket table (measured {ns:.0f} ns/msg)\n")
    print("| bucket | samples | self % | ns/msg (approx) |")
    print("|---|---:|---:|---:|")
    for b, c, pct, ns_msg in rows:
        print(f"| {b} | {c} | {pct:.1f}% | {ns_msg:.0f} |")
    tot_pct = sum(p for _, _, p, _ in rows)
    tot_ns = sum(n for _, _, _, n in rows)
    print(f"| **TOTAL** | {total} | {tot_pct:.1f}% | {tot_ns:.0f} |")


def dominant_path_report(name: str, parsed, hot_thread: str):
    print(f"\n### {name} — annotated hot-path tree "
          f"(dominant family, inclusive %; [tag] = bucket)\n")
    tree, total = build_tree(parsed, thread_filter=hot_thread)
    lines = render_tree(tree, total, min_pct=1.0)
    print("```")
    for ln in lines:
        print(ln)
    print("```")


def side_by_side(t: Counter, r: Counter, t_total: int, r_total: int,
                 t_ns: float, r_ns: float):
    print("\n### Side-by-side — same taxonomy, both legs\n")
    print("| bucket | trouper self % (ns) | ractor self % (ns) | Δ ns/msg |")
    print("|---|---:|---:|---:|")
    for b in BUCKET_ORDER:
        tc = t.get(b, 0)
        rc = r.get(b, 0)
        tp = 100.0 * tc / t_total
        rp = 100.0 * rc / r_total
        tns = tp / 100.0 * t_ns
        rns = rp / 100.0 * r_ns
        delta = tns - rns
        print(f"| {b} | {tp:.1f}% ({tns:.0f}) | {rp:.1f}% ({rns:.0f}) "
              f"| {delta:+.0f} |")
    print()
    print("(0.0% (0) marks a bucket with no samples in that leg — the "
          "category exists in the taxonomy but not in that runtime.)")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("trouper_folded")
    ap.add_argument("ractor_folded")
    ap.add_argument("--trouper-ns", type=float, required=True)
    ap.add_argument("--ractor-ns", type=float, required=True)
    args = ap.parse_args()

    t_parsed = parse(args.trouper_folded)
    r_parsed = parse(args.ractor_folded)

    t_buckets = self_buckets(t_parsed)
    r_buckets = self_buckets(r_parsed)
    t_total = sum(t_buckets.values())
    r_total = sum(r_buckets.values())

    unknown_pct_t = 100.0 * t_buckets.get("unwound-libc", 0) / t_total
    unknown_pct_r = 100.0 * r_buckets.get("unwound-libc", 0) / r_total

    print(f"# Accounting: trouper {t_total} samples "
          f"({unknown_pct_t:.1f}% unwound-libc), "
          f"ractor {r_total} samples "
          f"({unknown_pct_r:.1f}% unwound-libc)")

    bucket_table("trouper", t_buckets, t_total, args.trouper_ns)
    bucket_table("ractor", r_buckets, r_total, args.ractor_ns)

    # Hot threads: the multi-thread runtime worker threads.
    t_hot = max(t_parsed, key=lambda th: sum(t_parsed[th].values()))
    r_hot = max(r_parsed, key=lambda th: sum(r_parsed[th].values()))
    dominant_path_report("trouper", t_parsed, t_hot)
    dominant_path_report("ractor", r_parsed, r_hot)

    side_by_side(t_buckets, r_buckets, t_total, r_total,
                 args.trouper_ns, args.ractor_ns)
    return 0


if __name__ == "__main__":
    sys.exit(main())
