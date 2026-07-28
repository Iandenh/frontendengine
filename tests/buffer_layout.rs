//! Buffers handed to the caller must be freeable with the layout the caller
//! is told to free them with.
//!
//! `resolve`/`resolve_all` leak a `Vec<u8>` and pass back `(ptr, len)`.
//! `free_rust_buffer` rebuilds it as `Vec::from_raw_parts(ptr, len, len)`, so
//! the allocation must be exactly `len` bytes. A `Vec` grown by protobuf
//! encoding is not: `bytes::BufMut for Vec<u8>` grows amortized, so capacity
//! always overshoots (len 14 / cap 16, len 44 / cap 84, ...).
//!
//! The default Unix allocator hides this — `System::dealloc` throws the layout
//! away and calls `libc::free`, which recovers the block size itself. Any
//! allocator that trusts the layout it is given (jemalloc's `sdallocx`,
//! mimalloc) corrupts the heap instead. This allocator is the honest one: it
//! records the size at allocation and checks it at deallocation.

// Assertions are the point of a test; the crate-level bans on these exist to
// keep panics out of the FFI layer.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use common::*;
use frontendengine::*;
use prost::Message;
use std::alloc::{GlobalAlloc, Layout, System};
use std::ffi::CString;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering::SeqCst};
use std::sync::Mutex;

const SLOTS: usize = 1 << 17;
const PROBE_LIMIT: usize = 128;

struct Slot {
    ptr: AtomicUsize,
    size: AtomicUsize,
}

static TABLE: [Slot; SLOTS] = [const {
    Slot {
        ptr: AtomicUsize::new(0),
        size: AtomicUsize::new(0),
    }
}; SLOTS];

static TRACKING: AtomicBool = AtomicBool::new(false);
static MISMATCHES: AtomicUsize = AtomicUsize::new(0);
static UNTRACKABLE: AtomicUsize = AtomicUsize::new(0);
static ALLOCATED_SIZE: AtomicUsize = AtomicUsize::new(0);
static CLAIMED_SIZE: AtomicUsize = AtomicUsize::new(0);

fn slot_for(addr: usize) -> usize {
    // Addresses are at least 8-byte aligned; drop the dead low bits.
    (addr >> 4) % SLOTS
}

/// Wraps the system allocator and verifies that each deallocation presents the
/// same size that the matching allocation asked for.
struct LayoutChecking;

unsafe impl GlobalAlloc for LayoutChecking {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() && TRACKING.load(SeqCst) {
            let start = slot_for(ptr as usize);
            for offset in 0..PROBE_LIMIT {
                let slot = &TABLE[(start + offset) % SLOTS];
                if slot
                    .ptr
                    .compare_exchange(0, ptr as usize, SeqCst, SeqCst)
                    .is_ok()
                {
                    slot.size.store(layout.size(), SeqCst);
                    return ptr;
                }
            }
            // No free slot: record that coverage was incomplete rather than
            // letting the test pass on the strength of an unchecked buffer.
            UNTRACKABLE.fetch_add(1, SeqCst);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        if TRACKING.load(SeqCst) {
            let start = slot_for(ptr as usize);
            for offset in 0..PROBE_LIMIT {
                let slot = &TABLE[(start + offset) % SLOTS];
                if slot.ptr.load(SeqCst) == ptr as usize {
                    let allocated = slot.size.load(SeqCst);
                    slot.ptr.store(0, SeqCst);
                    if allocated != layout.size() {
                        MISMATCHES.fetch_add(1, SeqCst);
                        ALLOCATED_SIZE.store(allocated, SeqCst);
                        CLAIMED_SIZE.store(layout.size(), SeqCst);
                    }
                    break;
                }
            }
        }
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: LayoutChecking = LayoutChecking;

/// Serializes the tests: tracking state is process-global, and cargo runs
/// tests in parallel by default. The gate is held across the *whole* test body,
/// setup included, so no sibling test can allocate inside a tracked window.
static GATE: Mutex<()> = Mutex::new(());

/// Runs `body` with layout checking on, then asserts every free matched.
///
/// The table is cleared first. Without that, an entry recorded in one window and
/// freed after tracking was switched off lingers; when the allocator later
/// reuses that address the stale entry shadows the new one and the size
/// comparison comes out wrong.
fn assert_layouts_match(what: &str, body: impl FnOnce()) {
    let _guard = GATE.lock().unwrap_or_else(|poisoned| poisoned.into_inner());

    for slot in &TABLE {
        slot.ptr.store(0, SeqCst);
        slot.size.store(0, SeqCst);
    }
    MISMATCHES.store(0, SeqCst);
    UNTRACKABLE.store(0, SeqCst);

    TRACKING.store(true, SeqCst);
    body();
    TRACKING.store(false, SeqCst);

    let mismatches = MISMATCHES.load(SeqCst);
    assert_eq!(
        mismatches,
        0,
        "{what}: {mismatches} deallocation(s) used the wrong layout \
         (allocated {} bytes, freed as {} bytes)",
        ALLOCATED_SIZE.load(SeqCst),
        CLAIMED_SIZE.load(SeqCst),
    );
    assert_eq!(
        UNTRACKABLE.load(SeqCst),
        0,
        "{what}: allocation table overflowed, result is not trustworthy"
    );
}

#[test]
fn resolve_buffer_frees_with_the_layout_it_was_allocated_with() {
    assert_layouts_match("resolve", || {
        let engine = engine_with_state();
        let name = CString::new("flag.on").unwrap();
        let context = encode_context(Some("user-1"));

        let buffer = unsafe { call_resolve(engine, name.as_ptr(), &context) };
        assert!(!buffer.is_null());
        let bytes = buffer.take();
        let toggle = unleashengine::EvaluatedToggle::decode(bytes.as_slice()).unwrap();
        assert_eq!(toggle.name, "flag.on");
        assert!(toggle.enabled);

        unsafe { free_engine(engine) };
    });
}

#[test]
fn resolve_all_buffer_frees_with_the_layout_it_was_allocated_with() {
    assert_layouts_match("resolve_all", || {
        let engine = engine_with_state();
        let context = encode_context(Some("user-1"));
        let include_all = true;

        let buffer = unsafe { call_resolve_all(engine, &context, &include_all) };
        assert!(!buffer.is_null());
        let bytes = buffer.take();
        let list = unleashengine::EvaluatedToggleList::decode(bytes.as_slice()).unwrap();
        assert_eq!(list.toggles.len(), 2);

        unsafe { free_engine(engine) };
    });
}

#[test]
fn payloads_of_every_size_free_cleanly() {
    // Payload length drives the capacity overshoot, so sweep across the
    // allocator's size classes rather than trusting one sample.
    assert_layouts_match("mixed sizes", || {
        let engine = engine_with_state();
        let context = encode_context(Some("user-1"));
        let include_all = true;

        for length in [1usize, 4, 7, 8, 9, 16, 31, 64, 200, 1000] {
            let name = CString::new("x".repeat(length)).unwrap();
            unsafe { call_resolve(engine, name.as_ptr(), &context) }.discard();
            unsafe { call_resolve_all(engine, &context, &include_all) }.discard();
        }

        unsafe { free_engine(engine) };
    });
}

#[test]
fn repeated_resolution_frees_cleanly() {
    assert_layouts_match("1000 round trips", || {
        let engine = engine_with_state();
        let name = CString::new("flag.on").unwrap();
        let context = encode_context(Some("user-1"));

        for _ in 0..1000 {
            unsafe { call_resolve(engine, name.as_ptr(), &context) }.discard();
        }

        unsafe { free_engine(engine) };
    });
}

#[test]
fn take_state_response_frees_cleanly() {
    assert_layouts_match("take_state", || {
        let engine = new_engine();
        let _ = unsafe { take_state_json(engine, STATE) };
        unsafe { free_engine(engine) };
    });
}
