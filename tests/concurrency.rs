//! Evaluation happens under a read lock while `take_state` takes the write
//! lock, so readers run concurrently with each other and are serialised only
//! against a state swap.
//!
//! This exercises that arrangement for deadlock and for torn results: taking the
//! wrong guard on a read path, or holding one across a call that needs the
//! other, shows up here as a hang or a wrong answer.

// Assertions are the point of a test; the crate-level bans on these exist to
// keep panics out of the FFI layer.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use common::*;
use frontendengine::*;
use prost::Message;
use std::ffi::{c_void, CString};
use std::thread;

/// The engine pointer is an opaque handle the C ABI is happy to share between
/// threads; Rust needs telling.
#[derive(Clone, Copy)]
struct EnginePtr(*mut c_void);

unsafe impl Send for EnginePtr {}
unsafe impl Sync for EnginePtr {}

impl EnginePtr {
    /// Reached through a method so closures capture the `Send` wrapper rather
    /// than the raw pointer field.
    fn as_raw(self) -> *mut c_void {
        self.0
    }
}

const READERS: usize = 8;
const ITERATIONS: usize = 500;

#[test]
fn concurrent_readers_all_see_consistent_state() {
    let engine = EnginePtr(engine_with_state());

    let handles: Vec<_> = (0..READERS)
        .map(|reader| {
            thread::spawn(move || {
                let name = CString::new("flag.on").unwrap();
                let off = CString::new("flag.off").unwrap();
                let context = encode_context(Some(&format!("user-{reader}")));
                let include_all = true;

                for _ in 0..ITERATIONS {
                    assert!(unsafe {
                        is_enabled(
                            engine.as_raw(),
                            name.as_ptr(),
                            context.as_ptr(),
                            context.len(),
                        )
                    });
                    assert!(!unsafe {
                        is_enabled(
                            engine.as_raw(),
                            off.as_ptr(),
                            context.as_ptr(),
                            context.len(),
                        )
                    });

                    let buffer = unsafe { call_resolve(engine.as_raw(), name.as_ptr(), &context) };
                    let toggle =
                        unleashengine::EvaluatedToggle::decode(buffer.take().as_slice()).unwrap();
                    assert_eq!(toggle.name, "flag.on");
                    assert!(toggle.enabled);

                    let buffer =
                        unsafe { call_resolve_all(engine.as_raw(), &context, &include_all) };
                    let list = unleashengine::EvaluatedToggleList::decode(buffer.take().as_slice())
                        .unwrap();
                    assert_eq!(list.toggles.len(), 2);
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().expect("no reader may panic");
    }

    unsafe { free_engine(engine.as_raw()) };
}

#[test]
fn readers_survive_concurrent_state_replacement() {
    let engine = EnginePtr(engine_with_state());

    let writer = thread::spawn(move || {
        for _ in 0..ITERATIONS {
            let response = unsafe { take_state_json(engine.as_raw(), STATE) };
            assert_eq!(response["error_message"], serde_json::Value::Null);
        }
    });

    let readers: Vec<_> = (0..READERS)
        .map(|reader| {
            thread::spawn(move || {
                let name = CString::new("flag.on").unwrap();
                let context = encode_context(Some(&format!("user-{reader}")));

                for _ in 0..ITERATIONS {
                    // The state is replaced with an identical document, so the
                    // answer is the same before, during and after every swap.
                    assert!(unsafe {
                        is_enabled(
                            engine.as_raw(),
                            name.as_ptr(),
                            context.as_ptr(),
                            context.len(),
                        )
                    });
                }
            })
        })
        .collect();

    writer.join().expect("the writer may not panic");
    for reader in readers {
        reader.join().expect("no reader may panic");
    }

    unsafe { free_engine(engine.as_raw()) };
}

#[test]
fn a_rejected_update_does_not_disturb_concurrent_readers() {
    let engine = EnginePtr(engine_with_state());

    let writer = thread::spawn(move || {
        for _ in 0..ITERATIONS {
            // Every one of these is refused; none may damage the live state.
            let _ = unsafe { take_state_json(engine.as_raw(), "{ not json") };
        }
    });

    let readers: Vec<_> = (0..READERS)
        .map(|_| {
            thread::spawn(move || {
                let name = CString::new("flag.on").unwrap();
                let context = encode_context(None);

                for _ in 0..ITERATIONS {
                    assert!(unsafe {
                        is_enabled(
                            engine.as_raw(),
                            name.as_ptr(),
                            context.as_ptr(),
                            context.len(),
                        )
                    });
                }
            })
        })
        .collect();

    writer.join().expect("the writer may not panic");
    for reader in readers {
        reader.join().expect("no reader may panic");
    }

    unsafe { free_engine(engine.as_raw()) };
}

#[test]
fn engine_handles_are_independent() {
    // Each engine owns its own state; one being replaced must not affect another.
    let loaded = EnginePtr(engine_with_state());
    let empty = EnginePtr(new_engine());

    let handles: Vec<_> = (0..READERS)
        .map(|_| {
            thread::spawn(move || {
                let name = CString::new("flag.on").unwrap();
                let context = encode_context(None);

                for _ in 0..ITERATIONS {
                    assert!(unsafe {
                        is_enabled(
                            loaded.as_raw(),
                            name.as_ptr(),
                            context.as_ptr(),
                            context.len(),
                        )
                    });
                    // No state was ever loaded into this one.
                    assert!(!unsafe {
                        is_enabled(
                            empty.as_raw(),
                            name.as_ptr(),
                            context.as_ptr(),
                            context.len(),
                        )
                    });
                    let buffer = unsafe { call_resolve(empty.as_raw(), name.as_ptr(), &context) };
                    assert!(buffer.is_null());
                    buffer.discard();
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().expect("no reader may panic");
    }

    unsafe {
        free_engine(loaded.as_raw());
        free_engine(empty.as_raw());
    }
}
