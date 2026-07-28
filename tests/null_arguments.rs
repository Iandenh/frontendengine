//! No pointer a caller can pass may abort the process or reach undefined
//! behaviour.
//!
//! An empty `Context` is entirely legal — every field of the proto is optional
//! — and Go hands over a nil pointer for a zero-length slice, so
//! `(context_data = NULL, context_len = 0)` is a case that happens in normal
//! operation, not just under abuse.

// Assertions are the point of a test; the crate-level bans on these exist to
// keep panics out of the FFI layer.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use common::*;
use frontendengine::*;
use prost::Message;
use std::ffi::CString;

#[test]
fn resolve_accepts_a_null_context_of_length_zero() {
    let engine = engine_with_state();
    let name = CString::new("flag.on").unwrap();

    let mut len: usize = 0;
    let ptr = unsafe { resolve(engine, name.as_ptr(), std::ptr::null(), 0, &mut len) };
    let buffer = Buffer { ptr, len };

    assert!(
        !buffer.is_null(),
        "an empty context is valid input and must resolve"
    );
    let toggle = unleashengine::EvaluatedToggle::decode(buffer.take().as_slice()).unwrap();
    assert_eq!(toggle.name, "flag.on");
    assert!(toggle.enabled);

    unsafe { free_engine(engine) };
}

#[test]
fn resolve_all_accepts_a_null_context_of_length_zero() {
    let engine = engine_with_state();
    let include_all = true;

    let mut len: usize = 0;
    let ptr = unsafe { resolve_all(engine, std::ptr::null(), &include_all, 0, &mut len) };
    let buffer = Buffer { ptr, len };

    assert!(!buffer.is_null(), "an empty context must resolve");
    let list = unleashengine::EvaluatedToggleList::decode(buffer.take().as_slice()).unwrap();
    assert_eq!(list.toggles.len(), 2);

    unsafe { free_engine(engine) };
}

#[test]
fn is_enabled_accepts_a_null_context_of_length_zero() {
    let engine = engine_with_state();
    let name = CString::new("flag.on").unwrap();

    let enabled = unsafe { is_enabled(engine, name.as_ptr(), std::ptr::null(), 0) };

    assert!(enabled, "flag.on is on for an empty context");

    unsafe { free_engine(engine) };
}

#[test]
fn resolve_all_survives_a_null_include_all() {
    let engine = engine_with_state();
    let context = encode_context(None);

    let buffer = unsafe { call_resolve_all(engine, &context, std::ptr::null()) };

    // The value cannot be read, so there is no result to return; the contract
    // is only that the call reports failure rather than dereferencing NULL.
    assert!(buffer.is_null());
    buffer.discard();

    unsafe { free_engine(engine) };
}

#[test]
fn resolve_survives_a_null_out_len() {
    let engine = engine_with_state();
    let context = encode_context(None);
    let name = CString::new("flag.on").unwrap();

    let ptr = unsafe {
        resolve(
            engine,
            name.as_ptr(),
            context.as_ptr(),
            context.len(),
            std::ptr::null_mut(),
        )
    };

    // Without somewhere to write the length the payload is unreachable, so the
    // only correct answer is to hand back nothing — and not leak it.
    assert!(ptr.is_null());

    unsafe { free_engine(engine) };
}

#[test]
fn resolve_all_survives_a_null_out_len() {
    let engine = engine_with_state();
    let context = encode_context(None);
    let include_all = true;

    let ptr = unsafe {
        resolve_all(
            engine,
            context.as_ptr(),
            &include_all,
            context.len(),
            std::ptr::null_mut(),
        )
    };

    assert!(ptr.is_null());

    unsafe { free_engine(engine) };
}

#[test]
fn resolve_survives_a_null_toggle_name() {
    let engine = engine_with_state();
    let context = encode_context(None);

    let buffer = unsafe { call_resolve(engine, std::ptr::null(), &context) };

    assert!(buffer.is_null());
    buffer.discard();

    unsafe { free_engine(engine) };
}

#[test]
fn is_enabled_survives_a_null_toggle_name() {
    let engine = engine_with_state();
    let context = encode_context(None);

    let enabled = unsafe { is_enabled(engine, std::ptr::null(), context.as_ptr(), context.len()) };

    assert!(!enabled, "is_enabled fails closed");

    unsafe { free_engine(engine) };
}

#[test]
fn every_entry_point_survives_a_null_engine() {
    let context = encode_context(None);
    let name = CString::new("flag.on").unwrap();
    let include_all = true;
    let null = std::ptr::null_mut();

    unsafe {
        assert!(call_resolve(null, name.as_ptr(), &context).is_null());
        assert!(call_resolve_all(null, &context, &include_all).is_null());
        assert!(!is_enabled(
            null,
            name.as_ptr(),
            context.as_ptr(),
            context.len()
        ));
    }
}

#[test]
fn a_truncated_context_is_rejected_without_crashing() {
    let engine = engine_with_state();
    let name = CString::new("flag.on").unwrap();

    // Claim more bytes than the proto actually contains a valid message for.
    let garbage: &[u8] = &[0xff, 0xff, 0xff, 0xff];

    let buffer = unsafe { call_resolve(engine, name.as_ptr(), garbage) };
    assert!(buffer.is_null(), "undecodable context must be rejected");
    buffer.discard();

    // A bad request must not damage the engine.
    let context = encode_context(None);
    assert!(unsafe { is_enabled(engine, name.as_ptr(), context.as_ptr(), context.len()) });

    unsafe { free_engine(engine) };
}

#[test]
fn free_functions_accept_null() {
    unsafe {
        free_engine(std::ptr::null_mut());
        free_response(std::ptr::null_mut());
        free_rust_buffer(std::ptr::null_mut(), 0);
        free_rust_buffer(std::ptr::null_mut(), 32);
    }
}

#[test]
fn a_nil_context_pointer_behaves_like_an_empty_one() {
    let engine = engine_with_state();
    let name = CString::new("flag.on").unwrap();

    // cgo hands over a nil data pointer for an empty Go byte slice, so these
    // two spellings of "no context" must produce the same answer.
    let mut nil_len: usize = 0;
    let from_nil = unsafe {
        let ptr = resolve(engine, name.as_ptr(), std::ptr::null(), 0, &mut nil_len);
        Buffer { ptr, len: nil_len }
    };
    let from_empty = unsafe { call_resolve(engine, name.as_ptr(), &[]) };

    assert!(
        !from_nil.is_null(),
        "a nil context pointer must be accepted"
    );
    assert!(!from_empty.is_null());
    assert_eq!(from_nil.take(), from_empty.take());

    unsafe { free_engine(engine) };
}
