//! `take_state` must report bad input as an error response, never by killing
//! the host process.
//!
//! Before the fix each of these tests aborted the whole test binary with
//! SIGABRT: the `unwrap`s panicked, and a panic crossing an `extern "C"`
//! boundary is a non-unwinding panic, so it is not catchable by the caller.

// Assertions are the point of a test; the crate-level bans on these exist to
// keep panics out of the FFI layer.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use common::*;
use frontendengine::*;
use serde_json::Value;

#[test]
fn valid_state_reports_no_error() {
    let engine = engine_with_state();
    unsafe { free_engine(engine) };
}

#[test]
fn malformed_json_reports_an_error_instead_of_aborting() {
    let engine = new_engine();

    let response = unsafe { take_state_json(engine, "{ this is not json }") };

    let message = response["error_message"]
        .as_str()
        .expect("a malformed payload must come back with an error message");
    assert!(
        message.contains("JSON"),
        "error message should name the cause, got: {message}"
    );

    unsafe { free_engine(engine) };
}

#[test]
fn json_that_is_not_a_state_document_reports_an_error() {
    let engine = new_engine();

    // Syntactically valid JSON, but not an UpdateMessage.
    let response = unsafe { take_state_json(engine, r#"{"hello":"world"}"#) };

    assert!(
        response["error_message"].is_string(),
        "unexpected JSON shape must be reported, got: {response}"
    );

    unsafe { free_engine(engine) };
}

#[test]
fn null_engine_reports_an_error_instead_of_aborting() {
    let response = unsafe { take_state_json(std::ptr::null_mut(), STATE) };

    assert!(
        response["error_message"].is_string(),
        "a null engine must be reported, got: {response}"
    );
}

#[test]
fn null_json_reports_an_error_instead_of_aborting() {
    let engine = new_engine();

    let response = unsafe { take_state_raw(engine, std::ptr::null()) };

    assert!(
        response["error_message"].is_string(),
        "a null payload must be reported, got: {response}"
    );

    unsafe { free_engine(engine) };
}

#[test]
fn a_successful_update_carries_no_error_message() {
    let engine = new_engine();

    let response = unsafe { take_state_json(engine, STATE) };

    assert_eq!(response["error_message"], Value::Null);

    unsafe { free_engine(engine) };
}

#[test]
fn state_can_be_replaced_after_a_rejected_update() {
    let engine = new_engine();

    // A rejected update must leave the engine usable.
    let _ = unsafe { take_state_json(engine, "not json at all") };
    let response = unsafe { take_state_json(engine, STATE) };
    assert_eq!(response["error_message"], Value::Null);

    let context = encode_context(None);
    let name = std::ffi::CString::new("flag.on").unwrap();
    assert!(unsafe { is_enabled(engine, name.as_ptr(), context.as_ptr(), context.len()) });

    unsafe { free_engine(engine) };
}
