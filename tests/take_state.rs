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

// A toggle that fails to compile does not mean the update failed: the state was
// applied, and the rest of it works. Reporting that as an error made every
// refresh of a real Unleash feature file look like a failure.
#[test]
fn warnings_are_reported_as_a_success_carrying_detail() {
    let engine = new_engine();

    let response = unsafe { take_state_json(engine, STATE_WITH_WARNINGS) };

    assert_eq!(
        response["error_message"],
        Value::Null,
        "an applied update is not an error: {response}"
    );

    let warnings = response["value"]
        .as_array()
        .expect("warnings should be reported in `value`");
    assert!(!warnings.is_empty(), "expected at least one warning");

    let joined = warnings
        .iter()
        .filter_map(Value::as_str)
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        joined.contains("flag.uncompilable"),
        "a warning should name the toggle it concerns, got: {joined}"
    );

    unsafe { free_engine(engine) };
}

#[test]
fn a_clean_update_reports_no_warnings() {
    let engine = new_engine();

    let response = unsafe { take_state_json(engine, STATE) };

    let warnings = response["value"]
        .as_array()
        .expect("`value` should always be a list of warnings");
    assert!(
        warnings.is_empty(),
        "a clean update should report no warnings, got: {warnings:?}"
    );

    unsafe { free_engine(engine) };
}

#[test]
fn toggles_that_compile_still_work_after_a_partial_update() {
    let engine = new_engine();

    let _ = unsafe { take_state_json(engine, STATE_WITH_WARNINGS) };

    let context = encode_context(None);
    let good = std::ffi::CString::new("flag.on").unwrap();
    assert!(
        unsafe { is_enabled(engine, good.as_ptr(), context.as_ptr(), context.len()) },
        "the toggles that did compile must still evaluate"
    );

    // The uncompilable one is documented to evaluate as off rather than error.
    let bad = std::ffi::CString::new("flag.uncompilable").unwrap();
    assert!(!unsafe { is_enabled(engine, bad.as_ptr(), context.as_ptr(), context.len()) });

    unsafe { free_engine(engine) };
}
