//! Shared helpers for the FFI integration tests.
//!
//! Every test drives the crate through its real `extern "C"` surface, the same
//! way Overleash does, because that boundary is where the interesting failure
//! modes live.

#![allow(dead_code)]

use frontendengine::*;
use serde_json::Value;
use std::ffi::{c_char, c_void, CStr, CString};

/// Two flags, one on and one off, so tests can tell filtering from failure.
pub const STATE: &str = r#"{
  "version": 2,
  "features": [
    {"name": "flag.on",  "enabled": true,  "strategies": [{"name": "default"}]},
    {"name": "flag.off", "enabled": false, "strategies": [{"name": "default"}]}
  ]
}"#;

/// A state document that applies cleanly but cannot compile one toggle, so the
/// engine reports warnings alongside a successful update. `flag.on` is included
/// so tests can check the rest of the state still works.
pub const STATE_WITH_WARNINGS: &str = r#"{
  "version": 2,
  "features": [
    {"name": "flag.on", "enabled": true, "strategies": [{"name": "default"}]},
    {
      "name": "flag.uncompilable",
      "enabled": true,
      "strategies": [{
        "name": "default",
        "constraints": [
          {"contextName": "userId", "operator": "NOT_A_REAL_OPERATOR", "values": ["1"]}
        ]
      }]
    }
  ]
}"#;

/// An engine with `STATE` loaded. Caller must `free_engine`.
pub fn engine_with_state() -> *mut c_void {
    let engine = new_engine();
    let response = unsafe { take_state_json(engine, STATE) };
    assert_eq!(
        response["error_message"],
        Value::Null,
        "loading the fixture state should not report an error: {response}"
    );
    engine
}

/// Calls `take_state` with `json` and parses the response it hands back.
///
/// # Safety
/// `engine` must be null or a pointer from `new_engine`.
pub unsafe fn take_state_json(engine: *mut c_void, json: &str) -> Value {
    let json = CString::new(json).unwrap();
    unsafe { take_state_raw(engine, json.as_ptr()) }
}

/// # Safety
/// `engine` must be null or a pointer from `new_engine`; `json` must be null or
/// a valid C string.
pub unsafe fn take_state_raw(engine: *mut c_void, json: *const c_char) -> Value {
    unsafe {
        let raw = take_state(engine, json);
        assert!(!raw.is_null(), "take_state must always return a response");
        let parsed: Value = serde_json::from_str(CStr::from_ptr(raw).to_str().unwrap())
            .expect("take_state must return parseable JSON");
        free_response(raw as *mut c_char);
        parsed
    }
}

/// Encodes a `Context` proto the way the Go side would.
pub fn encode_context(user_id: Option<&str>) -> Vec<u8> {
    use prost::Message;
    let context = frontendengine::unleashengine::Context {
        user_id: user_id.map(str::to_string),
        ..Default::default()
    };
    context.encode_to_vec()
}

/// Result of a call that hands back a leaked protobuf buffer.
pub struct Buffer {
    pub ptr: *const u8,
    pub len: usize,
}

impl Buffer {
    pub fn is_null(&self) -> bool {
        self.ptr.is_null()
    }

    /// Copies the payload out and frees the buffer through the FFI free fn.
    pub fn take(self) -> Vec<u8> {
        assert!(!self.ptr.is_null(), "expected a payload, got NULL");
        unsafe {
            let bytes = std::slice::from_raw_parts(self.ptr, self.len).to_vec();
            free_rust_buffer(self.ptr as *mut u8, self.len);
            bytes
        }
    }

    /// Frees the buffer without inspecting it.
    pub fn discard(self) {
        if !self.ptr.is_null() {
            unsafe { free_rust_buffer(self.ptr as *mut u8, self.len) }
        }
    }
}

/// # Safety
/// Pointer arguments must satisfy the documented contract of `resolve`.
pub unsafe fn call_resolve(engine: *mut c_void, name: *const c_char, context: &[u8]) -> Buffer {
    let mut len: usize = 0;
    let ptr = unsafe { resolve(engine, name, context.as_ptr(), context.len(), &mut len) };
    Buffer { ptr, len }
}

/// # Safety
/// Pointer arguments must satisfy the documented contract of `resolve_all`.
pub unsafe fn call_resolve_all(
    engine: *mut c_void,
    context: &[u8],
    include_all: *const bool,
) -> Buffer {
    let mut len: usize = 0;
    let ptr = unsafe {
        resolve_all(
            engine,
            context.as_ptr(),
            include_all,
            context.len(),
            &mut len,
        )
    };
    Buffer { ptr, len }
}
