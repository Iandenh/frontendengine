//! C ABI over the Unleash Yggdrasil engine, consumed by Overleash through cgo.
//!
//! # Rules for every entry point
//!
//! * No panic may escape. A panic crossing an `extern "C"` boundary is a
//!   non-unwinding panic, which aborts the process — the caller cannot recover
//!   from it. Every entry point therefore runs inside [`guarded`].
//! * Every pointer from the caller is treated as untrusted and checked.
//! * A `(data, len)` pair with `len == 0` is accepted with a null `data`,
//!   because cgo hands over a nil pointer for an empty Go slice and an empty
//!   `Context` is valid input.
//!
//! # Memory ownership
//!
//! * `new_engine` → free with `free_engine`.
//! * `take_state` → free with `free_response`.
//! * `resolve` / `resolve_all` → free with `free_rust_buffer(ptr, len)`, passing
//!   back the same `len` that was written to `out_len`.
//!
//! Buffers are handed over as boxed slices so that the allocation is exactly
//! `len` bytes and `free_rust_buffer` can reconstruct it with a matching layout.
//!
//! This crate must not be built with `panic = "abort"`: that would defeat the
//! [`guarded`] wrappers and restore the abort-on-bad-input behaviour.

use prost::Message;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::collections::HashMap;
use std::ffi::{c_char, c_void, CStr, CString};
use std::fmt;
use std::fmt::{Display, Formatter};
use std::mem::forget;
use std::panic::AssertUnwindSafe;
use std::str::Utf8Error;
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};
use unleash_yggdrasil::{Context, EngineState, EvalWarning, ResolvedToggle, UpdateMessage};
use unleashengine::{EvaluatedToggle, EvaluatedVariant, VariantPayload};

pub mod unleashengine {
    include!(concat!(env!("OUT_DIR"), "/unleashengine.rs"));
}

/// Only `take_state` needs exclusive access; every evaluation path takes `&self`
/// on the engine, so readers run concurrently.
type RawPointerDataType = RwLock<EngineState>;
type ManagedEngine = Arc<RawPointerDataType>;

use unleashengine::Context as OtherContext;

impl From<OtherContext> for Context {
    fn from(value: OtherContext) -> Self {
        Context {
            user_id: value.user_id,
            session_id: value.session_id,
            environment: value.environment,
            app_name: value.app_name,
            current_time: value.current_time,
            remote_address: value.remote_address,
            // Proto3 maps are not optional, so an absent map arrives as an
            // empty one. Yggdrasil treats `Some(empty)` and `None` alike.
            properties: Some(value.properties),
        }
    }
}

#[derive(Debug)]
enum Error {
    Utf8,
    Null,
    InvalidJson(String),
    InvalidProto(String),
    /// The engine has not been given any toggle state yet.
    StateNotLoaded,
    /// No toggle by that name is known.
    ToggleNotFound,
    /// A panic was caught before it could cross the FFI boundary.
    Panic,
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            Error::Utf8 => write!(f, "Detected a non UTF-8 string in the input, this is a serious issue and you should report this as a bug."),
            Error::Null => write!(f, "Null error detected, this is a serious issue and you should report this as a bug."),
            Error::InvalidJson(message) => write!(f, "Failed to parse JSON: {message}"),
            Error::InvalidProto(message) => write!(f, "Invalid Proto Buf input detected: {message}"),
            Error::StateNotLoaded => write!(f, "The engine has not received any feature toggle state yet."),
            Error::ToggleNotFound => write!(f, "No feature toggle with that name is known to the engine; it may not exist, or the engine may not have received state yet."),
            Error::Panic => write!(f, "A panic was caught inside the engine, this is a serious issue and you should report this as a bug."),
        }
    }
}

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Error::InvalidJson(e.to_string())
    }
}

impl From<Utf8Error> for Error {
    fn from(_: Utf8Error) -> Self {
        Error::Utf8
    }
}

#[derive(Serialize, Deserialize, PartialEq, Eq)]
enum ResponseCode {
    Error = -2,
    NotFound = -1,
    Ok = 1,
}

#[derive(Serialize, Deserialize)]
struct Response<T> {
    status_code: ResponseCode,
    value: Option<T>,
    error_message: Option<String>,
}

impl<T> From<Result<Option<T>, Error>> for Response<T> {
    fn from(value: Result<Option<T>, Error>) -> Self {
        match value {
            Ok(Some(enabled)) => Response {
                status_code: ResponseCode::Ok,
                value: Some(enabled),
                error_message: None,
            },
            Ok(None) => Response {
                status_code: ResponseCode::NotFound,
                value: None,
                error_message: None,
            },
            Err(e) => Response {
                status_code: ResponseCode::Error,
                value: None,
                error_message: Some(e.to_string()),
            },
        }
    }
}

/// Runs `body`, converting a panic into `fallback` so that nothing unwinds past
/// the `extern "C"` boundary.
fn guarded<T>(fallback: T, body: impl FnOnce() -> T) -> T {
    std::panic::catch_unwind(AssertUnwindSafe(body)).unwrap_or(fallback)
}

#[unsafe(no_mangle)]
pub extern "C" fn new_engine() -> *mut c_void {
    guarded(std::ptr::null_mut(), || {
        let engine = RwLock::new(EngineState::default());
        Arc::into_raw(Arc::new(engine)) as *mut c_void
    })
}

/// # Safety
/// `engine_ptr` must be null, or a pointer returned by [`new_engine`] that has
/// not already been freed. Must not be called concurrently with any other call
/// using the same pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn free_engine(engine_ptr: *mut c_void) {
    if engine_ptr.is_null() {
        return;
    }
    guarded((), || {
        drop(unsafe { Arc::from_raw(engine_ptr as *const RawPointerDataType) });
    })
}

/// Replaces the engine's toggle state from a JSON `UpdateMessage`.
///
/// Always returns a non-null JSON [`Response`], which the caller must release
/// with [`free_response`].
///
/// On success `value` holds the warnings reported while compiling the state —
/// an empty list when everything compiled. Warnings are *not* failures: the
/// state was applied, and only the toggles named in them are affected (they
/// evaluate as off). A rejected update instead leaves the previous state in
/// place and sets `error_message`.
///
/// # Safety
/// `engine_ptr` must be null or from [`new_engine`]; `json_ptr` must be null or
/// a valid NUL-terminated C string that stays alive for the call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn take_state(
    engine_ptr: *mut c_void,
    json_ptr: *const c_char,
) -> *const c_char {
    let result = guarded(Err(Error::Panic), || {
        let engine = unsafe { get_engine(engine_ptr) }?;
        let toggles: UpdateMessage = unsafe { get_json(json_ptr) }?;

        let warnings = {
            let mut state = write_lock(&engine);
            state.take_state(toggles)
        };

        Ok(Some(
            warnings
                .unwrap_or_default()
                .iter()
                .map(describe_warning)
                .collect::<Vec<String>>(),
        ))
    });

    result_to_json_ptr(result)
}

/// Renders a compile warning as one line naming the toggle it concerns.
fn describe_warning(warning: &EvalWarning) -> String {
    format!("{}: {}", warning.toggle_name, warning.message)
}

/// Evaluates every known toggle.
///
/// On success returns a `EvaluatedToggleList` protobuf and writes its length to
/// `out_len`; the buffer must be released with [`free_rust_buffer`]. On failure
/// returns null and writes `0`.
///
/// # Safety
/// `engine_ptr` must be null or from [`new_engine`]. `context_data` must be
/// readable for `context_len` bytes, or null when `context_len` is 0.
/// `include_all` and `out_len` must be null or valid for the call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn resolve_all(
    engine_ptr: *mut c_void,
    context_data: *const u8,
    include_all: *const bool,
    context_len: usize,
    out_len: *mut usize,
) -> *const u8 {
    let result = guarded(Err(Error::Panic), || {
        let engine = unsafe { get_engine(engine_ptr) }?;
        let context = unsafe { decode_context(context_data, context_len) }?;
        let include_all = unsafe { deref_bool(include_all) }?;

        // `resolve_all` returns `None` only when no state has been loaded.
        let resolved = {
            let state = read_lock(&engine);
            state
                .resolve_all(&context, &None)
                .ok_or(Error::StateNotLoaded)?
        };

        encode(&into_list(resolved, include_all))
    });

    unsafe { emit_buffer(result, out_len) }
}

/// Evaluates a single toggle by name.
///
/// On success returns an `EvaluatedToggle` protobuf and writes its length to
/// `out_len`; the buffer must be released with [`free_rust_buffer`]. On failure
/// — including when no such toggle is known — returns null and writes `0`.
///
/// # Safety
/// `engine_ptr` must be null or from [`new_engine`]. `toggle_name_ptr` must be
/// null or a valid C string. `context_data` must be readable for `context_len`
/// bytes, or null when `context_len` is 0. `out_len` must be null or valid.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn resolve(
    engine_ptr: *mut c_void,
    toggle_name_ptr: *const c_char,
    context_data: *const u8,
    context_len: usize,
    out_len: *mut usize,
) -> *const u8 {
    let result = guarded(Err(Error::Panic), || {
        let engine = unsafe { get_engine(engine_ptr) }?;
        let context = unsafe { decode_context(context_data, context_len) }?;

        unsafe {
            with_str(toggle_name_ptr, |toggle_name| {
                let resolved = {
                    let state = read_lock(&engine);
                    state
                        .resolve(toggle_name, &context, &None)
                        .ok_or(Error::ToggleNotFound)?
                };

                encode(&into_toggle(toggle_name.to_string(), resolved))
            })
        }
    });

    unsafe { emit_buffer(result, out_len) }
}

/// Returns whether a toggle is enabled, failing closed: any error, unknown
/// toggle, or missing state yields `false`.
///
/// # Safety
/// `engine_ptr` must be null or from [`new_engine`]. `toggle_name_ptr` must be
/// null or a valid C string. `context_data` must be readable for `context_len`
/// bytes, or null when `context_len` is 0.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn is_enabled(
    engine_ptr: *mut c_void,
    toggle_name_ptr: *const c_char,
    context_data: *const u8,
    context_len: usize,
) -> bool {
    guarded(false, || {
        let result: Result<bool, Error> = (|| {
            let engine = unsafe { get_engine(engine_ptr) }?;
            let context = unsafe { decode_context(context_data, context_len) }?;

            unsafe {
                with_str(toggle_name_ptr, |toggle_name| {
                    let state = read_lock(&engine);
                    Ok(state.is_enabled(toggle_name, &context, &None))
                })
            }
        })();

        result.unwrap_or(false)
    })
}

/// Releases a buffer returned by [`resolve`] or [`resolve_all`].
///
/// # Safety
/// `ptr`/`len` must be a pair produced by [`resolve`] or [`resolve_all`] and not
/// yet freed, or `ptr` must be null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn free_rust_buffer(ptr: *mut u8, len: usize) {
    if ptr.is_null() {
        return;
    }
    guarded((), || {
        // Buffers are handed out as boxed slices, so the allocation is exactly
        // `len` bytes and this layout matches the one used to allocate it.
        drop(unsafe { Box::from_raw(std::ptr::slice_from_raw_parts_mut(ptr, len)) });
    })
}

/// Releases a response returned by [`take_state`].
///
/// # Safety
/// `response_ptr` must be null or a pointer from [`take_state`] that has not
/// already been freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn free_response(response_ptr: *mut c_char) {
    if response_ptr.is_null() {
        return;
    }
    guarded((), || {
        drop(unsafe { CString::from_raw(response_ptr) });
    })
}

/// Clones the `Arc` behind an opaque engine pointer without consuming it.
///
/// # Safety
/// `engine_ptr` must be null or a live pointer from [`new_engine`].
unsafe fn get_engine(engine_ptr: *mut c_void) -> Result<ManagedEngine, Error> {
    if engine_ptr.is_null() {
        return Err(Error::Null);
    }
    let arc_instance = unsafe { Arc::from_raw(engine_ptr as *const RawPointerDataType) };

    let cloned_arc = arc_instance.clone();
    forget(arc_instance);

    Ok(cloned_arc)
}

/// A poisoned lock only means some previous caller panicked; the state itself is
/// still the best we have, so keep using it rather than failing every call.
fn read_lock(lock: &RwLock<EngineState>) -> RwLockReadGuard<'_, EngineState> {
    lock.read().unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn write_lock(lock: &RwLock<EngineState>) -> RwLockWriteGuard<'_, EngineState> {
    lock.write()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// # Safety
/// `json_ptr` must be null or a valid NUL-terminated C string.
unsafe fn get_json<T: DeserializeOwned>(json_ptr: *const c_char) -> Result<T, Error> {
    unsafe {
        with_str(json_ptr, |json| {
            serde_json::from_str(json).map_err(Error::from)
        })
    }
}

/// Passes the C string at `ptr` to `f` as a `&str`.
///
/// The borrow is confined to the closure, so it cannot outlive the caller's
/// buffer.
///
/// # Safety
/// `ptr` must be null or a valid NUL-terminated C string that stays alive and
/// unmodified for the duration of the call.
unsafe fn with_str<R>(
    ptr: *const c_char,
    f: impl FnOnce(&str) -> Result<R, Error>,
) -> Result<R, Error> {
    if ptr.is_null() {
        return Err(Error::Null);
    }
    let value = unsafe { CStr::from_ptr(ptr) }.to_str()?;
    f(value)
}

/// Reads a caller-supplied `bool` out-of-band value.
///
/// # Safety
/// `ptr` must be null or point to an initialised `bool`.
unsafe fn deref_bool(ptr: *const bool) -> Result<bool, Error> {
    if ptr.is_null() {
        return Err(Error::Null);
    }
    Ok(unsafe { *ptr })
}

/// Borrows a caller-supplied `(data, len)` pair.
///
/// A null `data` is accepted when `len` is 0: cgo passes a nil pointer for an
/// empty Go slice, and an empty `Context` is valid input.
///
/// # Safety
/// When `len > 0`, `data` must be readable for `len` bytes and stay alive for
/// the duration of the call.
unsafe fn as_slice<'a>(data: *const u8, len: usize) -> Result<&'a [u8], Error> {
    if len == 0 {
        return Ok(&[]);
    }
    if data.is_null() {
        return Err(Error::Null);
    }
    Ok(unsafe { std::slice::from_raw_parts(data, len) })
}

/// # Safety
/// See [`as_slice`].
unsafe fn decode_context(data: *const u8, len: usize) -> Result<Context, Error> {
    let bytes = unsafe { as_slice(data, len) }?;
    let proto = OtherContext::decode(bytes).map_err(|e| Error::InvalidProto(e.to_string()))?;
    Ok(proto.into())
}

fn encode<M: Message>(message: &M) -> Result<Vec<u8>, Error> {
    let mut buf = Vec::with_capacity(message.encoded_len());
    message
        .encode(&mut buf)
        .map_err(|e| Error::InvalidProto(e.to_string()))?;
    Ok(buf)
}

/// Hands a protobuf payload to the caller, or reports failure as `(null, 0)`.
///
/// The payload is converted to a boxed slice first so its allocation is exactly
/// `len` bytes, which is the layout [`free_rust_buffer`] frees it with.
///
/// # Safety
/// `out_len` must be null or valid for a `usize` write.
unsafe fn emit_buffer(result: Result<Vec<u8>, Error>, out_len: *mut usize) -> *const u8 {
    if out_len.is_null() {
        // With nowhere to report the length the payload would be unreachable,
        // so there is nothing useful to hand over — and nothing to leak.
        return std::ptr::null();
    }

    match result {
        Ok(bytes) => {
            let boxed = bytes.into_boxed_slice();
            unsafe { *out_len = boxed.len() };
            Box::into_raw(boxed) as *const u8
        }
        Err(_) => {
            unsafe { *out_len = 0 };
            std::ptr::null()
        }
    }
}

/// Renders a result as a JSON C string. Total by construction: the caller is
/// promised a non-null, parseable response on every path.
fn result_to_json_ptr<T: Serialize>(result: Result<Option<T>, Error>) -> *const c_char {
    /// Used if serialising the real response fails. A C string literal, so
    /// building it cannot fail either.
    const FALLBACK: &CStr =
        cr#"{"status_code":"Error","value":null,"error_message":"Failed to serialise response"}"#;

    let response: Response<T> = result.into();

    // `CString::new` rejects interior NUL bytes. serde escapes them as `\u0000`
    // inside JSON strings, so this should be unreachable — but the caller gets a
    // valid response either way rather than a panic.
    match serde_json::to_string(&response).map(CString::new) {
        Ok(Ok(json)) => json.into_raw(),
        _ => FALLBACK.to_owned().into_raw(),
    }
}

fn into_variant(variant: unleash_yggdrasil::ExtendedVariantDef) -> EvaluatedVariant {
    EvaluatedVariant {
        name: variant.name,
        enabled: variant.enabled,
        payload: variant.payload.map(|p| VariantPayload {
            r#type: p.payload_type,
            value: p.value,
        }),
        feature_enabled: variant.feature_enabled,
        // Yggdrasil only tracks one flag here; the duplicate proto field is
        // kept so older Overleash builds keep decoding.
        old_feature_enabled: variant.feature_enabled,
    }
}

fn into_toggle(name: String, resolved: ResolvedToggle) -> EvaluatedToggle {
    EvaluatedToggle {
        name,
        enabled: resolved.enabled,
        impression_data: resolved.impression_data,
        variant: Some(into_variant(resolved.variant)),
    }
}

fn into_list(
    map: HashMap<String, ResolvedToggle>,
    include_all: bool,
) -> unleashengine::EvaluatedToggleList {
    let toggles = map
        .into_iter()
        // Filter before building the payload so discarded toggles cost nothing.
        .filter(|(_, resolved)| include_all || resolved.enabled)
        .map(|(name, resolved)| into_toggle(name, resolved))
        .collect();

    unleashengine::EvaluatedToggleList { toggles }
}

#[cfg(test)]
// Tests are where assertions belong; the bans exist to keep them out of the
// FFI layer, not out of the test module.
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn guarded_returns_the_fallback_when_the_body_panics() {
        let value = guarded(-1, || panic!("engine exploded"));

        assert_eq!(value, -1, "a panic must be converted, not propagated");
    }

    #[test]
    fn guarded_returns_the_body_value_when_nothing_panics() {
        assert_eq!(guarded(-1, || 7), 7);
    }

    #[test]
    fn guarded_converts_a_panic_carrying_a_non_string_payload() {
        let value = guarded(-1, || std::panic::panic_any(42u8));

        assert_eq!(value, -1);
    }

    #[test]
    fn a_zero_length_slice_is_accepted_with_a_null_pointer() {
        let slice = unsafe { as_slice(std::ptr::null(), 0) }.expect("valid input");

        assert!(slice.is_empty());
    }

    #[test]
    fn a_null_pointer_with_a_nonzero_length_is_rejected() {
        let result = unsafe { as_slice(std::ptr::null(), 8) };

        assert!(matches!(result, Err(Error::Null)));
    }

    #[test]
    fn encoded_buffers_have_no_spare_capacity() {
        // `free_rust_buffer` frees `len` bytes, so the allocation must be `len`
        // bytes. Boxing enforces it, but encoding exactly is what makes boxing
        // free rather than a reallocation.
        let list = unleashengine::EvaluatedToggleList {
            toggles: vec![EvaluatedToggle {
                name: "some.flag".into(),
                enabled: true,
                impression_data: false,
                variant: None,
            }],
        };

        let buf = encode(&list).expect("encodes");

        assert_eq!(buf.len(), buf.capacity());
    }

    #[test]
    fn into_list_keeps_only_enabled_toggles_by_default() {
        let list = into_list(resolved_pair(), false);

        let names: Vec<_> = list.toggles.iter().map(|t| t.name.as_str()).collect();
        assert_eq!(names, ["on"]);
    }

    #[test]
    fn into_list_keeps_everything_when_asked() {
        let list = into_list(resolved_pair(), true);

        let mut names: Vec<_> = list.toggles.iter().map(|t| t.name.as_str()).collect();
        names.sort_unstable();
        assert_eq!(names, ["off", "on"]);
    }

    #[test]
    fn a_response_always_serialises() {
        let ptr = result_to_json_ptr::<()>(Err(Error::Panic));
        assert!(!ptr.is_null());

        let json = unsafe { CStr::from_ptr(ptr) }.to_str().unwrap().to_owned();
        unsafe { free_response(ptr as *mut c_char) };

        let parsed: serde_json::Value = serde_json::from_str(&json).expect("parseable");
        assert!(parsed["error_message"].is_string());
    }

    fn resolved_pair() -> HashMap<String, ResolvedToggle> {
        HashMap::from([
            ("on".to_string(), resolved(true)),
            ("off".to_string(), resolved(false)),
        ])
    }

    fn resolved(enabled: bool) -> ResolvedToggle {
        ResolvedToggle {
            enabled,
            impression_data: false,
            project: "default".into(),
            variant: unleash_yggdrasil::ExtendedVariantDef {
                name: "disabled".into(),
                payload: None,
                enabled: false,
                feature_enabled: enabled,
            },
        }
    }
}
