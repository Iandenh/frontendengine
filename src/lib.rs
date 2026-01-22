use std::collections::HashMap;
use prost::Message;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::ffi::{c_char, c_void, CStr, CString};
use std::{fmt};
use std::fmt::{Display, Formatter};
use std::mem::forget;
use std::str::Utf8Error;
use std::sync::{Arc, Mutex, MutexGuard};
use unleash_yggdrasil::{
     Context, EngineState, EvalWarning, ExtendedVariantDef,
    ResolvedToggle, UpdateMessage,
};
use unleashengine::{EvaluatedToggle, EvaluatedVariant, VariantPayload};

pub mod unleashengine {
    include!(concat!(env!("OUT_DIR"), "/unleashengine.rs"));
}

type RawPointerDataType = Mutex<EngineState>;
type ManagedEngine = Arc<RawPointerDataType>;

pub struct EvaluatedToggleList(unleashengine::EvaluatedToggleList);


use unleashengine::Context as OtherContext;

impl Into<Context> for OtherContext {
    fn into(self) -> Context {
        Context {
            user_id: self.user_id,
            session_id: self.session_id,
            environment: self.environment,
            app_name: self.app_name,
            current_time: self.current_time,
            remote_address: self.remote_address,
            properties: Option::from(self.properties),
        }
    }
}

#[derive(Debug)]
enum Error {
    Utf8Error,
    NullError,
    InvalidJson(String),
    PartialUpdate(Vec<EvalWarning>),
    InvalidProto(String),
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            Error::Utf8Error => write!(f, "Detected a non UTF-8 string in the input, this is a serious issue and you should report this as a bug."),
            Error::NullError => write!(f, "Null error detected, this is a serious issue and you should report this as a bug."),
            Error::InvalidJson(message) => write!(f, "Failed to parse JSON: {}", message),
            Error::PartialUpdate(messages) => write!(
                f,
                "Engine state was updated but warnings were reported, this may result in some flags evaluating in unexpected ways, please report this: {:?}",
                messages
            ),
            Error::InvalidProto(message) => write!(f, "Invalid Proto Buf input detected: {}", message),
        }
    }
}

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Error::InvalidJson(e.to_string())
    }
}


impl From<Utf8Error> for Error{
    fn from(_: Utf8Error) -> Self {
        Error::Utf8Error
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

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ResolvedToggleState {
    pub enabled: bool,
    pub impression_data: bool,
    pub project: String,
    pub variant: ExtendedVariantDef,
}

#[unsafe(no_mangle)]
pub extern "C" fn new_engine() -> *mut c_void {
    let engine = Mutex::new(EngineState::default());
    let arc = Arc::new(engine);
    Arc::into_raw(arc) as *mut c_void
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn free_engine(engine_ptr: *mut c_void) {
    unsafe {
        if engine_ptr.is_null() {
            return;
        }
        drop(Arc::from_raw(engine_ptr as *const Mutex<EngineState>));
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn take_state(
    engine_ptr: *mut c_void,
    json_ptr: *const c_char,
) -> *const c_char {
    let result  = {
        let guard = get_engine(engine_ptr).unwrap();
        let mut engine = recover_lock(&guard);

        let toggles: UpdateMessage = get_json(json_ptr).unwrap();

        if let Some(warnings) = engine.take_state(toggles) {
            Err(Error::PartialUpdate(warnings))
        } else {
            Ok(Some(()))
        }
    };

    result_to_json_ptr(result)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn resolve_all(
    engine_ptr: *mut c_void,
    context_data: *const u8,
    include_all: *const bool,
    context_len: usize,
    out_len: *mut usize,
) -> *const u8 {
    let result: Result<Vec<u8>, Error> = (|| {
        let guard = get_engine(engine_ptr)?;
        let engine = recover_lock(&guard);

        let input_slice = std::slice::from_raw_parts(context_data, context_len);
        let context_proto = OtherContext::decode(input_slice)
            .map_err(|_| Error::InvalidJson("Invalid Proto Context".into()))?;

        let context: Context = context_proto.into();
        let resolved = engine.resolve_all(&context, &None)
            .ok_or(Error::NullError)?;

        let list: EvaluatedToggleList = into_list(resolved, *include_all);

        // Serialize to Protobuf bytes
        let mut buf = Vec::new();
        list.0.encode(&mut buf).map_err(|_| Error::InvalidJson("Error".into()))?;
        Ok(buf)
    })();

    match result {
        Ok(bytes) => {
            unsafe {
                *out_len = bytes.len();
                let ptr = bytes.as_ptr();
                std::mem::forget(bytes); // Prevent Rust from freeing memory before Go reads it
                ptr
            }
        }
        Err(_) => {
            unsafe { *out_len = 0; std::ptr::null() }
        }
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn resolve(
    engine_ptr: *mut c_void,
    toggle_name_ptr: *const c_char,
    context_data: *const u8,
    context_len: usize,
    out_len: *mut usize,
) -> *const u8 {
    let result: Result<Vec<u8>, Error> = (|| {
        let guard = get_engine(engine_ptr)?;
        let engine = recover_lock(&guard);

        // 1. Handle Inputs
        let toggle_name = get_str(toggle_name_ptr)?;
        let input_slice = std::slice::from_raw_parts(context_data, context_len);
        let context_proto = OtherContext::decode(input_slice)
            .map_err(|_| Error::InvalidProto("Invalid Context".into()))?;
        let context: Context = context_proto.into();

        // 2. Resolve Logic
        let resolved = engine.resolve(toggle_name, &context, &None)
            .ok_or(Error::NullError)?;

        let evaluated = EvaluatedToggle {
            name: toggle_name.to_string(),
            enabled: resolved.enabled,
            impression_data: resolved.impression_data,
            variant: Some(unleashengine::EvaluatedVariant {
                name: resolved.variant.name,
                enabled: resolved.variant.enabled,
                feature_enabled: resolved.variant.feature_enabled,
                old_feature_enabled: resolved.variant.feature_enabled,
                payload: resolved.variant.payload.map(|p| VariantPayload {
                    r#type: p.payload_type,
                    value: p.value,
                }),
            }),
        };

        // 4. Serialize
        let mut buf = Vec::new();
        evaluated.encode(&mut buf).map_err(|_| Error::InvalidJson("Error".into()))?;
        Ok(buf)
    })();

    // 5. Return binary pointer and length to Go
    match result {
        Ok(bytes) => {
            *out_len = bytes.len();
            let ptr = bytes.as_ptr();
            std::mem::forget(bytes); // Hand over ownership to Go (must be freed later!)
            ptr
        }
        Err(_) => {
            *out_len = 0;
            std::ptr::null()
        }
    }
}

unsafe fn get_engine(engine_ptr: *mut c_void) -> Result<ManagedEngine, Error> {
    if engine_ptr.is_null() {
        return Err(Error::NullError);
    }
    let arc_instance = Arc::from_raw(engine_ptr as *const RawPointerDataType);

    let cloned_arc = arc_instance.clone();
    forget(arc_instance);

    Ok(cloned_arc)
}

fn recover_lock<T>(lock: &Mutex<T>) -> MutexGuard<'_, T> {
    lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

unsafe fn get_json<T: DeserializeOwned>(json_ptr: *const c_char) -> Result<T, Error> {
    unsafe {
        let json_str = get_str(json_ptr)?;
        serde_json::from_str(json_str).map_err(Error::from)
    }
}


unsafe fn get_str<'a>(ptr: *const c_char) -> Result<&'a str, Error> {
    if ptr.is_null() {
        Err(Error::NullError)
    } else {
        unsafe { CStr::from_ptr(ptr).to_str().map_err(Error::from) }
    }
}

fn result_to_json_ptr<T: Serialize>(result: Result<Option<T>, Error>) -> *mut c_char {
    let response: Response<T> = result.into();
    let json_string = serde_json::to_string(&response).unwrap();
    CString::new(json_string).unwrap().into_raw()
}

#[no_mangle]
pub unsafe extern "C" fn free_rust_buffer(ptr: *mut u8, len: usize) {
    if ptr.is_null() {
        return;
    }
    // Reconstruct the Vec so its Drop implementation can run
    let _ = Vec::from_raw_parts(ptr, len, len);
}

#[no_mangle]
pub unsafe extern "C" fn free_response(response_ptr: *mut c_char) {
    if response_ptr.is_null() {
        return;
    }
    drop(CString::from_raw(response_ptr));
}

fn into_list(map: HashMap<String, ResolvedToggle>, include_all: bool) -> EvaluatedToggleList {
    let toggles = map
        .into_iter()
        .map(|(name, resolved)| EvaluatedToggle {
            name,
            enabled: resolved.enabled,
            impression_data: resolved.impression_data,
            variant: Some(EvaluatedVariant {
                name: resolved.variant.name,
                enabled: resolved.variant.enabled,
                payload: resolved.variant.payload.map(|p| VariantPayload {
                    r#type:  p.payload_type,
                    value: p.value,
                }),
                feature_enabled: resolved.variant.feature_enabled,
                old_feature_enabled: resolved.variant.feature_enabled
            }),
        })
        .filter(|x| {
            include_all || x.enabled
        })
        .collect();

    EvaluatedToggleList(unleashengine::EvaluatedToggleList { toggles })
}