# Frontend engine for Overleash

This is the frontend engine, build with Yggdrasil core from Unleash.

Implementation based on the Yggdrasil-binding project.

The crate builds a `cdylib` and a `staticlib` exposing a C ABI, which Overleash
consumes through cgo. `frontend_engine.h` describes that ABI and is committed;
CI regenerates it and fails if it has drifted.

## Building

```bash
cargo build --release
```

No system dependencies are needed. `protoc` is supplied by the
`protoc-bin-vendored` build dependency — `prost-build` requires protoc >= 3.15
for the proto3 `optional` fields in `context.proto`, which is newer than the
package most distributions ship. Set `PROTOC` to override the vendored binary.

## Tests

```bash
cargo test              # debug: includes extra UB precondition assertions
cargo test --release    # what actually ships
cargo clippy --all-targets -- -D warnings
```

`tests/buffer_layout.rs` installs a `#[global_allocator]` that checks each
deallocation against the size its allocation asked for. The default Unix
allocator hides layout mismatches — `System::dealloc` discards the layout and
calls `libc::free`, which recovers the block size itself — so a mismatch is
invisible until someone swaps in an allocator that trusts the layout
(jemalloc, mimalloc). That test makes it visible now.

## Regenerating the header

```bash
cargo install --force cbindgen

cbindgen --config cbindgen.toml --lang c --crate frontendengine --output frontend_engine.h
```

cbindgen prints one harmless warning about being unable to mangle the private
`RwLock<EngineState>` alias. Nothing is missing from the output; the engine
crosses the boundary as an opaque `void *`.

## The ABI contract

### Memory ownership

Every allocation crosses the boundary with exactly one matching free:

| Produced by                | Released with              |
| -------------------------- | -------------------------- |
| `new_engine()`             | `free_engine(ptr)`         |
| `take_state()`             | `free_response(ptr)`       |
| `resolve()`, `resolve_all()` | `free_rust_buffer(ptr, len)` |

`free_rust_buffer` must be given the same `len` that was written to `out_len`.
Buffers are handed over as boxed slices, so the allocation is exactly `len`
bytes and the free reconstructs it with a matching layout. All four free
functions accept `NULL`.

### Calling rules

* **Errors never panic.** Every entry point catches panics internally and
  reports failure through its return value. A panic crossing an `extern "C"`
  boundary is a non-unwinding panic that aborts the process, which the Go
  caller cannot recover from.
* **Do not build with `panic = "abort"`.** That defeats the panic guards and
  restores the abort-on-bad-input behaviour.
* **An empty context is valid.** Every field of the `Context` proto is
  optional, so `(context_data = NULL, context_len = 0)` is accepted — which is
  what cgo produces for an empty Go byte slice.
* **`is_enabled` fails closed.** A null engine, an unknown toggle, a bad
  context, or missing state all yield `false`.

### Return values

`take_state` always returns a non-null, parseable JSON `Response`. On success,
`value` lists the warnings reported while compiling the state:

```json
{"status_code": "Ok", "value": [], "error_message": null}
```

```json
{"status_code": "Ok",
 "value": ["some.flag: Failed to compile toggle, this will always be off ..."],
 "error_message": null}
```

**Warnings are not failures.** The state was applied; only the toggles named in
the warnings are affected, and those evaluate as off. A real Unleash feature
file routinely contains a toggle this engine version cannot compile, so treating
warnings as an error makes every refresh look like it failed. A *rejected*
update is the different case: the previous state stays in place and
`error_message` is set.

`status_code` is serialised as a **string** — `"Ok"`, `"NotFound"` or
`"Error"` — not as a number. The `-2 / -1 / 1` discriminants in the Rust enum
are ignored by serde for unit variants. On failure, `error_message` carries the
reason.

`resolve` and `resolve_all` return a protobuf buffer and write its length to
`out_len`. On failure they return `NULL` and write `0`.

### Known ABI limitations

These need a coordinated change on the Overleash side, so they are recorded
rather than fixed:

* `resolve` cannot distinguish *toggle not found* from *no state loaded yet*
  from *internal error* — all three are `(NULL, 0)`. Both non-error cases
  happen in normal operation; the second one on every boot before the first
  `take_state`.
* An empty `resolve_all` result (every toggle filtered out) encodes to zero
  protobuf bytes, so it is also indistinguishable from failure.
* `include_all` is a `const bool *` rather than a `bool`, so a null pointer is
  an error case that would not otherwise exist. Its position between
  `context_data` and `context_len` also separates the pointer from its length.
* The producing and freeing functions disagree about const-ness:
  `take_state` returns `const char *` but `free_response` takes `char *`, and
  `resolve`/`resolve_all` return `const uint8_t *` while `free_rust_buffer`
  takes `uint8_t *`. A C caller therefore has to discard `const` to free what
  it was given. cgo is unaffected — it maps both spellings to `*C.char` — so
  this costs nothing today, and aligning it would only move the cast into Rust,
  since `CString::from_raw` and `Box::from_raw` need a mutable pointer to take
  ownership. Worth fixing if a direct C consumer ever appears.
* `ResolvedToggle.project` is available from Yggdrasil but is not carried in
  `EvaluatedToggle`, so it never reaches the caller.
* The `Variant` and `ResolvedToggle` messages in `toggles.proto` are unused by
  this crate. They are kept in case the Go side generates from the same file.
* No metrics or impression events are exposed. Yggdrasil offers `count_toggle`,
  `count_variant`, `get_metrics`, `should_emit_impression_event` and
  `apply_delta`; none are reachable over the ABI, so flag usage cannot be
  reported back to Unleash and only full state updates are supported.
