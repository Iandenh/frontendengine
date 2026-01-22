# Frontend engine for Overleash

This is the frontend engine, build with Yggdrasil core from Unleash.

Implementation based on the Yggdrasil-binding project:

## Create header file

```bash
cargo install --force cbindgen

cbindgen --config cbindgen.toml --lang c --crate frontendengine --output frontend_engine.h
```
