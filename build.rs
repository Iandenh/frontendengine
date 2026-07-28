use std::io::Result;

const PROTOS: &[&str] = &["src/proto/context.proto", "src/proto/toggles.proto"];

fn main() -> Result<()> {
    // Prefer a protoc the caller pointed us at, otherwise fall back to the
    // vendored binary so a plain `cargo build` works on a clean machine.
    // The proto3 `optional` fields in context.proto need protoc >= 3.15.
    if std::env::var_os("PROTOC").is_none() {
        if let Ok(protoc) = protoc_bin_vendored::protoc_bin_path() {
            std::env::set_var("PROTOC", protoc);
        }
    }

    for proto in PROTOS {
        println!("cargo:rerun-if-changed={proto}");
    }

    // Which protoc is used is an input to the build, so a change to it has to
    // rerun this script. Without this, Cargo only watches the files above.
    println!("cargo:rerun-if-env-changed=PROTOC");

    prost_build::compile_protos(PROTOS, &["src/"])?;
    Ok(())
}
