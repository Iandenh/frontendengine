use std::io::Result;
fn main() -> Result<()> {
    prost_build::compile_protos(&["src/proto/context.proto", "src/proto/toggles.proto"], &["src/"])?;
    Ok(())
}