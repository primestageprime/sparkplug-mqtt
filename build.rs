use std::io::Result;

fn main() -> Result<()> {
    if let Ok(path) = protoc_bin_vendored::protoc_bin_path() {
        // SAFETY: build scripts run single-threaded before compilation.
        unsafe { std::env::set_var("PROTOC", path) };
    }
    prost_build::compile_protos(&["src/sparkplugb.proto"], &["src/"])?;
    Ok(())
}
