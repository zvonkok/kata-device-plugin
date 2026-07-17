fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Bake the build's git commit into the binary so the startup log
    // identifies exactly what is running, including uncommitted changes
    // ("-dirty").  Image builds have no .git in the build context; the
    // Makefile passes GIT_SHA through docker instead.
    let sha = std::env::var("GIT_SHA")
        .ok()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| {
            let git = |args: &[&str]| {
                std::process::Command::new("git")
                    .args(args)
                    .output()
                    .ok()
                    .filter(|o| o.status.success())
            };
            match git(&["rev-parse", "--short", "HEAD"]) {
                Some(o) => {
                    let sha = String::from_utf8_lossy(&o.stdout).trim().to_owned();
                    // diff-index succeeds only on a clean tree.
                    match git(&["diff-index", "--quiet", "HEAD", "--"]) {
                        Some(_) => sha,
                        None => format!("{sha}-dirty"),
                    }
                }
                None => "unknown".into(),
            }
        });
    println!("cargo:rustc-env=GIT_SHA={sha}");
    println!("cargo:rerun-if-env-changed=GIT_SHA");
    // Keep the sha honest across commits/staging without rebuilding otherwise.
    println!("cargo:rerun-if-changed=.git/HEAD");
    println!("cargo:rerun-if-changed=.git/index");

    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&["proto/deviceplugin.proto"], &["proto"])?;
    Ok(())
}
