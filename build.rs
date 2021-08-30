fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .extern_path(".eraftpb", "::raft::eraftpb")
        .compile(
            &["proto/app.proto"],
            &["proto/"], // specify the root location to search proto dependencies
        )?;
    Ok(())
}
