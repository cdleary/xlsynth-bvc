// SPDX-License-Identifier: Apache-2.0

use std::error::Error;
use std::fs;
use std::io::Write;
use std::path::PathBuf;
use std::process::{Command, Stdio};

fn main() -> Result<(), Box<dyn Error>> {
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    let out_dir = PathBuf::from(std::env::var_os("OUT_DIR").ok_or("OUT_DIR is not set")?);

    let mut config = prost_build::Config::new();
    config.protoc_executable(&protoc);
    config.btree_map(["."]);
    config.file_descriptor_set_path(out_dir.join("xlsynth_bvc_descriptor.bin"));
    config.compile_protos(
        &[
            "proto/xlsynth/bvc/v1/common.proto",
            "proto/xlsynth/bvc/v1/action.proto",
            "proto/xlsynth/bvc/v1/queue.proto",
            "proto/xlsynth/bvc/v1/store.proto",
            "proto/xlsynth/bvc/v1/provenance.proto",
            "proto/xlsynth/bvc/v1/publication.proto",
            "proto/xlsynth/bvc/v1/campaign.proto",
            "proto/xlsynth/bvc/v1/analysis.proto",
            "proto/xlsynth/bvc/v1/deployment.proto",
        ],
        &["proto"],
    )?;

    let campaign_text = fs::read("campaigns/release-qor-v1.textproto")?;
    let mut child = Command::new(&protoc)
        .args([
            "--proto_path=proto",
            "--encode=xlsynth.bvc.v1.CampaignSpec",
            "proto/xlsynth/bvc/v1/campaign.proto",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()?;
    child
        .stdin
        .as_mut()
        .ok_or("protoc campaign stdin unavailable")?
        .write_all(&campaign_text)?;
    let output = child.wait_with_output()?;
    if !output.status.success() {
        return Err(format!(
            "protoc failed to compile release campaign textproto: {}",
            String::from_utf8_lossy(&output.stderr)
        )
        .into());
    }
    fs::write(out_dir.join("release-qor-v1.pb"), output.stdout)?;

    println!("cargo:rerun-if-changed=proto/xlsynth/bvc/v1/common.proto");
    println!("cargo:rerun-if-changed=proto/xlsynth/bvc/v1/action.proto");
    println!("cargo:rerun-if-changed=proto/xlsynth/bvc/v1/queue.proto");
    println!("cargo:rerun-if-changed=proto/xlsynth/bvc/v1/store.proto");
    println!("cargo:rerun-if-changed=proto/xlsynth/bvc/v1/provenance.proto");
    println!("cargo:rerun-if-changed=proto/xlsynth/bvc/v1/publication.proto");
    println!("cargo:rerun-if-changed=proto/xlsynth/bvc/v1/campaign.proto");
    println!("cargo:rerun-if-changed=proto/xlsynth/bvc/v1/analysis.proto");
    println!("cargo:rerun-if-changed=proto/xlsynth/bvc/v1/deployment.proto");
    println!("cargo:rerun-if-changed=campaigns/release-qor-v1.textproto");
    println!("cargo:rerun-if-changed=proto/README.md");
    Ok(())
}
