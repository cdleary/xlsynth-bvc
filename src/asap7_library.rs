// SPDX-License-Identifier: Apache-2.0

//! Compact library metadata is a resource; full Liberty inputs are verified
//! execution-cache materializations and are never needed to render a site.

use anyhow::{Context, Result, bail};
use flate2::read::GzDecoder;
use prost::Message;
use regex::Regex;
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::fs;
use std::io::Read;
use std::path::{Component, Path, PathBuf};
use std::process::Command;

use crate::techmap_proto::wire::Asap7LibrarySummary;

const SUMMARY: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/asap7-summary.pb"));
const MATERIALIZER: &str = "scripts/materialize_asap7.py";
const SOURCE_REPOSITORY: &str = "https://github.com/siliconcompiler/lambdapdk";
const MEMBER_NAMES: [&str; 4] = [
    "asap7sc7p5t_AO_RVT_SS_nldm.lib",
    "asap7sc7p5t_INVBUF_RVT_SS_nldm.lib",
    "asap7sc7p5t_OA_RVT_SS_nldm.lib",
    "asap7sc7p5t_SIMPLE_RVT_SS_nldm.lib",
];

pub(crate) fn summary() -> Result<Asap7LibrarySummary> {
    let summary = Asap7LibrarySummary::decode(SUMMARY)?;
    if summary.encode_to_vec() != SUMMARY {
        bail!("noncanonical ASAP7 library summary");
    }
    validate_summary(&summary)?;
    Ok(summary)
}

fn validate_summary(summary: &Asap7LibrarySummary) -> Result<()> {
    if summary.record_version != 1
        || summary.library_name != "asap7_rvt_ss"
        || summary.source_repository != SOURCE_REPOSITORY
        || summary.source_commit.len() != 40
        || !summary
            .source_commit
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
        || summary.source_directory.is_empty()
        || !Path::new(&summary.source_directory)
            .components()
            .all(|part| matches!(part, Component::Normal(_)))
        || summary.source_directory.contains('\\')
        || summary.archive_sha256.len() != 32
        || summary.merged_liberty_sha256.len() != 32
        || summary.preparation_script_sha256.len() != 32
        || !(1..=128 * 1024 * 1024).contains(&summary.archive_bytes)
        || summary.members.len() != MEMBER_NAMES.len()
        || summary.threshold_voltage != "RVT"
        || summary.process_corner != "SS"
        || !summary.nominal_voltage_volts.is_finite()
        || summary.nominal_voltage_volts <= 0.0
        || !summary.nominal_temperature_celsius.is_finite()
        || summary.time_unit != "1ps"
        || summary.capacitance_unit != "1ff"
    {
        bail!("invalid ASAP7 library summary identity or corner");
    }
    let mut cells = 0_u64;
    for (member, expected_name) in summary.members.iter().zip(MEMBER_NAMES) {
        if member.filename != expected_name
            || member.sha256.len() != 32
            || member.gzip_sha256.len() != 32
            || !(1..=128 * 1024 * 1024).contains(&member.bytes)
            || !(1..=128 * 1024 * 1024).contains(&member.gzip_bytes)
            || member.cell_count == 0
        {
            bail!("invalid ASAP7 library summary member");
        }
        cells = cells
            .checked_add(member.cell_count)
            .context("ASAP7 cell count overflow")?;
    }
    if cells != summary.cell_count {
        bail!("ASAP7 library summary cell count mismatch");
    }
    Ok(())
}

fn digest(bytes: &[u8]) -> Vec<u8> {
    Sha256::digest(bytes).to_vec()
}

fn scalar<'a>(text: &'a str, name: &str) -> Result<&'a str> {
    let pattern = Regex::new(&format!(r"(?m)^\s*{}\s*:\s*([^;]+);", regex::escape(name)))?;
    let value = pattern
        .captures(text)
        .and_then(|capture| capture.get(1))
        .with_context(|| format!("ASAP7 library lacks {name}"))?;
    Ok(value.as_str().trim().trim_matches('"'))
}

fn validate_bundle(path: &Path, summary: &Asap7LibrarySummary) -> Result<()> {
    let mut bytes = Vec::new();
    fs::File::open(path)
        .context("reading cached ASAP7 input bundle")?
        .take(summary.archive_bytes + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 != summary.archive_bytes || digest(&bytes) != summary.archive_sha256 {
        bail!("cached ASAP7 input bundle checksum or size mismatch");
    }
    let mut archive = tar::Archive::new(GzDecoder::new(bytes.as_slice()));
    let mut entries = archive.entries()?;
    let cell = Regex::new(r"(?m)^  cell\s*\(([^)]*)\)")?;
    let capacitance = Regex::new(r"capacitive_load_unit\s*\(\s*([^,]+),\s*([^)]+)\)")?;
    let mut seen = BTreeSet::new();
    for member in &summary.members {
        let mut entry = entries
            .next()
            .context("ASAP7 input bundle is missing a member")??;
        if entry.path()?.as_ref() != Path::new(&member.filename)
            || !entry.header().entry_type().is_file()
            || entry.size() != member.bytes
        {
            bail!("ASAP7 input bundle member identity mismatch");
        }
        let mut raw = Vec::new();
        entry.read_to_end(&mut raw)?;
        if digest(&raw) != member.sha256 {
            bail!("ASAP7 input bundle member checksum mismatch");
        }
        let text = std::str::from_utf8(&raw)?;
        let names: Vec<_> = cell
            .captures_iter(text)
            .map(|row| row[1].to_string())
            .collect();
        let units = capacitance
            .captures(text)
            .context("ASAP7 library lacks capacitance units")?;
        if names.len() as u64 != member.cell_count
            || names.iter().any(|name| !seen.insert(name.clone()))
            || scalar(text, "time_unit")? != summary.time_unit
            || format!("{}{}", units[1].trim(), units[2].trim()) != summary.capacitance_unit
            || scalar(text, "nom_voltage")?.parse::<f64>()? != summary.nominal_voltage_volts
            || scalar(text, "nom_temperature")?.parse::<f64>()?
                != summary.nominal_temperature_celsius
        {
            bail!("ASAP7 derived library summary disagrees with source members");
        }
    }
    if entries.next().is_some() {
        bail!("ASAP7 input bundle contains an undeclared member");
    }
    Ok(())
}

/// A preseeded bundle at this content-addressed path supports fully offline
/// execution. Corrupt entries fail closed instead of being silently replaced.
pub(crate) fn materialize(
    resource_root: &Path,
    shared_cache: &Path,
    summary: &Asap7LibrarySummary,
) -> Result<PathBuf> {
    validate_summary(summary)?;
    let directory = shared_cache
        .join("inputs/asap7")
        .join(hex::encode(&summary.archive_sha256));
    let _guard = crate::techmap_corpus::lock(&directory.with_extension("lock"))?;
    fs::create_dir_all(&directory)?;
    let bundle = directory.join("bundle.tar.gz");
    if bundle.exists() {
        validate_bundle(&bundle, summary)?;
        return Ok(bundle);
    }
    let attempt = crate::techmap_corpus::attempt_directory(&directory)?;
    let staged = attempt.join("bundle.tar.gz");
    let mut command = Command::new("python3");
    command
        .arg(resource_root.join(MATERIALIZER))
        .arg("--output")
        .arg(&staged);
    for member in &summary.members {
        let url = format!(
            "https://raw.githubusercontent.com/siliconcompiler/lambdapdk/{}/{}/{}.gz",
            summary.source_commit, summary.source_directory, member.filename
        );
        command
            .arg("--member")
            .arg(&member.filename)
            .arg(url)
            .arg(hex::encode(&member.gzip_sha256))
            .arg(member.gzip_bytes.to_string())
            .arg(hex::encode(&member.sha256))
            .arg(member.bytes.to_string());
    }
    crate::techmap_corpus::command_output(&mut command)
        .context("materializing pinned ASAP7 input bundle")?;
    validate_bundle(&staged, summary)?;
    fs::rename(staged, &bundle)?;
    Ok(bundle)
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::{Compression, write::GzEncoder};

    struct Fixture(PathBuf);
    impl Fixture {
        fn new() -> Self {
            let root = std::env::temp_dir().join(format!(
                "bvc-asap7-library-{}-{}",
                std::process::id(),
                chrono::Utc::now().timestamp_nanos_opt().unwrap()
            ));
            fs::create_dir_all(&root).unwrap();
            Self(root)
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn fixture_bundle() -> (Asap7LibrarySummary, Vec<u8>) {
        let mut summary = summary().unwrap();
        summary.cell_count = 4;
        let mut archive = tar::Builder::new(GzEncoder::new(Vec::new(), Compression::best()));
        for (index, member) in summary.members.iter_mut().enumerate() {
            let text = format!(
                "library (fixture) {{\n  time_unit : \"1ps\";\n  capacitive_load_unit (1,ff);\n  nom_voltage : 0.63;\n  nom_temperature : 100;\n  cell (fixture_{index}) {{}}\n}}\n"
            );
            member.bytes = text.len() as u64;
            member.sha256 = digest(text.as_bytes());
            member.cell_count = 1;
            let mut header = tar::Header::new_ustar();
            header.set_size(member.bytes);
            header.set_mode(0o644);
            header.set_cksum();
            archive
                .append_data(&mut header, &member.filename, text.as_bytes())
                .unwrap();
        }
        let bytes = archive.into_inner().unwrap().finish().unwrap();
        summary.archive_bytes = bytes.len() as u64;
        summary.archive_sha256 = digest(&bytes);
        (summary, bytes)
    }

    #[test]
    fn compact_summary_preserves_historical_identity_and_derived_corner() {
        let summary = summary().unwrap();
        assert!(SUMMARY.len() < 2048);
        assert_eq!(summary.cell_count, 169);
        assert_eq!(
            summary
                .members
                .iter()
                .map(|member| member.cell_count)
                .collect::<Vec<_>>(),
            vec![42, 37, 34, 56]
        );
        assert_eq!(summary.nominal_voltage_volts, 0.63);
        assert_eq!(summary.nominal_temperature_celsius, 100.0);
        assert_eq!(summary.time_unit, "1ps");
        assert_eq!(summary.capacitance_unit, "1ff");
        assert_eq!(
            hex::encode(summary.archive_sha256),
            "30d3c357ccbcc58d8f3aacdc0006b511a34329568e4eea4a1a03d37f39fa835a"
        );
        assert_eq!(
            hex::encode(summary.merged_liberty_sha256),
            "9faa3906c56c021af6eb677ec14b2e5cc4a62cca6e547916e3c8c212b0ed8778"
        );
        assert_eq!(
            summary.preparation_script_sha256,
            digest(include_bytes!("../scripts/prepare_asap7.py"))
        );
    }

    #[test]
    fn validates_summary_shape_and_derived_metadata_against_bundle() {
        let fixture = Fixture::new();
        let (summary, bytes) = fixture_bundle();
        let bundle = fixture.0.join("bundle.tar.gz");
        fs::write(&bundle, &bytes).unwrap();
        validate_summary(&summary).unwrap();
        validate_bundle(&bundle, &summary).unwrap();

        let mut wrong = summary.clone();
        wrong.members[0].cell_count += 1;
        assert!(validate_summary(&wrong).is_err());
        wrong.cell_count += 1;
        validate_summary(&wrong).unwrap();
        assert!(validate_bundle(&bundle, &wrong).is_err());
        let mut wrong = summary.clone();
        wrong.nominal_voltage_volts = 0.7;
        assert!(validate_bundle(&bundle, &wrong).is_err());
        let mut wrong = summary.clone();
        wrong.members.swap(0, 1);
        assert!(validate_summary(&wrong).is_err());
        let mut wrong = summary.clone();
        wrong.members[0].sha256 = vec![0; 32];
        assert!(validate_bundle(&bundle, &wrong).is_err());
    }

    #[test]
    fn preseeded_cache_is_offline_and_rejects_corruption_without_replacement() {
        let fixture = Fixture::new();
        let (summary, bytes) = fixture_bundle();
        let directory = fixture
            .0
            .join("inputs/asap7")
            .join(hex::encode(&summary.archive_sha256));
        fs::create_dir_all(&directory).unwrap();
        let bundle = directory.join("bundle.tar.gz");
        fs::write(&bundle, &bytes).unwrap();
        // No resource directory or download helper exists: a valid preseed
        // needs only the compact summary and cache contents.
        let absent_resources = fixture.0.join("absent-resources");
        assert_eq!(
            materialize(&absent_resources, &fixture.0, &summary).unwrap(),
            bundle
        );
        let mut corrupt = bytes;
        corrupt[0] ^= 1;
        fs::write(&bundle, &corrupt).unwrap();
        assert!(materialize(&absent_resources, &fixture.0, &summary).is_err());
        assert_eq!(fs::read(bundle).unwrap(), corrupt);
    }

    #[test]
    fn deterministic_materializer_checks_sources_and_archive_format() {
        let output = Command::new("python3")
            .arg(Path::new(env!("CARGO_MANIFEST_DIR")).join("scripts/materialize_asap7_test.py"))
            .output()
            .expect("starting deterministic materializer fixture tests");
        assert!(
            output.status.success(),
            "materializer tests failed:\n{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}
