// SPDX-License-Identifier: Apache-2.0

mod action;
mod provenance;
mod queue;
mod release_input;

pub(crate) use action::{
    action_id_to_hex, action_id_to_proto, action_spec_from_proto, action_spec_to_proto,
    compute_model_action_id_v2, digest_from_hex, digest_to_hex, driver_runtime_from_proto,
    driver_runtime_to_proto,
};
pub(crate) use provenance::{decode_provenance, encode_provenance};
pub(crate) use queue::*;
pub(crate) use release_input::release_input_for_dso_version;

#[allow(dead_code)]
pub(crate) mod v1 {
    include!(concat!(env!("OUT_DIR"), "/xlsynth.bvc.v1.rs"));
}

pub(crate) const FILE_DESCRIPTOR_SET: &[u8] =
    include_bytes!(concat!(env!("OUT_DIR"), "/xlsynth_bvc_descriptor.bin"));
// Descriptor set immediately before the wire-compatible optional
// DriverRuntime.source_revision field was added.
pub(crate) const PRE_SOURCE_REVISION_SCHEMA_DESCRIPTOR_SHA256: &str =
    "3bf017ecf749a0fb5a9c1718e726cbee0bbdfc69883faf608f7db631cee596fc";
// Descriptor set immediately before fixed-corpus progression publication
// evidence was added. That change added publication-only messages and did not
// change any protobuf record persisted in an artifact store.
pub(crate) const PRE_PROGRESSION_EVIDENCE_SCHEMA_DESCRIPTOR_SHA256: &str =
    "ced8e57a652e20d8fd25b36eeb7d9e5aa0436a5aa5bf06acb773fb856ba1a022";
// Descriptor set immediately before static-site release metadata evidence was
// added. This also added publication-only messages and did not change records
// persisted in an artifact store.
pub(crate) const PRE_STATIC_SITE_RELEASE_METADATA_SCHEMA_DESCRIPTOR_SHA256: &str =
    "a3cb7fba40f06c82b69abe643f2c7f4daeac69afdf86849af8be131f4c81e0ec";
pub(crate) const COMPATIBLE_PRIOR_SCHEMA_DESCRIPTOR_SHA256S: &[&str] = &[
    PRE_SOURCE_REVISION_SCHEMA_DESCRIPTOR_SHA256,
    PRE_PROGRESSION_EVIDENCE_SCHEMA_DESCRIPTOR_SHA256,
    PRE_STATIC_SITE_RELEASE_METADATA_SCHEMA_DESCRIPTOR_SHA256,
];
pub(crate) const DEFAULT_RELEASE_CAMPAIGN: &[u8] =
    include_bytes!(concat!(env!("OUT_DIR"), "/release-qor-v1.pb"));
pub(crate) const DEFAULT_RELEASE_INPUTS: &[u8] =
    include_bytes!(concat!(env!("OUT_DIR"), "/release-inputs-v1.pb"));
pub(crate) const RELEASE_PROGRESSION_IR_SCHEDULING_POLICY: &[u8] =
    include_bytes!(concat!(env!("OUT_DIR"), "/release-progression-ir-v1.pb"));
pub(crate) const MFFC_PROGRESSION_IR_SCHEDULING_POLICY: &[u8] =
    include_bytes!(concat!(env!("OUT_DIR"), "/mffc-progression-ir-v1.pb"));

#[cfg(test)]
mod tests {
    use prost::Message;

    use super::v1::Sha256Digest;

    #[test]
    fn sha256_digest_message_round_trips() {
        let original = Sha256Digest {
            value: vec![0x5a; 32],
        };
        let encoded = original.encode_to_vec();
        let decoded = Sha256Digest::decode(encoded.as_slice()).expect("decode digest");
        assert_eq!(decoded, original);
    }

    #[test]
    fn descriptor_set_is_embedded() {
        assert!(!super::FILE_DESCRIPTOR_SET.is_empty());
    }
}
