// SPDX-License-Identifier: Apache-2.0

mod action;
mod provenance;
mod queue;

pub(crate) use action::{
    action_id_to_hex, action_id_to_proto, action_spec_from_proto, action_spec_to_proto,
    compute_model_action_id_v2, driver_runtime_from_proto, driver_runtime_to_proto,
    yosys_runtime_to_proto,
};
pub(crate) use provenance::{decode_provenance, encode_provenance};
pub(crate) use queue::*;

#[allow(dead_code)]
pub(crate) mod v1 {
    include!(concat!(env!("OUT_DIR"), "/xlsynth.bvc.v1.rs"));
}

pub(crate) const FILE_DESCRIPTOR_SET: &[u8] =
    include_bytes!(concat!(env!("OUT_DIR"), "/xlsynth_bvc_descriptor.bin"));
pub(crate) const DEFAULT_RELEASE_CAMPAIGN: &[u8] =
    include_bytes!(concat!(env!("OUT_DIR"), "/release-qor-v1.pb"));

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
