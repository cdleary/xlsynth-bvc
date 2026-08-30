# SPDX-License-Identifier: Apache-2.0

FROM python:3.12-bookworm

ARG BVC_RUNTIME_FINGERPRINT
LABEL org.xlsynth-bvc.runtime-fingerprint="${BVC_RUNTIME_FINGERPRINT}"

COPY testdata/persistent_runners/fake_yosys.py /usr/local/bin/yosys

RUN chmod +x /usr/local/bin/yosys
