# SPDX-License-Identifier: Apache-2.0

FROM python:3.12-bookworm

ARG BVC_RUNTIME_FINGERPRINT
LABEL org.xlsynth-bvc.runtime-fingerprint="${BVC_RUNTIME_FINGERPRINT}"

COPY testdata/persistent_runners/fake_xlsynth_driver.py /usr/local/bin/xlsynth-driver
COPY testdata/persistent_runners/fake_protoc.py /usr/local/bin/protoc

RUN chmod +x /usr/local/bin/xlsynth-driver /usr/local/bin/protoc
