# SPDX-License-Identifier: Apache-2.0

FROM python:3.12-bookworm

COPY testdata/persistent_runners/fake_yosys.py /usr/local/bin/yosys

RUN chmod +x /usr/local/bin/yosys
