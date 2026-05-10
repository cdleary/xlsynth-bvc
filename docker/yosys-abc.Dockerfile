# SPDX-License-Identifier: Apache-2.0

FROM ubuntu:24.04

ARG DEBIAN_FRONTEND=noninteractive
ARG YOSYS_COMMIT=1f023432681c159885f0b834a2e0717e67c4c115
ARG YOSYS_COMMIT_PREFIX=1f023432
ARG YOSYS_BUILD_JOBS=8

RUN apt-get update && apt-get install -y --no-install-recommends \
    bison \
    build-essential \
    ca-certificates \
    flex \
    git \
    gawk \
    libffi-dev \
    libfl-dev \
    libreadline-dev \
    pkg-config \
    python3 \
    tcl-dev \
    yosys-abc \
    zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*

RUN git clone https://github.com/YosysHQ/yosys.git /tmp/yosys \
    && cd /tmp/yosys \
    && git checkout "${YOSYS_COMMIT}" \
    && git submodule update --init --recursive \
    && make config-gcc \
    && make -j"${YOSYS_BUILD_JOBS}" ABCEXTERNAL=yosys-abc \
    && make install PREFIX=/opt/yosys-head ABCEXTERNAL=yosys-abc \
    && rm -rf /tmp/yosys

ENV PATH="/opt/yosys-head/bin:${PATH}"

RUN yosys -V | grep -q "${YOSYS_COMMIT_PREFIX}" \
    && command -v yosys-abc \
    && python3 --version
