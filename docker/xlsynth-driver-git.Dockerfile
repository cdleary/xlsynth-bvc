# SPDX-License-Identifier: Apache-2.0

FROM ubuntu:24.04

ARG DRIVER_GIT_REPOSITORY=https://github.com/xlsynth/xlsynth-crate
ARG DRIVER_GIT_COMMIT
ARG DRIVER_SOURCE_BUILD_RECIPE
ARG BVC_RUNTIME_FINGERPRINT
ARG DEBIAN_FRONTEND=noninteractive
ARG RUST_TOOLCHAIN=nightly-2026-02-12
ARG BITWUZLA_RELEASE_TAG=bitwuzla-binaries-b29041fbbe6318cb4c19a6e11c7616efc4cb4d32

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    git \
    python3 \
    python3-requests \
    protobuf-compiler \
    libc++1 \
    libc++abi1 \
    build-essential \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

RUN curl https://sh.rustup.rs -sSf | sh -s -- -y --profile minimal
ENV PATH="/root/.cargo/bin:${PATH}"
RUN rustup toolchain install "${RUST_TOOLCHAIN}" --profile minimal

RUN set -eux; \
    base_url="https://github.com/xlsynth/boolector-build/releases/download/${BITWUZLA_RELEASE_TAG}"; \
    tmpdir="$(mktemp -d)"; \
    cd "${tmpdir}"; \
    for lib in libbitwuzla libbitwuzlabb libbitwuzlabv libbitwuzlals libcadical; do \
      curl -fsSLO "${base_url}/${lib}-rocky8.so"; \
      curl -fsSLO "${base_url}/${lib}-rocky8.so.sha256"; \
      expected="$(awk '{print $1}' "${lib}-rocky8.so.sha256")"; \
      echo "${expected}  ${lib}-rocky8.so" | sha256sum -c -; \
      install -m 0644 "${lib}-rocky8.so" "/usr/local/lib/${lib}-rocky8.so"; \
      ln -sf "/usr/local/lib/${lib}-rocky8.so" "/usr/local/lib/${lib}.so"; \
    done; \
    echo "/usr/local/lib" > /etc/ld.so.conf.d/usr-local-lib.conf; \
    ldconfig; \
    rm -rf "${tmpdir}"

ENV LIBRARY_PATH="/usr/local/lib"
ENV LD_LIBRARY_PATH="/usr/local/lib"
ENV RUSTFLAGS="-L native=/usr/local/lib"

RUN set -eux; \
    printf '%s' "${DRIVER_GIT_COMMIT}" | grep -Eq '^[0-9a-f]{40}$'; \
    test "${DRIVER_GIT_REPOSITORY}" = "https://github.com/xlsynth/xlsynth-crate"; \
    git init /opt/xlsynth-crate; \
    cd /opt/xlsynth-crate; \
    git remote add origin "${DRIVER_GIT_REPOSITORY}"; \
    git fetch --depth 1 origin "${DRIVER_GIT_COMMIT}"; \
    git checkout --detach "${DRIVER_GIT_COMMIT}"; \
    test "$(git rev-parse HEAD)" = "${DRIVER_GIT_COMMIT}"; \
    if test ! -f Cargo.lock; then cargo +"${RUST_TOOLCHAIN}" generate-lockfile; fi; \
    test -s Cargo.lock; \
    install -d /opt/xlsynth; \
    sha256sum Cargo.lock | tee /opt/xlsynth/source-Cargo.lock.sha256; \
    cargo +"${RUST_TOOLCHAIN}" install --locked \
      --path xlsynth-driver \
      --features with-bitwuzla-system,with-easy-smt

LABEL org.xlsynth-bvc.runtime-fingerprint="${BVC_RUNTIME_FINGERPRINT}" \
      org.xlsynth-bvc.driver-source-repository="${DRIVER_GIT_REPOSITORY}" \
      org.xlsynth-bvc.driver-source-commit="${DRIVER_GIT_COMMIT}" \
      org.xlsynth-bvc.driver-source-build-recipe="${DRIVER_SOURCE_BUILD_RECIPE}"

COPY third_party/xlsynth-crate/v0.29.0/scripts/download_release.py /opt/xlsynth/download_release.py
RUN chmod +x /opt/xlsynth/download_release.py
