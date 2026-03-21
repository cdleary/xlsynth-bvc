FROM ubuntu:24.04

ARG DEBIAN_FRONTEND=noninteractive
ARG DRIVER_CRATE_VERSION=0.29.0
ARG RUST_TOOLCHAIN=nightly-2026-02-12
ARG BITWUZLA_RELEASE_TAG=bitwuzla-binaries-b29041fbbe6318cb4c19a6e11c7616efc4cb4d32

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
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

RUN if [ "${DRIVER_CRATE_VERSION}" = "latest" ]; then \
      cargo +"${RUST_TOOLCHAIN}" install --locked --features with-bitwuzla-system,with-easy-smt xlsynth-driver ; \
    else \
      cargo +"${RUST_TOOLCHAIN}" install --locked --version "${DRIVER_CRATE_VERSION}" --features with-bitwuzla-system,with-easy-smt xlsynth-driver ; \
    fi

COPY third_party/xlsynth-crate/v0.29.0/scripts/download_release.py /opt/xlsynth/download_release.py
RUN chmod +x /opt/xlsynth/download_release.py
