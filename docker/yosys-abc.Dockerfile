FROM ubuntu:24.04

ARG DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    yosys \
    yosys-abc \
    && rm -rf /var/lib/apt/lists/*
