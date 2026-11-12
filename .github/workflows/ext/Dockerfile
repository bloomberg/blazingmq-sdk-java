FROM ubuntu/jdk:25-26.04_stable@sha256:1dde130a4a7be1abaa9c5275b67d8b38d3381d26399959c3404fc9c3e618a5c3

USER root

# Avoid interactive prompts during apt install
ENV DEBIAN_FRONTEND=noninteractive

# 'maven' install fails without reinstalling 'debconf'
RUN apt-get update \
    && apt-get install -y --reinstall debconf \
    && apt-get install -y --no-install-recommends maven \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace

COPY LICENSE /workspace
COPY license-header /workspace
COPY pom.xml /workspace
COPY bmq-sdk /workspace/bmq-sdk
COPY bmq-examples /workspace/bmq-examples

RUN mvn --batch-mode clean -Dmaven.test.skip=true -Dspotbugs.skip=true install
