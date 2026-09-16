FROM ubuntu/jdk:25-26.04_stable@sha256:01a3a41da3511b8d9f696d31044eb8ca1d4bfc2a537a93d131965fb62cdbb4e8

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
