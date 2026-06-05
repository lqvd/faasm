ARG FAASM_VERSION=0.33.0
FROM ghcr.io/faasm/cli:${FAASM_VERSION}

ARG LQVD_BRANCH=main

WORKDIR /

RUN rm -rf /usr/local/code/faasm \
    && git clone \
        -b ${LQVD_BRANCH} \
        https://github.com/lqvd/faasm \
        /usr/local/code/faasm \
    && cd /usr/local/code/faasm \
    && git submodule update --init \
    && git config --global --add safe.directory /usr/local/code/faasm

WORKDIR /usr/local/code/faasm