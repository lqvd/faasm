ARG FAASM_VERSION=0.33.0

FROM ghcr.io/lqvd/cli:${FAASM_VERSION} AS collector

ARG FAASM_VERSION=0.33.0

ENV FAASM_GEN_PROTO_LOCAL=on
ENV FAASM_VERSION=${FAASM_VERSION}

# Build upload and codegen from the same fork/config as the worker image.
RUN cd /usr/local/code/faasm \
    && ./bin/create_venv.sh \
    && . venv/bin/activate \
    && inv dev.cc codegen_shared_obj \
    && inv dev.cc codegen_func \
    && inv dev.cc upload \
    && mkdir -p /tmp/rootfs \
    && /usr/local/code/faasm/bin/collect_runtime_deps.sh \
        --dest /tmp/rootfs \
        --bin /build/faasm/bin/codegen_shared_obj \
        --bin /build/faasm/bin/codegen_func \
        --bin /build/faasm/bin/upload \
        --emit-tar /tmp/rootfs.tar

FROM ubuntu:24.04

COPY --from=collector /tmp/rootfs.tar /rootfs.tar
RUN tar -xf /rootfs.tar -C / && rm /rootfs.tar

# Match the Faasm runtime layout expected by the entrypoint scripts.
RUN ln -s /build/faasm/release/bin /build/faasm/bin

# These are needed by upload/codegen entrypoints.
COPY --from=collector /usr/local/code/faasm/bin/entrypoint_upload.sh /entrypoint.sh
COPY --from=collector /usr/local/code/faasm/bin/entrypoint_codegen.sh /entrypoint_codegen.sh

RUN chmod +x /entrypoint.sh /entrypoint_codegen.sh

# Sanity checks: fail image build if the binary or shared libs are broken.
RUN test -x /build/faasm/bin/upload \
    && test -x /build/faasm/bin/codegen_func \
    && test -x /build/faasm/bin/codegen_shared_obj \
    && ldd /build/faasm/bin/upload | awk '/not found/ {nf=1} END{exit nf}'

ENTRYPOINT ["/entrypoint.sh"]
CMD ["/build/faasm/bin/upload"]