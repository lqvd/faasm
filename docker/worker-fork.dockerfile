FROM ghcr.io/lqvd/cli:0.33.0 AS collector

RUN cd /usr/local/code/faasm \
    && ./bin/create_venv.sh \
    && source venv/bin/activate \
    && inv dev.cmake --build Release --disable-spinlock --sgx Disabled \
    && inv dev.cc codegen_shared_obj \
    && inv dev.cc codegen_func \
    && inv dev.cc pool_runner \
    && mkdir -p /tmp/rootfs \
    && /usr/local/code/faasm/bin/collect_runtime_deps.sh \
        --dest /tmp/rootfs \
        --bin /build/faasm/bin/codegen_shared_obj \
        --bin /build/faasm/bin/codegen_func \
        --bin /build/faasm/bin/pool_runner \
        --emit-tar /tmp/rootfs.tar

FROM ubuntu:24.04
COPY --from=collector /tmp/rootfs.tar /rootfs.tar
RUN tar -xf /rootfs.tar -C / && rm /rootfs.tar
RUN apt update && apt install -y dnsutils
RUN ln -s /build/faasm/release/bin /build/faasm/bin
COPY --from=collector /usr/local/faasm/runtime_root /usr/local/faasm/runtime_root
COPY --from=collector /usr/local/code/faasm/bin/entrypoint_worker.sh /usr/local/code/faasm/bin/entrypoint_worker.sh
RUN groupadd -g 1001 faasm && useradd -u 1001 -g 1001 faasm
ENTRYPOINT ["/usr/local/code/faasm/bin/entrypoint_worker.sh"]
CMD ["/build/faasm/bin/pool_runner"]