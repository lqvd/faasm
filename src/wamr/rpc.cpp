#include <faabric/executor/ExecutorContext.h>
#include <faabric/rpc/rpc.h>
#include <faabric/rpc/RpcContext.h>
#include <faabric/rpc/RpcContextRegistry.h>
#include <faabric/rpc/RpcServer.h>
#include <faabric/transport/common.h>
#include <faabric/util/bytes.h>
#include <faabric/util/logging.h>
#include <wamr/WAMRModuleMixin.h>
#include <wamr/WAMRWasmModule.h>
#include <wamr/native.h>
#include <wasm/migration.h>

#include <chrono>
#include <cstring>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <wasm_export.h>

using namespace faabric::rpc;

namespace wasm {

namespace {

// ------
// wasm pointer helpers
// ------
std::string getStringFromWasm(WAMRWasmModule* module, int32_t* wasmPtr)
{
    module->validateNativePointer(wasmPtr, 1);

    char* strPtr = reinterpret_cast<char*>(wasmPtr);
    size_t maxLen =
      module->getMemorySizeBytes() -
      (reinterpret_cast<uint8_t*>(wasmPtr) - module->getMemoryBase());
    size_t strLen = strnlen(strPtr, maxLen);

    module->validateNativePointer(wasmPtr, strLen + 1);
    return std::string(strPtr, strLen);
}

uint8_t* getBufferFromWasm(WAMRWasmModule* module,
                           int32_t* wasmPtr,
                           int32_t len)
{
    if (len > 0) {
        module->validateNativePointer(wasmPtr, len);
    }
    return reinterpret_cast<uint8_t*>(wasmPtr);
}

void writeStringToWasm(WAMRWasmModule* module,
                       const std::string& src,
                       int32_t* outOffsetPtr,
                       int32_t* outLenPtr)
{
    void* nativePtr = nullptr;
    uint32_t wasmOffset =
        module->wasmModuleMalloc(src.size() + 1, &nativePtr);

    if (nativePtr == nullptr && !src.empty()) {
        throw std::runtime_error("Wasm heap allocation failed");
    }

    if (!src.empty()) {
        std::memcpy(nativePtr, src.data(), src.size());
    }
    if (nativePtr != nullptr) {
        static_cast<char*>(nativePtr)[src.size()] = '\0';
    }

    *outOffsetPtr = static_cast<int32_t>(wasmOffset);
    *outLenPtr = static_cast<int32_t>(src.size());
}

// ------
// per-call setup
// ------

std::shared_ptr<RpcContext> getCurrentContext()
{
    int32_t msgId = faabric::executor::ExecutorContext::get()->getMsg().id();
    return getRpcContextRegistry().getContext(msgId);
}

int32_t handleException(WAMRWasmModule* module,
                        const std::exception& e,
                        const char* op)
{
    SPDLOG_ERROR("RPC {} failed: {}", op, e.what());
    std::runtime_error ex(e.what());
    module->doThrowException(ex);
    return static_cast<int32_t>(Rpc_StatusCode::INTERNAL);
}

} // namespace

// ------
// channel lifecycle
// ------

static int32_t Rpc_ChannelCreate_wrapper(wasm_exec_env_t,
                                         int32_t* targetUriPtr,
                                         int32_t* outChannelIdPtr)
{
    auto* module = getExecutingWAMRModule();
    auto ctx = getCurrentContext();
    if (!ctx) {
        SPDLOG_WARN("RPC channel create rejected (no context)");
        return static_cast<int32_t>(Rpc_StatusCode::INTERNAL);
    }

    module->validateNativePointer(outChannelIdPtr, sizeof(int32_t));
    std::string targetUri = getStringFromWasm(module, targetUriPtr);

    try {
        *outChannelIdPtr = ctx->createChannel(targetUri);
        return static_cast<int32_t>(Rpc_StatusCode::OK);
    } catch (const std::exception& e) {
        return handleException(module, e, "channel create");
    }
}

static int32_t Rpc_ChannelClose_wrapper(wasm_exec_env_t, int32_t channelId)
{
    auto* module = getExecutingWAMRModule();
    auto ctx = getCurrentContext();
    if (!ctx) {
        SPDLOG_WARN("RPC channel close rejected (no context)");
        return static_cast<int32_t>(Rpc_StatusCode::INTERNAL);
    }

    try {
        ctx->closeChannel(channelId);
        return static_cast<int32_t>(Rpc_StatusCode::OK);
    } catch (const std::exception& e) {
        return handleException(module, e, "channel close");
    }
}

// ------
// unary call
// ------

static int32_t __faasm_rpc_unary_start_wrapper(wasm_exec_env_t,
                                               int32_t channelId,
                                               int32_t* methodStrPtr,
                                               int32_t* reqBufPtr,
                                               int32_t reqLen,
                                               uint32_t* outRequestIdPtr,
                                               int32_t timeoutMs)
{
    auto* module = getExecutingWAMRModule();

    if (reqLen < 0) {
        SPDLOG_WARN("RPC request length must be non-negative");
        return static_cast<int32_t>(Rpc_StatusCode::INVALID_ARGUMENT);
    }

    module->validateNativePointer(outRequestIdPtr, sizeof(int32_t));
    std::string methodName = getStringFromWasm(module, methodStrPtr);
    uint8_t* reqBuf = getBufferFromWasm(module, reqBufPtr, reqLen);

    auto ctx = getCurrentContext();
    if (!ctx) {
        SPDLOG_WARN("RPC channel close rejected (no context)");
        return static_cast<int32_t>(Rpc_StatusCode::INTERNAL);
    }

    try {
        uint32_t requestId =
          ctx->startUnary(channelId, methodName, reqBuf, reqLen, timeoutMs);
        *outRequestIdPtr = requestId;

        // Register the [request, msgId] mapping so the server thread can
        // route the response back to the righ context.
        int32_t msgId =
          faabric::executor::ExecutorContext::get()->getMsg().id();

        SPDLOG_TRACE("RPC unary start ch={} method={} req={}",
                     channelId, methodName, requestId);
        return static_cast<int32_t>(Rpc_StatusCode::OK);
    } catch (const std::exception& e) {
        return handleException(module, e, "unary start");
    }
}

static int32_t __faasm_rpc_test_response_wrapper(wasm_exec_env_t,
                                                 uint32_t requestId)
{
    auto ctx = getCurrentContext();
    if (!ctx) {
        SPDLOG_WARN("RPC test_response: no context");
        return 0;
    }

    try {
        return ctx->testResponse(requestId) ? 1 : 0;
    } catch (const std::exception& e) {
        SPDLOG_ERROR("RPC test_response failed: {}", e.what());
        return 0;
    }
}

static void __faasm_rpc_wait_migratable_wrapper(wasm_exec_env_t,
                                                uint32_t requestId,
                                                int32_t wasmResumeTarget,
                                                int32_t frameOffset)
{
    SPDLOG_DEBUG("S - faasm_rpc_wait_migratable {} {} {}",
                 requestId , wasmResumeTarget, frameOffset);
    auto* module = getExecutingWAMRModule();
    auto ctx = getCurrentContext();
    if (!ctx) {
        SPDLOG_ERROR("RPC no context for executing message");
        std::runtime_error ex("RPC wait: no context for executing message");
        module->doThrowException(ex);
        return;
    }

    while (true) {
        wasm::doMigrationPoint(wasmResumeTarget, std::to_string(frameOffset));

        if (ctx->testResponse(requestId)) {
            SPDLOG_INFO("RPC wait: got response. Returning.");
            return;
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
}

static int32_t __faasm_rpc_get_response_wrapper(wasm_exec_env_t,
                                                uint32_t requestId,
                                                int32_t* outRespBufOffsetPtr,
                                                int32_t* outRespLenPtr)
{
    auto* module = getExecutingWAMRModule();

    auto ctx = getCurrentContext();
    if (!ctx) {
        SPDLOG_WARN("RPC channel close rejected (no context)");
        return static_cast<int32_t>(Rpc_StatusCode::INTERNAL);
    }

    module->validateNativePointer(outRespBufOffsetPtr, sizeof(int32_t));
    module->validateNativePointer(outRespLenPtr, sizeof(int32_t));

    try {
        faabric::RpcResponse resp;
        if (!ctx->getResponse(requestId, resp)) {
            SPDLOG_WARN("RPC get_response called before ready (req={})",
                        requestId);
            return static_cast<int32_t>(Rpc_StatusCode::UNAVAILABLE);
        }

        // Response consumed... drop the [request, msgId] mapping.
        getRpcContextRegistry().clearRequest(requestId);

        if (resp.statuscode() != Rpc_StatusCode::OK) {
            return resp.statuscode();
        }

        const std::string& payload = resp.payload();
        void* nativePtr = nullptr;
        uint32_t wasmOffset =
          module->wasmModuleMalloc(payload.size(), &nativePtr);

        if (nativePtr == nullptr && !payload.empty()) {
            std::runtime_error ex("Wasm heap allocation failed");
            module->doThrowException(ex);
            return static_cast<int32_t>(Rpc_StatusCode::RESOURCE_EXHAUSTED);
        }

        if (!payload.empty()) {
            std::memcpy(nativePtr, payload.data(), payload.size());
        }

        *outRespBufOffsetPtr = static_cast<int32_t>(wasmOffset);
        *outRespLenPtr = static_cast<int32_t>(payload.size());
        return static_cast<int32_t>(Rpc_StatusCode::OK);
    } catch (const std::exception& e) {
        return handleException(module, e, "get response");
    }
}

static int32_t __faasm_rpc_send_response_wrapper(wasm_exec_env_t,
                                                 uint32_t requestId,
                                                 int32_t* replyHostPtr,
                                                 int32_t replyPort,
                                                 int32_t statusCode,
                                                 int32_t* payloadPtr,
                                                 int32_t payloadLen,
                                                 int32_t* errorMsgPtr,
                                                 int32_t errorMsgLen)
{
    auto* module = getExecutingWAMRModule();

    if (payloadLen < 0 || errorMsgLen < 0) {
        SPDLOG_WARN("RPC send_response: negative length");
        return static_cast<int32_t>(Rpc_StatusCode::INVALID_ARGUMENT);
    }

    std::string replyHost = getStringFromWasm(module, replyHostPtr);
    if (replyHost.empty() || replyPort <= 0) {
        SPDLOG_WARN("RPC send_response: missing reply destination");
        return static_cast<int32_t>(Rpc_StatusCode::INVALID_ARGUMENT);
    }

    uint8_t* payload = getBufferFromWasm(module, payloadPtr, payloadLen);

    std::string errorMessage;
    if (errorMsgLen > 0) {
        errorMessage = getStringFromWasm(module, errorMsgPtr);
    }

    try {
        faabric::RpcResponse resp;
        resp.set_requestid(requestId);
        resp.set_statuscode(statusCode);

        if (payloadLen > 0) {
            resp.set_payload(std::string(
                reinterpret_cast<const char*>(payload), payloadLen));
        }

        if (!errorMessage.empty()) {
            resp.set_errormessage(errorMessage);
        }

        // Local fast path: caller is on this host, skip the network and
        // deliver straight to the registry.
        if (replyHost == faabric::util::getSystemConfig().endpointHost) {
            SPDLOG_DEBUG("RPC send_response {} - local fast path", requestId);
            faabric::rpc::getRpcServer().deliverResponse(resp);
        } else {
            SPDLOG_DEBUG("RPC send_response {} - sending to {}:{}",
                         requestId, replyHost, replyPort);
            faabric::rpc::RpcTransportClient client(
                replyHost, replyPort, RPC_SYNC_PORT, kRpcTimeoutMs);
            client.asyncSendResponse(resp);
        }

        return static_cast<int32_t>(Rpc_StatusCode::OK);
    } catch (const std::exception& e) {
        return handleException(module, e, "send response");
    }
}

static int32_t __faasm_rpc_get_request_wrapper(wasm_exec_env_t,
                                               int32_t wasmResumeTarget,
                                               int32_t frameOffset,
                                               uint32_t* outRequestIdPtr,
                                               int32_t* outMethodOffsetPtr,
                                               int32_t* outMethodLenPtr,
                                               int32_t* outPayloadOffsetPtr,
                                               int32_t* outPayloadLenPtr,
                                               int32_t* outReplyHostOffsetPtr,
                                               int32_t* outReplyHostLenPtr,
                                               int32_t* outReplyPortPtr)
{
    auto* module = getExecutingWAMRModule();

    module->validateNativePointer(outRequestIdPtr, sizeof(uint32_t));
    module->validateNativePointer(outMethodOffsetPtr, sizeof(int32_t));
    module->validateNativePointer(outMethodLenPtr, sizeof(int32_t));
    module->validateNativePointer(outPayloadOffsetPtr, sizeof(int32_t));
    module->validateNativePointer(outPayloadLenPtr, sizeof(int32_t));
    module->validateNativePointer(outReplyHostOffsetPtr, sizeof(int32_t));
    module->validateNativePointer(outReplyHostLenPtr, sizeof(int32_t));
    module->validateNativePointer(outReplyPortPtr, sizeof(int32_t));

    try {
        const auto& msg = faabric::executor::ExecutorContext::get()->getMsg();
        const int32_t appId = msg.appid();
        const int32_t messageId = msg.id();

        auto& server = faabric::rpc::getRpcServer();

        constexpr auto migrationCheckInterval =
            std::chrono::milliseconds(100);
        auto nextMigrationCheck =
            std::chrono::steady_clock::now() + migrationCheckInterval;

        std::optional<faabric::rpc::PendingInvocation> inv;
        while (true) {
            auto now = std::chrono::steady_clock::now();
            if (now >= nextMigrationCheck) {
                wasm::doMigrationPoint(
                  wasmResumeTarget, std::to_string(frameOffset));
                nextMigrationCheck = now + migrationCheckInterval;
            }

            inv = server.tryDequeueInvocation(appId, messageId);
            if (inv.has_value()) {
                break;
            }

            if (server.isShutdownRequested(appId, messageId)) {
                SPDLOG_INFO("RPC - Service app={} msg={} drained, "
                            "returning CANCELLED",
                            appId, messageId);
                *outRequestIdPtr = 0;
                *outMethodLenPtr = 0;
                *outPayloadLenPtr = 0;
                *outReplyHostLenPtr = 0;
                *outReplyPortPtr = 0;
                return static_cast<int32_t>(Rpc_StatusCode::CANCELLED);
            }

            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }

        *outRequestIdPtr = inv->requestId;
        writeStringToWasm(module, inv->method,
                          outMethodOffsetPtr, outMethodLenPtr);
        writeStringToWasm(module, inv->payload,
                          outPayloadOffsetPtr, outPayloadLenPtr);
        writeStringToWasm(module, inv->replyHost,
                          outReplyHostOffsetPtr, outReplyHostLenPtr);
        *outReplyPortPtr = inv->replyPort;

        return static_cast<int32_t>(Rpc_StatusCode::OK);
    } catch (const std::exception& e) {
        return handleException(module, e, "get request");
    }
}

static NativeSymbol ns[] = {
    REG_NATIVE_FUNC(Rpc_ChannelCreate, "(**)i"),
    REG_NATIVE_FUNC(Rpc_ChannelClose, "(i)i"),
    REG_NATIVE_FUNC(__faasm_rpc_unary_start, "(i**i*i)i"),
    REG_NATIVE_FUNC(__faasm_rpc_test_response, "(i)i"),
    REG_NATIVE_FUNC(__faasm_rpc_wait_migratable, "(iii)"),
    REG_NATIVE_FUNC(__faasm_rpc_get_response, "(i**)i"),
    REG_NATIVE_FUNC(__faasm_rpc_send_response, "(i*ii*i*i)i"),
    REG_NATIVE_FUNC(__faasm_rpc_get_request, "(ii********)i"),
};

uint32_t getFaasmRpcApi(NativeSymbol** nativeSymbols)
{
    *nativeSymbols = ns;
    return sizeof(ns) / sizeof(NativeSymbol);
}

} // namespace wasm