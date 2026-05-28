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
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <wasm_export.h>

using namespace faabric::rpc;

#define RPC_CATCH_RETURN(op)                                                  \
    catch (const std::bad_alloc& e) {                                         \
        return handleRpcException(e, op, Rpc_StatusCode::RESOURCE_EXHAUSTED); \
    } catch (const std::exception& e) {                                       \
        return handleRpcException(e, op);                                     \
    }

#define RPC_STATUS_CODE(code) \
    static_cast<int32_t>(code)

namespace wasm {

namespace {

// ------
// status helpers
// ------

static int32_t handleRpcException(
  const std::exception& e,
  const char* op,
  Rpc_StatusCode code = Rpc_StatusCode::INTERNAL)
{
    SPDLOG_ERROR("RPC {} failed: {}", op, e.what());
    return RPC_STATUS_CODE(code);
}

// ------
// Wasm exception helpers
// ------

static void throwWasmException(WAMRWasmModule* module,
                                  const std::string& message)
{
    SPDLOG_ERROR("RPC Wasm ABI violation: {}", message);

    std::runtime_error ex(message);
    module->doThrowException(ex);
}

static bool validateWasmPointerOrThrow(WAMRWasmModule* module,
                                       void* ptr,
                                       size_t len,
                                       const char* what)
{
    try {
        module->validateNativePointer(ptr, len);
        return true;
    } catch (const std::exception& e) {
        throwWasmException(
          module,
          std::string("Invalid Wasm pointer for ") + what + ": " + e.what());
        return false;
    }
}

static const char* getStringFromWasmOrThrow(WAMRWasmModule* module,
                                            int32_t* wasmPtr,
                                            size_t& outLen,
                                            const char* what)
{
    outLen = 0;

    if (!validateWasmPointerOrThrow(module, wasmPtr, 1, what)) {
        return nullptr;
    }

    char* strPtr = reinterpret_cast<char*>(wasmPtr);

    size_t maxLen =
      module->getMemorySizeBytes() -
      (reinterpret_cast<uint8_t*>(wasmPtr) - module->getMemoryBase());

    size_t strLen = strnlen(strPtr, maxLen);

    if (!validateWasmPointerOrThrow(module, wasmPtr, strLen + 1, what)) {
        return nullptr;
    }

    outLen = strLen;
    return strPtr;
}

static uint8_t* getBufferFromWasmOrThrow(WAMRWasmModule* module,
                                         int32_t* wasmPtr,
                                         int32_t len,
                                         const char* what)
{
    if (len < 0) {
        throwWasmException(module,
                           std::string("Negative Wasm buffer length for ") +
                             what);
        return nullptr;
    }

    if (len > 0 &&
        !validateWasmPointerOrThrow(
          module, wasmPtr, static_cast<size_t>(len), what)) {
        return nullptr;
    }

    return reinterpret_cast<uint8_t*>(wasmPtr);
}

// ------
// Wasm allocation helpers
// ------

static int32_t writeBytesToWasm(WAMRWasmModule* module,
                                const uint8_t* src,
                                size_t len,
                                int32_t* outOffsetPtr,
                                int32_t* outLenPtr,
                                bool nullTerminate)
{
    *outOffsetPtr = 0;
    *outLenPtr = 0;

    if (len > static_cast<size_t>(std::numeric_limits<int32_t>::max())) {
        return RPC_STATUS_CODE(Rpc_StatusCode::RESOURCE_EXHAUSTED);
    }

    size_t allocLen = len + (nullTerminate ? 1 : 0);

    if (allocLen == 0) {
        return RPC_STATUS_CODE(Rpc_StatusCode::OK);
    }

    if (allocLen > static_cast<size_t>(std::numeric_limits<uint32_t>::max())) {
        return RPC_STATUS_CODE(Rpc_StatusCode::RESOURCE_EXHAUSTED);
    }

    void* nativePtr = nullptr;
    uint32_t wasmOffset =
      module->wasmModuleMalloc(static_cast<uint32_t>(allocLen), &nativePtr);

    if (nativePtr == nullptr) {
        return RPC_STATUS_CODE(Rpc_StatusCode::RESOURCE_EXHAUSTED);
    }

    if (len > 0) {
        std::memcpy(nativePtr, src, len);
    }

    if (nullTerminate) {
        static_cast<char*>(nativePtr)[len] = '\0';
    }

    *outOffsetPtr = static_cast<int32_t>(wasmOffset);
    *outLenPtr = static_cast<int32_t>(len);

    return RPC_STATUS_CODE(Rpc_StatusCode::OK);
}

static int32_t writeStringToWasm(WAMRWasmModule* module,
                                 const std::string& src,
                                 int32_t* outOffsetPtr,
                                 int32_t* outLenPtr)
{
    return writeBytesToWasm(module,
                            reinterpret_cast<const uint8_t*>(src.data()),
                            src.size(),
                            outOffsetPtr,
                            outLenPtr,
                            true);
}

// ------
// per-call setup
// ------

static std::shared_ptr<RpcContext> getCurrentContext()
{
    try {
        auto msg = faabric::executor::ExecutorContext::get()->getMsg();
        return getRpcContextRegistry().getContext(msg.appid(), msg.id());
    } catch (const std::exception& e) {
        SPDLOG_ERROR("RPC failed to get current RpcContext: {}", e.what());
        return nullptr;
    }
}

} // namespace

// ------
// channel lifecycle
// ------

static int32_t __faasm_rpc_channel_create_wrapper(
  wasm_exec_env_t,
  int32_t* targetUriPtr,
  int32_t* outChannelIdPtr)
{
    auto* module = getExecutingWAMRModule();

    if (!validateWasmPointerOrThrow(
          module, outChannelIdPtr, sizeof(int32_t), "channel id output")) {
        return RPC_STATUS_CODE(Rpc_StatusCode::INTERNAL);
    }

    *outChannelIdPtr = -1;

    size_t targetUriLen = 0;
    const char* targetUriData =
      getStringFromWasmOrThrow(module, targetUriPtr, targetUriLen, "target URI");

    if (targetUriData == nullptr) {
        return RPC_STATUS_CODE(Rpc_StatusCode::INTERNAL);
    }

    auto ctx = getCurrentContext();
    if (!ctx) {
        SPDLOG_WARN("RPC channel create rejected: no RpcContext");
        return RPC_STATUS_CODE(Rpc_StatusCode::INTERNAL);
    }

    try {
        std::string targetUri(targetUriData, targetUriLen);
        *outChannelIdPtr = ctx->createChannel(targetUri);
        return RPC_STATUS_CODE(Rpc_StatusCode::OK);
    }
    RPC_CATCH_RETURN("channel create")
}

static int32_t __faasm_rpc_channel_close_wrapper(wasm_exec_env_t,
                                                 int32_t channelId)
{
    auto ctx = getCurrentContext();
    if (!ctx) {
        SPDLOG_WARN("RPC channel close rejected: no RpcContext");
        return RPC_STATUS_CODE(Rpc_StatusCode::INTERNAL);
    }

    try {
        ctx->closeChannel(channelId);
        return RPC_STATUS_CODE(Rpc_StatusCode::OK);
    } catch (const std::exception& e) {
        return handleRpcException(e, "channel close");
    }
}

// ------
// unary client call
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

    if (!validateWasmPointerOrThrow(
          module, outRequestIdPtr, sizeof(uint32_t), "request id output")) {
        return RPC_STATUS_CODE(Rpc_StatusCode::INTERNAL);
    }

    *outRequestIdPtr = 0;

    size_t methodNameLen = 0;
    const char* methodNameData =
      getStringFromWasmOrThrow(module, methodStrPtr, methodNameLen, "method");

    if (methodNameData == nullptr) {
        return RPC_STATUS_CODE(Rpc_StatusCode::INTERNAL);
    }

    uint8_t* reqBuf =
      getBufferFromWasmOrThrow(module, reqBufPtr, reqLen, "request payload");

    if (reqBuf == nullptr && reqLen != 0) {
        return RPC_STATUS_CODE(Rpc_StatusCode::INTERNAL);
    }

    auto ctx = getCurrentContext();
    if (!ctx) {
        SPDLOG_WARN("RPC unary_start rejected: no RpcContext");
        return RPC_STATUS_CODE(Rpc_StatusCode::INTERNAL);
    }

    try {
        std::string methodName(methodNameData, methodNameLen);
        uint32_t requestId =
          ctx->startUnary(channelId, methodName, reqBuf, reqLen, timeoutMs);
        *outRequestIdPtr = requestId;

        SPDLOG_TRACE("RPC unary_start ch={} method={} req={}",
                     channelId,
                     methodName,
                     requestId);

        return RPC_STATUS_CODE(Rpc_StatusCode::OK);
    }
    RPC_CATCH_RETURN("unary start");
}

static int32_t __faasm_rpc_test_response_wrapper(wasm_exec_env_t,
                                                 uint32_t requestId)
{
    auto ctx = getCurrentContext();
    if (!ctx) {
        SPDLOG_WARN("RPC test_response: no RpcContext");
        return 0;
    }

    try {
        return ctx->testResponse(requestId) ? 1 : 0;
    } catch (const std::exception& e) {
        SPDLOG_ERROR("RPC test_response failed: {}", e.what());
        return 0;
    }
}

static int32_t __faasm_rpc_wait_migratable_wrapper(wasm_exec_env_t,
                                                   uint32_t requestId,
                                                   int32_t wasmResumeTarget,
                                                   int32_t frameOffset)
{
    SPDLOG_DEBUG("RPC wait_migratable req={} target={} frame={}",
                 requestId,
                 wasmResumeTarget,
                 frameOffset);

    auto ctx = getCurrentContext();
    if (!ctx) {
        SPDLOG_ERROR("RPC wait_migratable rejected: no RpcContext");
        return RPC_STATUS_CODE(Rpc_StatusCode::INTERNAL);
    }

    try {
        while (true) {
            wasm::doMigrationPoint(wasmResumeTarget,
                                   std::to_string(frameOffset));

            if (ctx->testResponse(requestId)) {
                SPDLOG_INFO("RPC wait_migratable req={} ready", requestId);
                return RPC_STATUS_CODE(Rpc_StatusCode::OK);
            }

            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    } catch (const std::exception& e) {
        return handleRpcException(e, "wait migratable");
    }
}

static int32_t __faasm_rpc_get_response_wrapper(
  wasm_exec_env_t,
  uint32_t requestId,
  int32_t* outRespBufOffsetPtr,
  int32_t* outRespLenPtr,
  int32_t* outErrorMsgOffsetPtr,
  int32_t* outErrorMsgLenPtr)
{
    auto* module = getExecutingWAMRModule();

    if (!validateWasmPointerOrThrow(
          module, outRespBufOffsetPtr, sizeof(int32_t), "response offset output") ||
        !validateWasmPointerOrThrow(
          module, outRespLenPtr, sizeof(int32_t), "response length output") ||
        !validateWasmPointerOrThrow(
          module, outErrorMsgOffsetPtr, sizeof(int32_t), "error offset output") ||
        !validateWasmPointerOrThrow(
          module, outErrorMsgLenPtr, sizeof(int32_t), "error length output")) {
        return RPC_STATUS_CODE(Rpc_StatusCode::INTERNAL);
    }

    *outRespBufOffsetPtr = 0;
    *outRespLenPtr = 0;
    *outErrorMsgOffsetPtr = 0;
    *outErrorMsgLenPtr = 0;

    auto ctx = getCurrentContext();
    if (!ctx) {
        SPDLOG_WARN("RPC get_response rejected: no RpcContext");
        return RPC_STATUS_CODE(Rpc_StatusCode::INTERNAL);
    }

    try {
        faabric::RpcResponse resp;
        if (!ctx->getResponse(requestId, resp)) {
            SPDLOG_WARN("RPC get_response called before ready (req={})",
                        requestId);
            return RPC_STATUS_CODE(Rpc_StatusCode::UNAVAILABLE);
        }

        // Response consumed... drop the [request, msgId] mapping.
        getRpcContextRegistry().clearRequest(requestId);

        if (resp.statuscode() != Rpc_StatusCode::OK) {
            const std::string& errorMessage = resp.errormessage();

            if (!errorMessage.empty()) {
                int32_t writeStatus =
                  writeStringToWasm(module,
                                    errorMessage,
                                    outErrorMsgOffsetPtr,
                                    outErrorMsgLenPtr);

                if (writeStatus != RPC_STATUS_CODE(Rpc_StatusCode::OK)) {
                    return writeStatus;
                }
            }

            return resp.statuscode();
        }

        const std::string& payload = resp.payload();

        int32_t writeStatus =
          writeBytesToWasm(module,
                           reinterpret_cast<const uint8_t*>(payload.data()),
                           payload.size(),
                           outRespBufOffsetPtr,
                           outRespLenPtr,
                           false);

        if (writeStatus != RPC_STATUS_CODE(Rpc_StatusCode::OK)) {
            return writeStatus;
        }

        return RPC_STATUS_CODE(Rpc_StatusCode::OK);
    }
    RPC_CATCH_RETURN("get response");
}

static int32_t __faasm_rpc_send_response_wrapper(wasm_exec_env_t,
                                                 uint32_t requestId,
                                                 int32_t* replyHostPtr,
                                                 int32_t replyPort,
                                                 int32_t RPC_STATUS_CODEIn,
                                                 int32_t* payloadPtr,
                                                 int32_t payloadLen,
                                                 int32_t* errorMsgPtr,
                                                 int32_t errorMsgLen)
{
    auto* module = getExecutingWAMRModule();

    if (replyPort <= 0) {
        throwWasmException(module,
                           "RPC send_response called with invalid reply port");
        return RPC_STATUS_CODE(Rpc_StatusCode::INTERNAL);
    }

    size_t replyHostLen = 0;
    const char* replyHostData =
      getStringFromWasmOrThrow(module, replyHostPtr, replyHostLen, "reply host");

    if (replyHostData == nullptr) {
        return RPC_STATUS_CODE(Rpc_StatusCode::INTERNAL);
    }

    uint8_t* payloadBuf =
      getBufferFromWasmOrThrow(module, payloadPtr, payloadLen, "response payload");

    if (payloadBuf == nullptr && payloadLen != 0) {
        return RPC_STATUS_CODE(Rpc_StatusCode::INTERNAL);
    }

    uint8_t* errorMsgBuf = nullptr;
    if (errorMsgLen > 0) {
        errorMsgBuf =
          getBufferFromWasmOrThrow(module, errorMsgPtr, errorMsgLen, "error message");

        if (errorMsgBuf == nullptr) {
            return RPC_STATUS_CODE(Rpc_StatusCode::INTERNAL);
        }
    } else if (errorMsgLen < 0) {
        throwWasmException(module,
                           "RPC send_response called with negative error length");
        return RPC_STATUS_CODE(Rpc_StatusCode::INTERNAL);
    }

    try {
        std::string replyHost(replyHostData, replyHostLen);

        if (replyHost.empty()) {
            throwWasmException(module,
                               "RPC send_response called with empty reply host");
            return RPC_STATUS_CODE(Rpc_StatusCode::INTERNAL);
        }

        std::string payload;
        if (payloadLen > 0) {
            payload = std::string(reinterpret_cast<const char*>(payloadBuf),
                                  static_cast<size_t>(payloadLen));
        }

        std::string errorMessage;
        if (errorMsgLen > 0) {
            errorMessage =
              std::string(reinterpret_cast<const char*>(errorMsgBuf),
                          static_cast<size_t>(errorMsgLen));
        }

        faabric::RpcResponse resp;
        resp.set_requestid(requestId);
        resp.set_statuscode(RPC_STATUS_CODEIn);

        if (!payload.empty()) {
            resp.set_payload(std::move(payload));
        }

        if (!errorMessage.empty()) {
            resp.set_errormessage(std::move(errorMessage));
        }

        if (replyHost == faabric::util::getSystemConfig().endpointHost) {
            SPDLOG_DEBUG("RPC send_response req={} local fast path",
                         requestId);

            try {
                faabric::rpc::getRpcServer().deliverResponse(resp);
            } catch (const std::exception& e) {
                return handleRpcException(
                  e, "local response delivery", Rpc_StatusCode::INTERNAL);
            }
        } else {
            SPDLOG_DEBUG("RPC send_response req={} remote {}:{}",
                         requestId,
                         replyHost,
                         replyPort);

            try {
                faabric::rpc::RpcTransportClient client(
                  replyHost, replyPort, RPC_SYNC_PORT, kRpcTimeoutMs);

                client.asyncSendResponse(resp);
            } catch (const std::exception& e) {
                return handleRpcException(
                  e, "remote response delivery", Rpc_StatusCode::UNAVAILABLE);
            }
        }

        return RPC_STATUS_CODE(Rpc_StatusCode::OK);
    }
    RPC_CATCH_RETURN("send response");
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

    if (!validateWasmPointerOrThrow(
          module, outRequestIdPtr, sizeof(uint32_t), "request id output") ||
        !validateWasmPointerOrThrow(
          module, outMethodOffsetPtr, sizeof(int32_t), "method offset output") ||
        !validateWasmPointerOrThrow(
          module, outMethodLenPtr, sizeof(int32_t), "method length output") ||
        !validateWasmPointerOrThrow(
          module, outPayloadOffsetPtr, sizeof(int32_t), "payload offset output") ||
        !validateWasmPointerOrThrow(
          module, outPayloadLenPtr, sizeof(int32_t), "payload length output") ||
        !validateWasmPointerOrThrow(
          module, outReplyHostOffsetPtr, sizeof(int32_t), "reply host offset output") ||
        !validateWasmPointerOrThrow(
          module, outReplyHostLenPtr, sizeof(int32_t), "reply host length output") ||
        !validateWasmPointerOrThrow(
          module, outReplyPortPtr, sizeof(int32_t), "reply port output")) {
        return RPC_STATUS_CODE(Rpc_StatusCode::INTERNAL);
    }

    *outRequestIdPtr = 0;
    *outMethodOffsetPtr = 0;
    *outMethodLenPtr = 0;
    *outPayloadOffsetPtr = 0;
    *outPayloadLenPtr = 0;
    *outReplyHostOffsetPtr = 0;
    *outReplyHostLenPtr = 0;
    *outReplyPortPtr = 0;

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
                wasm::doMigrationPoint(wasmResumeTarget,
                                       std::to_string(frameOffset));
                nextMigrationCheck = now + migrationCheckInterval;
            }

            inv = server.tryDequeueInvocation(appId, messageId);
            if (inv.has_value()) {
                break;
            }

            if (server.isShutdownRequested(appId, messageId)) {
                SPDLOG_INFO("RPC get_request app={} msg={} drained",
                            appId,
                            messageId);

                return RPC_STATUS_CODE(Rpc_StatusCode::CANCELLED);
            }

            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }

        *outRequestIdPtr = inv->requestId;

        int32_t writeStatus =
          writeStringToWasm(module,
                            inv->method,
                            outMethodOffsetPtr,
                            outMethodLenPtr);

        if (writeStatus != RPC_STATUS_CODE(Rpc_StatusCode::OK)) {
            return writeStatus;
        }

        writeStatus =
          writeBytesToWasm(module,
                           reinterpret_cast<const uint8_t*>(
                             inv->payload.data()),
                           inv->payload.size(),
                           outPayloadOffsetPtr,
                           outPayloadLenPtr,
                           false);

        if (writeStatus != RPC_STATUS_CODE(Rpc_StatusCode::OK)) {
            return writeStatus;
        }

        writeStatus =
          writeStringToWasm(module,
                            inv->replyHost,
                            outReplyHostOffsetPtr,
                            outReplyHostLenPtr);

        if (writeStatus != RPC_STATUS_CODE(Rpc_StatusCode::OK)) {
            return writeStatus;
        }

        *outReplyPortPtr = inv->replyPort;

        return RPC_STATUS_CODE(Rpc_StatusCode::OK);
    }
    RPC_CATCH_RETURN("send response");
}

static NativeSymbol ns[] = {
    REG_NATIVE_FUNC(__faasm_rpc_channel_create, "(**)i"),
    REG_NATIVE_FUNC(__faasm_rpc_channel_close, "(i)i"),
    REG_NATIVE_FUNC(__faasm_rpc_unary_start, "(i**i*i)i"),
    REG_NATIVE_FUNC(__faasm_rpc_test_response, "(i)i"),
    REG_NATIVE_FUNC(__faasm_rpc_wait_migratable, "(iii)i"),
    REG_NATIVE_FUNC(__faasm_rpc_get_response, "(i****)i"),
    REG_NATIVE_FUNC(__faasm_rpc_send_response, "(i*ii*i*i)i"),
    REG_NATIVE_FUNC(__faasm_rpc_get_request, "(ii********)i"),
};

uint32_t getFaasmRpcApi(NativeSymbol** nativeSymbols)
{
    *nativeSymbols = ns;
    return sizeof(ns) / sizeof(NativeSymbol);
}

} // namespace wasm