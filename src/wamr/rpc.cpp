#include <faabric/executor/ExecutorContext.h>
#include <faabric/rpc/RpcContext.h>
#include <faabric/rpc/RpcContextRegistry.h>
#include <faabric/rpc/RpcServer.h>
#include <faabric/rpc/RpcTransportClient.h>
#include <faabric/rpc/rpc.h>
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

#define RPC_CATCH_RETURN(op)                                                   \
    catch (const std::bad_alloc& e)                                            \
    {                                                                          \
        return logAndReturn(e, op, Rpc_StatusCode::RESOURCE_EXHAUSTED);        \
    }                                                                          \
    catch (const std::exception& e)                                            \
    {                                                                          \
        return logAndReturn(e, op);                                            \
    }

namespace wasm {

namespace {

// ------
// Status / error helpers
// ------

static int32_t logAndReturn(const std::exception& e,
                            const char* op,
                            Rpc_StatusCode code = Rpc_StatusCode::INTERNAL)
{
    SPDLOG_ERROR("RPC {} failed: {}", op, e.what());
    return static_cast<int32_t>(code);
}

// Throws a Wasm-level exception for ABI violations (bad pointers, bad lens).
// Always returns INTERNAL so callers can `return throwAbi(...)`.
static int32_t throwAbi(WAMRWasmModule* module, const std::string& msg)
{
    SPDLOG_ERROR("RPC Wasm ABI violation: {}", msg);
    std::runtime_error ex(msg);
    module->doThrowException(ex);
    return static_cast<int32_t>(Rpc_StatusCode::INTERNAL);
}

// ------
// Wasm reader: validates pointers and extracts inputs.
// Latches the first ABI error; callers check ok() once.
// ------

class WasmAbi
{
  public:
    explicit WasmAbi(WAMRWasmModule* module)
      : module(module)
    {}

    bool ok() const { return !failed; }

    // Validate an output pointer. Returns false (and latches) on failure.
    bool validate(void* ptr, size_t len, const char* what)
    {
        if (failed) {
            return false;
        }
        try {
            module->validateNativePointer(ptr, len);
            return true;
        } catch (const std::exception& e) {
            throwAbi(module,
                     std::string("Invalid Wasm pointer for ") + what + ": " +
                       e.what());
            failed = true;
            return false;
        }
    }

    // Read a NUL-terminated string. Returns "" if anything fails (check ok()).
    std::string readString(int32_t* wasmPtr, const char* what)
    {
        if (!validate(wasmPtr, 1, what)) {
            return {};
        }
        char* p = reinterpret_cast<char*>(wasmPtr);
        size_t maxLen =
          module->getMemorySizeBytes() -
          (reinterpret_cast<uint8_t*>(wasmPtr) - module->getMemoryBase());
        size_t len = strnlen(p, maxLen);
        if (!validate(wasmPtr, len + 1, what)) {
            return {};
        }
        return std::string(p, len);
    }

    // Read a (ptr, len) byte buffer. Returns nullptr if len < 0 or validation
    // fails. Returns nullptr with !failed when len == 0 (caller must handle).
    uint8_t* readBuffer(int32_t* wasmPtr, int32_t len, const char* what)
    {
        if (failed) {
            return nullptr;
        }
        if (len < 0) {
            throwAbi(module, std::string("Negative buffer length for ") + what);
            failed = true;
            return nullptr;
        }
        if (len == 0) {
            return nullptr;
        }
        if (!validate(wasmPtr, static_cast<size_t>(len), what)) {
            return nullptr;
        }
        return reinterpret_cast<uint8_t*>(wasmPtr);
    }

    // Allocate Wasm memory and copy `src` (optionally NULL-terminated) into it.
    // On success writes the offset/length to the out pointers and returns OK.
    int32_t writeBytes(const uint8_t* src,
                       size_t len,
                       int32_t* outOffset,
                       int32_t* outLen,
                       bool nullTerminate)
    {
        *outOffset = 0;
        *outLen = 0;

        if (len > static_cast<size_t>(std::numeric_limits<int32_t>::max())) {
            return static_cast<int32_t>(Rpc_StatusCode::RESOURCE_EXHAUSTED);
        }

        size_t allocLen = len + (nullTerminate ? 1 : 0);
        if (allocLen == 0) {
            return static_cast<int32_t>(Rpc_StatusCode::OK);
        }

        void* native = nullptr;
        uint32_t offset =
          module->wasmModuleMalloc(static_cast<uint32_t>(allocLen), &native);
        if (native == nullptr) {
            return static_cast<int32_t>(Rpc_StatusCode::RESOURCE_EXHAUSTED);
        }

        if (len > 0) {
            std::memcpy(native, src, len);
        }
        if (nullTerminate) {
            static_cast<char*>(native)[len] = '\0';
        }

        *outOffset = static_cast<int32_t>(offset);
        *outLen = static_cast<int32_t>(len);
        return static_cast<int32_t>(Rpc_StatusCode::OK);
    }

    int32_t writeString(const std::string& s,
                        int32_t* outOffset,
                        int32_t* outLen)
    {
        return writeBytes(reinterpret_cast<const uint8_t*>(s.data()),
                          s.size(),
                          outOffset,
                          outLen,
                          /*nullTerminate=*/true);
    }

  private:
    WAMRWasmModule* module;
    bool failed = false;
};

// ------
// Context
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
// Channel lifecycle
// ------

static int32_t __faasm_rpc_channel_create_wrapper(wasm_exec_env_t,
                                                  int32_t* targetUri,
                                                  int32_t* outChannelId)
{
    WasmAbi abi(getExecutingWAMRModule());

    if (!abi.validate(outChannelId, sizeof(int32_t), "channel id output")) {
        return static_cast<int32_t>(Rpc_StatusCode::INTERNAL);
    }
    *outChannelId = -1;

    std::string targetUriStr = abi.readString(targetUri, "target URI");
    if (!abi.ok()) {
        return static_cast<int32_t>(Rpc_StatusCode::INTERNAL);
    }

    auto ctx = getCurrentContext();
    if (!ctx) {
        SPDLOG_WARN("RPC - channel create: no RpcContext");
        return static_cast<int32_t>(Rpc_StatusCode::INTERNAL);
    }

    try {
        *outChannelId = ctx->createChannel(targetUriStr);
        return static_cast<int32_t>(Rpc_StatusCode::OK);
    }
    RPC_CATCH_RETURN("channel create")
}

static int32_t __faasm_rpc_channel_close_wrapper(wasm_exec_env_t,
                                                 int32_t channelId)
{
    auto ctx = getCurrentContext();
    if (!ctx) {
        SPDLOG_WARN("RPC - channel close: no RpcContext");
        return static_cast<int32_t>(Rpc_StatusCode::INTERNAL);
    }

    try {
        ctx->closeChannel(channelId);
        return static_cast<int32_t>(Rpc_StatusCode::OK);
    }
    RPC_CATCH_RETURN("channel close")
}

// ------
// Unary client call
// ------

static int32_t __faasm_rpc_unary_start_wrapper(wasm_exec_env_t,
                                               int32_t channelId,
                                               int32_t* methodStr,
                                               int32_t* request,
                                               int32_t reqLen,
                                               uint32_t* outRequestId,
                                               int32_t timeoutMs)
{
    WasmAbi abi(getExecutingWAMRModule());

    if (!abi.validate(outRequestId, sizeof(uint32_t), "request id output")) {
        return static_cast<int32_t>(Rpc_StatusCode::INTERNAL);
    }
    *outRequestId = 0;

    std::string method = abi.readString(methodStr, "method");
    uint8_t* reqBuf = abi.readBuffer(request, reqLen, "request payload");
    if (!abi.ok()) {
        return static_cast<int32_t>(Rpc_StatusCode::INTERNAL);
    }

    auto ctx = getCurrentContext();
    if (!ctx) {
        SPDLOG_WARN("RPC - unary start: no RpcContext");
        return static_cast<int32_t>(Rpc_StatusCode::INTERNAL);
    }

    try {
        *outRequestId =
          ctx->startUnary(channelId, method, reqBuf, reqLen, timeoutMs);
        SPDLOG_TRACE("RPC unary_start ch={} method={} req={}",
                     channelId,
                     method,
                     *outRequestId);
        return static_cast<int32_t>(Rpc_StatusCode::OK);
    }
    RPC_CATCH_RETURN("unary start")
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
        SPDLOG_WARN("RPC - wait migratable: no RpcContext");
        return static_cast<int32_t>(Rpc_StatusCode::INTERNAL);
    }

    try {
        const auto deadline = std::chrono::steady_clock::now() +
                      std::chrono::milliseconds(kRpcTimeoutMs * 4);
        while (true) {
            wasm::doMigrationPoint(wasmResumeTarget, std::to_string(frameOffset));
            if (ctx->testResponse(requestId)) {
                return static_cast<int32_t>(Rpc_StatusCode::OK);
            }
            if (std::chrono::steady_clock::now() >= deadline) {
                SPDLOG_WARN("RPC wait_migratable req={} timed out", requestId);
                return static_cast<int32_t>(Rpc_StatusCode::DEADLINE_EXCEEDED);
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }
    RPC_CATCH_RETURN("wait migratable")
}

static int32_t __faasm_rpc_wait_wrapper(wasm_exec_env_t,
                                        uint32_t requestId)
{
    SPDLOG_DEBUG("RPC wait req={}", requestId);

    auto ctx = getCurrentContext();
    if (!ctx) {
        SPDLOG_WARN("RPC - wait migratable: no RpcContext");
        return static_cast<int32_t>(Rpc_StatusCode::INTERNAL);
    }

    try {
        const auto deadline = std::chrono::steady_clock::now() +
                      std::chrono::milliseconds(kRpcTimeoutMs * 4);
        while (true) {
            if (ctx->testResponse(requestId)) {
                return static_cast<int32_t>(Rpc_StatusCode::OK);
            }
            if (std::chrono::steady_clock::now() >= deadline) {
                SPDLOG_WARN("RPC wait_migratable req={} timed out", requestId);
                return static_cast<int32_t>(Rpc_StatusCode::DEADLINE_EXCEEDED);
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }
    RPC_CATCH_RETURN("wait migratable")
}

static int32_t __faasm_rpc_get_response_wrapper(wasm_exec_env_t,
                                                uint32_t requestId,
                                                int32_t* outRespOffset,
                                                int32_t* outRespLen,
                                                int32_t* outErrOffset,
                                                int32_t* outErrLen)
{
    WasmAbi abi(getExecutingWAMRModule());

    abi.validate(outRespOffset, sizeof(int32_t), "response offset");
    abi.validate(outRespLen, sizeof(int32_t), "response length");
    abi.validate(outErrOffset, sizeof(int32_t), "error offset");
    abi.validate(outErrLen, sizeof(int32_t), "error length");
    if (!abi.ok()) {
        return static_cast<int32_t>(Rpc_StatusCode::INTERNAL);
    }

    *outRespOffset = 0;
    *outRespLen = 0;
    *outErrOffset = 0;
    *outErrLen = 0;

    auto ctx = getCurrentContext();
    if (!ctx) {
        SPDLOG_WARN("RPC - get response: no RpcContext");
        return static_cast<int32_t>(Rpc_StatusCode::INTERNAL);
    }

    try {
        faabric::RpcResponse resp;
        if (!ctx->getResponse(requestId, resp)) {
            SPDLOG_WARN("RPC get_response called before ready (req={})",
                        requestId);
            return static_cast<int32_t>(Rpc_StatusCode::UNAVAILABLE);
        }
        getRpcContextRegistry().clearRequest(requestId);

        if (resp.statuscode() != Rpc_StatusCode::OK) {
            if (!resp.errormessage().empty()) {
                int32_t w =
                  abi.writeString(resp.errormessage(), outErrOffset, outErrLen);
                if (w != static_cast<int32_t>(Rpc_StatusCode::OK)) {
                    return w;
                }
            }
            return resp.statuscode();
        }

        const std::string& payload = resp.payload();
        return abi.writeBytes(reinterpret_cast<const uint8_t*>(payload.data()),
                              payload.size(),
                              outRespOffset,
                              outRespLen,
                              /*nullTerminate=*/false);
    }
    RPC_CATCH_RETURN("get response")
}

static int32_t __faasm_rpc_send_response_wrapper(wasm_exec_env_t,
                                                 uint32_t requestId,
                                                 int32_t* replyHost,
                                                 int32_t replyPort,
                                                 int32_t statusCodeIn,
                                                 int32_t* payload,
                                                 int32_t payloadLen,
                                                 int32_t* errorMsg,
                                                 int32_t errorMsgLen)
{
    auto* module = getExecutingWAMRModule();
    WasmAbi abi(module);

    if (replyPort <= 0) {
        return throwAbi(module, "send_response: invalid reply port "
                            + std::to_string(replyPort));
    }

    std::string replyHostStr = abi.readString(replyHost, "reply host");
    uint8_t* payloadBuf = abi.readBuffer(payload, payloadLen, "payload");
    if (errorMsgLen < 0) {
        return throwAbi(module, "send_response: negative error length");
    }

    uint8_t* errorBuf = abi.readBuffer(errorMsg, errorMsgLen, "error msg");
    if (!abi.ok()) {
        return static_cast<int32_t>(Rpc_StatusCode::INTERNAL);
    }
    if (replyHostStr.empty()) {
        return throwAbi(module, "send_response: empty reply host");
    }

    try {
        faabric::RpcResponse resp;
        resp.set_requestid(requestId);
        resp.set_statuscode(statusCodeIn);
        if (payloadLen > 0) {
            resp.set_payload(
              std::string(reinterpret_cast<const char*>(payloadBuf),
                          static_cast<size_t>(payloadLen)));
        }
        if (errorMsgLen > 0) {
            resp.set_errormessage(
              std::string(reinterpret_cast<const char*>(errorBuf),
                          static_cast<size_t>(errorMsgLen)));
        }

        if (replyHostStr == faabric::util::getSystemConfig().endpointHost) {
            faabric::rpc::getRpcServer().deliverResponse(resp);
        } else {
            faabric::rpc::getRpcServer().sendResponseToHost(
            resp, replyHostStr, replyPort);
        }
        return static_cast<int32_t>(Rpc_StatusCode::OK);
    }
    RPC_CATCH_RETURN("send response")
}

static int32_t __faasm_rpc_get_request_wrapper(wasm_exec_env_t,
                                               int32_t wasmResumeTarget,
                                               int32_t frameOffset,
                                               uint32_t* outRequestId,
                                               int32_t* outMethodOffset,
                                               int32_t* outMethodLen,
                                               int32_t* outPayloadOffset,
                                               int32_t* outPayloadLen,
                                               int32_t* outReplyHostOffset,
                                               int32_t* outReplyHostLen,
                                               int32_t* outReplyPort)
{
    WasmAbi abi(getExecutingWAMRModule());

    abi.validate(outRequestId, sizeof(uint32_t), "request id");
    abi.validate(outMethodOffset, sizeof(int32_t), "method offset");
    abi.validate(outMethodLen, sizeof(int32_t), "method length");
    abi.validate(outPayloadOffset, sizeof(int32_t), "payload offset");
    abi.validate(outPayloadLen, sizeof(int32_t), "payload length");
    abi.validate(outReplyHostOffset, sizeof(int32_t), "reply host offset");
    abi.validate(outReplyHostLen, sizeof(int32_t), "reply host length");
    abi.validate(outReplyPort, sizeof(int32_t), "reply port");
    if (!abi.ok()) {
        return static_cast<int32_t>(Rpc_StatusCode::INTERNAL);
    }

    *outRequestId = 0;
    *outMethodOffset = 0;
    *outMethodLen = 0;
    *outPayloadOffset = 0;
    *outPayloadLen = 0;
    *outReplyHostOffset = 0;
    *outReplyHostLen = 0;
    *outReplyPort = 0;

    try {
        const auto& msg = faabric::executor::ExecutorContext::get()->getMsg();
        const int32_t appId = msg.appid();
        const int32_t messageId = msg.id();
        auto& server = faabric::rpc::getRpcServer();

        constexpr auto kMigrationCheck = std::chrono::milliseconds(100);
        auto nextCheck = std::chrono::steady_clock::now() + kMigrationCheck;

        std::optional<faabric::rpc::PendingInvocation> inv;
        while (true) {
            auto now = std::chrono::steady_clock::now();
            if (now >= nextCheck) {
                wasm::doMigrationPoint(wasmResumeTarget,
                                       std::to_string(frameOffset));
                nextCheck = now + kMigrationCheck;
            }

            inv = server.tryDequeueInvocation(appId, messageId);
            if (inv.has_value()) {
                break;
            }
            if (server.isShutdownRequested(appId, messageId)) {
                SPDLOG_INFO(
                  "RPC get_request app={} msg={} drained", appId, messageId);
                return static_cast<int32_t>(Rpc_StatusCode::CANCELLED);
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }

        *outRequestId = inv->requestId;
        *outReplyPort = inv->replyPort;

        if (int32_t s =
              abi.writeString(inv->method, outMethodOffset, outMethodLen);
            s != static_cast<int32_t>(Rpc_StatusCode::OK)) {
            return s;
        }
        if (int32_t s = abi.writeBytes(
              reinterpret_cast<const uint8_t*>(inv->payload.data()),
              inv->payload.size(),
              outPayloadOffset,
              outPayloadLen,
              /*nullTerminate=*/false);
            s != static_cast<int32_t>(Rpc_StatusCode::OK)) {
            return s;
        }
        return abi.writeString(
          inv->replyHost, outReplyHostOffset, outReplyHostLen);
    }
    RPC_CATCH_RETURN("get request")
}

static int32_t __faasm_rpc_get_request_nomig_wrapper(wasm_exec_env_t,
                                                     uint32_t* outRequestId,
                                                     int32_t* outMethodOffset,
                                                     int32_t* outMethodLen,
                                                     int32_t* outPayloadOffset,
                                                     int32_t* outPayloadLen,
                                                     int32_t* outReplyHostOffset,
                                                     int32_t* outReplyHostLen,
                                                     int32_t* outReplyPort)
{
    WasmAbi abi(getExecutingWAMRModule());

    abi.validate(outRequestId, sizeof(uint32_t), "request id");
    abi.validate(outMethodOffset, sizeof(int32_t), "method offset");
    abi.validate(outMethodLen, sizeof(int32_t), "method length");
    abi.validate(outPayloadOffset, sizeof(int32_t), "payload offset");
    abi.validate(outPayloadLen, sizeof(int32_t), "payload length");
    abi.validate(outReplyHostOffset, sizeof(int32_t), "reply host offset");
    abi.validate(outReplyHostLen, sizeof(int32_t), "reply host length");
    abi.validate(outReplyPort, sizeof(int32_t), "reply port");
    if (!abi.ok()) {
        return static_cast<int32_t>(Rpc_StatusCode::INTERNAL);
    }

    *outRequestId = 0;
    *outMethodOffset = 0;
    *outMethodLen = 0;
    *outPayloadOffset = 0;
    *outPayloadLen = 0;
    *outReplyHostOffset = 0;
    *outReplyHostLen = 0;
    *outReplyPort = 0;

    try {
        const auto& msg = faabric::executor::ExecutorContext::get()->getMsg();
        const int32_t appId = msg.appid();
        const int32_t messageId = msg.id();
        auto& server = faabric::rpc::getRpcServer();

        constexpr auto kMigrationCheck = std::chrono::milliseconds(100);
        auto nextCheck = std::chrono::steady_clock::now() + kMigrationCheck;

        std::optional<faabric::rpc::PendingInvocation> inv;
        while (true) {
            auto now = std::chrono::steady_clock::now();
            inv = server.tryDequeueInvocation(appId, messageId);
            if (inv.has_value()) {
                break;
            }
            if (server.isShutdownRequested(appId, messageId)) {
                SPDLOG_INFO(
                  "RPC get_request app={} msg={} drained", appId, messageId);
                return static_cast<int32_t>(Rpc_StatusCode::CANCELLED);
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }

        *outRequestId = inv->requestId;
        *outReplyPort = inv->replyPort;

        if (int32_t s =
              abi.writeString(inv->method, outMethodOffset, outMethodLen);
            s != static_cast<int32_t>(Rpc_StatusCode::OK)) {
            return s;
        }
        if (int32_t s = abi.writeBytes(
              reinterpret_cast<const uint8_t*>(inv->payload.data()),
              inv->payload.size(),
              outPayloadOffset,
              outPayloadLen,
              /*nullTerminate=*/false);
            s != static_cast<int32_t>(Rpc_StatusCode::OK)) {
            return s;
        }
        return abi.writeString(
          inv->replyHost, outReplyHostOffset, outReplyHostLen);
    }
    RPC_CATCH_RETURN("get request")
}

static NativeSymbol ns[] = {
    REG_NATIVE_FUNC(__faasm_rpc_channel_create, "(**)i"),
    REG_NATIVE_FUNC(__faasm_rpc_channel_close, "(i)i"),
    REG_NATIVE_FUNC(__faasm_rpc_unary_start, "(i**i*i)i"),
    REG_NATIVE_FUNC(__faasm_rpc_test_response, "(i)i"),
    REG_NATIVE_FUNC(__faasm_rpc_wait_migratable, "(iii)i"),
    REG_NATIVE_FUNC(__faasm_rpc_wait, "(i)i"),
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

#undef RPC_CATCH_RETURN
