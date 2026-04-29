#include <faabric/executor/ExecutorContext.h>
#include <faabric/rpc/rpc.h>
#include <faabric/rpc/RpcContext.h>
#include <faabric/rpc/RpcContextRegistry.h>
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

// Holds the context by shared_ptr so the C++ object cannot be destroyed
// underneath an in-flight host call, even if the registry evicts it
// (e.g. on migration). Exits the call counter on destruction.
class RpcCallScopeGuard
{
  public:
    explicit RpcCallScopeGuard(std::shared_ptr<RpcContext> ctxIn)
      : ctx(std::move(ctxIn))
    {
        if (ctx) {
            active = ctx->tryEnterCall();
        }
    }

    ~RpcCallScopeGuard()
    {
        if (active && ctx) {
            ctx->exitCall();
        }
    }

    // `if (!guard)` syntax
    explicit operator bool() const { return active; }

    RpcCallScopeGuard(const RpcCallScopeGuard&) = delete;
    RpcCallScopeGuard& operator=(const RpcCallScopeGuard&) = delete;

    RpcCallScopeGuard(RpcCallScopeGuard&& other) noexcept
      : ctx(std::move(other.ctx)), active(other.active)
    {
        other.active = false;
    }
    RpcCallScopeGuard& operator=(RpcCallScopeGuard&&) = delete;

  private:
    std::shared_ptr<RpcContext> ctx;
    bool active = false;
};

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

void setResumeStep(WAMRWasmModule* module,
                   int32_t* statePtr,
                   int32_t resumeStep)
{
    if (statePtr == nullptr) {
        std::runtime_error ex("Null migration state pointer");
        module->doThrowException(ex);
        return;
    }
    module->validateNativePointer(statePtr, sizeof(int32_t));
    statePtr[0] = resumeStep;
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

    RpcCallScopeGuard guard(ctx);
    if (!guard) {
        SPDLOG_WARN("RPC channel create rejected (quiescing)");
        return static_cast<int32_t>(Rpc_StatusCode::UNAVAILABLE);
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

    RpcCallScopeGuard guard(ctx);
    if (!guard) {
        SPDLOG_WARN("RPC channel close rejected (quiescing)");
        return static_cast<int32_t>(Rpc_StatusCode::UNAVAILABLE);
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
                                               uint32_t* outRequestIdPtr)
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

    RpcCallScopeGuard guard(ctx);
    if (!guard) {
        return static_cast<int32_t>(Rpc_StatusCode::UNAVAILABLE);
    }

    try {
        uint32_t requestId =
          ctx->startUnary(channelId, methodName, reqBuf, reqLen);
        *outRequestIdPtr = requestId;

        // Register the request → msgId mapping so the server thread can
        // route the response back to the right context.
        int32_t msgId =
          faabric::executor::ExecutorContext::get()->getMsg().id();
        getRpcContextRegistry().registerInFlightRequest(requestId, msgId);

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
                                                int32_t wasmFuncPtr,
                                                int32_t* statePtr,
                                                int32_t resumeStep)
{
    auto* module = getExecutingWAMRModule();

    setResumeStep(module, statePtr, resumeStep);

    auto ctx = getCurrentContext();
    if (!ctx) {
        std::runtime_error ex("RPC wait: no context for executing message");
        module->doThrowException(ex);
        return;
    }

    while (true) {
        try {
            if (ctx->testResponse(requestId)) {
                return;
            }
        } catch (const std::exception& e) {
            handleException(module, e, "wait");
            return;
        }

        wasm::doMigrationPoint(wasmFuncPtr, "");
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

    RpcCallScopeGuard guard(ctx);
    if (!guard) {
        return static_cast<int32_t>(Rpc_StatusCode::UNAVAILABLE);
    }

    try {
        faabric::RpcResponse resp;
        if (!ctx->getResponse(requestId, resp)) {
            SPDLOG_WARN("RPC get_response called before ready (req={})",
                        requestId);
            return static_cast<int32_t>(Rpc_StatusCode::UNAVAILABLE);
        }

        // Response consumed — drop the [request, msgId] mapping.
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

static NativeSymbol ns[] = {
    REG_NATIVE_FUNC(Rpc_ChannelCreate, "(**)i"),
    REG_NATIVE_FUNC(Rpc_ChannelClose, "(i)i"),
    REG_NATIVE_FUNC(__faasm_rpc_unary_start, "(i**i*)i"),
    REG_NATIVE_FUNC(__faasm_rpc_test_response, "(i)i"),
    REG_NATIVE_FUNC(__faasm_rpc_wait_migratable, "(ii*i)"),
    REG_NATIVE_FUNC(__faasm_rpc_get_response, "(i**)i"),
};

uint32_t getFaasmRpcApi(NativeSymbol** nativeSymbols)
{
    *nativeSymbols = ns;
    return sizeof(ns) / sizeof(NativeSymbol);
}

} // namespace wasm