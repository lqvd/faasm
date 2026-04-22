#include <faabric/rpc/rpc.h>
#include <faabric/rpc/RpcContext.h>
#include <faabric/util/bytes.h>
#include <faabric/util/logging.h>
#include <wamr/WAMRModuleMixin.h>
#include <wamr/WAMRWasmModule.h>
#include <wamr/native.h>

#include <cstring>
#include <optional>
#include <wasm_export.h>

using namespace faabric::rpc;

namespace wasm {

namespace {

class RpcCallScopeGuard
{
  public:
    static std::optional<RpcCallScopeGuard> enter(faabric::rpc::RpcContext& ctx)
    {
        if (!ctx.tryEnterCall()) {
            return std::nullopt;
        }
        return RpcCallScopeGuard(ctx);
    }

    ~RpcCallScopeGuard()
    {
        if (active) {
            ctx.exitCall();
        }
    }

    RpcCallScopeGuard(const RpcCallScopeGuard&) = delete;
    RpcCallScopeGuard& operator=(const RpcCallScopeGuard&) = delete;

    RpcCallScopeGuard(RpcCallScopeGuard&& other) noexcept
      : ctx(other.ctx)
      , active(other.active)
    {
        other.active = false;
    }

    RpcCallScopeGuard& operator=(RpcCallScopeGuard&&) = delete;

  private:
    explicit RpcCallScopeGuard(faabric::rpc::RpcContext& ctxIn)
      : ctx(ctxIn)
    {}

    faabric::rpc::RpcContext& ctx;
    bool active = true;
};

static std::string getStringFromWasm(wasm::WAMRWasmModule* module, int32_t* wasmPtr)
{
    module->validateNativePointer(wasmPtr, 1);

    char* strPtr = reinterpret_cast<char*>(wasmPtr);
    size_t maxLen = module->getMemorySizeBytes()
                    - (reinterpret_cast<uint8_t*>(wasmPtr) - module->getMemoryBase());
    size_t strLen = strnlen(strPtr, maxLen);

    module->validateNativePointer(wasmPtr, strLen + 1);
    return std::string(strPtr, strLen);
}

static uint8_t* getBufferFromWasm(wasm::WAMRWasmModule* module, int32_t* wasmPtr, int32_t len)
{
    if (len > 0) {
        module->validateNativePointer(wasmPtr, len);
    }
    return reinterpret_cast<uint8_t*>(wasmPtr);
}

} // namespace

static int32_t RPC_ChannelCreate_wrapper(
  wasm_exec_env_t execEnv,
  int32_t* targetUriPtr,
  int32_t* outChannelIdPtr)
{
    auto* module = wasm::getExecutingWAMRModule();
    auto& rpcContext = faabric::rpc::getExecutingRpcContext();

    auto guard = RpcCallScopeGuard::enter(rpcContext);
    if (!guard) {
        SPDLOG_WARN("RPC channel create rejected because context is quiescing");
        return static_cast<int32_t>(faabric::rpc::StatusCode::UNAVAILABLE);
    }

    module->validateNativePointer(outChannelIdPtr, sizeof(int32_t));
    std::string targetUri = getStringFromWasm(module, targetUriPtr);

    try {
        int32_t newId = rpcContext.createChannel(targetUri);
        *outChannelIdPtr = newId;
        return static_cast<int32_t>(faabric::rpc::StatusCode::OK);
    } catch (std::exception& e) {
        SPDLOG_ERROR("Failed to create RPC channel: {}", e.what());
        module->doThrowException(e);
        return static_cast<int32_t>(faabric::rpc::StatusCode::INTERNAL);
    }
}

static int32_t RPC_ChannelClose_wrapper(
  wasm_exec_env_t execEnv,
  int32_t channelId)
{
    auto& rpcContext = faabric::rpc::getExecutingRpcContext();

    auto guard = RpcCallScopeGuard::enter(rpcContext);
    if (!guard) {
        SPDLOG_WARN("RPC channel close rejected because context is quiescing");
        return static_cast<int32_t>(faabric::rpc::StatusCode::UNAVAILABLE);
    }

    rpcContext.closeChannel(channelId);
    return static_cast<int32_t>(faabric::rpc::StatusCode::OK);
}

static int32_t RPC_UnaryCall_wrapper(
  wasm_exec_env_t execEnv,
  int32_t channelId,
  int32_t* methodStrPtr,
  int32_t* reqBufPtr,
  int32_t reqLen,
  int32_t* outRespBufOffsetPtr,
  int32_t* outRespLenPtr)
{
    auto* module = wasm::getExecutingWAMRModule();
    auto& rpcContext = faabric::rpc::getExecutingRpcContext();

    if (reqLen < 0) {
        SPDLOG_WARN("RPC request length must be non-negative");
        return static_cast<int32_t>(faabric::rpc::StatusCode::INVALID_ARGUMENT);
    }

    auto guard = RpcCallScopeGuard::enter(rpcContext);
    if (!guard) {
        SPDLOG_WARN("RPC call rejected because context is quiescing");
        return static_cast<int32_t>(faabric::rpc::StatusCode::UNAVAILABLE);
    }

    module->validateNativePointer(outRespBufOffsetPtr, sizeof(int32_t));
    module->validateNativePointer(outRespLenPtr, sizeof(int32_t));

    std::string methodName = getStringFromWasm(module, methodStrPtr);
    uint8_t* reqBuf = getBufferFromWasm(module, reqBufPtr, reqLen);

    try {
        auto channel = rpcContext.getChannel(channelId);

        std::vector<uint8_t> hostResponse;
        int status = channel->syncCall(methodName, reqBuf, reqLen, hostResponse);
        if (status != 0) {
            return status;
        }

        void* nativePtr = nullptr;
        uint32_t wasmOffset = module->wasmModuleMalloc(hostResponse.size(), &nativePtr);

        if (nativePtr == nullptr && !hostResponse.empty()) {
            std::runtime_error ex("Wasm heap allocation failed");
            module->doThrowException(ex);
            return static_cast<int32_t>(faabric::rpc::StatusCode::RESOURCE_EXHAUSTED);
        }

        if (!hostResponse.empty()) {
            std::memcpy(nativePtr, hostResponse.data(), hostResponse.size());
        }

        *outRespBufOffsetPtr = static_cast<int32_t>(wasmOffset);
        *outRespLenPtr = static_cast<int32_t>(hostResponse.size());
        return static_cast<int32_t>(faabric::rpc::StatusCode::OK);
    } catch (std::exception& e) {
        SPDLOG_ERROR("RPC unary call failed: {}", e.what());
        module->doThrowException(e);
        return static_cast<int32_t>(faabric::rpc::StatusCode::INTERNAL);
    }
}

static NativeSymbol ns[] = {
    REG_NATIVE_FUNC(RPC_ChannelCreate, "(**)i"),
    REG_NATIVE_FUNC(RPC_ChannelClose, "(i)i"),
    REG_NATIVE_FUNC(RPC_UnaryCall, "(i**i**)i")
};

uint32_t getFaasmRpcApi(NativeSymbol** nativeSymbols)
{
    *nativeSymbols = ns;
    return sizeof(ns) / sizeof(NativeSymbol);
}

} // namespace wasm