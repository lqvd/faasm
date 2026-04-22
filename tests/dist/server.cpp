#include <conf/FaasmConfig.h>
#include <faaslet/Faaslet.h>
#include <storage/S3Wrapper.h>

#include <faabric/endpoint/FaabricEndpoint.h>
#include <faabric/executor/ExecutorFactory.h>
#include <faabric/proto/faabric.pb.h>
#include <faabric/rpc/RpcServer.h>
#include <faabric/rpc/rpc.h>
#include <faabric/runner/FaabricMain.h>
#include <faabric/util/logging.h>

using namespace faabric::executor;

int main()
{
    faabric::util::initLogging();
    storage::initFaasmS3();

    // WARNING: All 0MQ-related operations must take place in a self-contined
    // scope to ensure all sockets are destructed before closing the context.
    {
        SPDLOG_INFO("Starting distributed test server on worker");
        std::shared_ptr<ExecutorFactory> fac =
          std::make_shared<faaslet::FaasletFactory>();
        faabric::runner::FaabricMain m(fac);
        m.startBackground();

        m.registerRpcHandler("ping",
            [](const uint8_t* reqData, size_t reqLen, std::vector<uint8_t>& respData) {
                SPDLOG_INFO("Worker received RPC ping payload of length {}", reqLen);
                
                // Return dummy empty response
                faabric::EmptyResponse resp;
                auto size = resp.ByteSizeLong();
                respData.resize(size);
                resp.SerializeToArray(respData.data(), size);
                
                return Rpc_Status{ Rpc_StatusCode::OK, "" };
            });

        SPDLOG_INFO("Starting HTTP endpoint on worker");
        faabric::endpoint::FaabricEndpoint endpoint;
        endpoint.start(faabric::endpoint::EndpointMode::SIGNAL);

        SPDLOG_INFO("Shutting down");
        m.shutdown();
    }
    storage::shutdownFaasmS3();

    return 0;
}
