#define CATCH_CONFIG_RUNNER

#include "faabric_utils.h"

#include <catch2/catch.hpp>
#include <chrono>
#include <thread>

#include <faabric/executor/ExecutorFactory.h>
#include <faabric/proto/faabric.pb.h>
#include <faabric/rpc/RpcServer.h>
#include <faabric/rpc/rpc.h>
#include <faabric/runner/FaabricMain.h>
#include <faabric/util/logging.h>
#include <faaslet/Faaslet.h>
#include <storage/S3Wrapper.h>

using namespace faabric::scheduler;

FAABRIC_CATCH_LOGGER

int main(int argc, char* argv[])
{
    faabric::util::initLogging();
    storage::initFaasmS3();

    // Start everything up
    SPDLOG_INFO("Starting distributed test server on master");
    std::shared_ptr<faaslet::FaasletFactory> fac =
      std::make_shared<faaslet::FaasletFactory>();

    faabric::runner::FaabricMain m(fac);
    m.startBackground();

    m.registerRpcHandler("ping",
        [](const uint8_t* reqData, size_t reqLen, std::vector<uint8_t>& respData) {
            SPDLOG_INFO("Worker received RPC ping payload of length {}", reqLen);
            
            std::this_thread::sleep_for(std::chrono::milliseconds(2000));

            // Return dummy empty response
            faabric::EmptyResponse resp;
            auto size = resp.ByteSizeLong();
            respData.resize(size);
            resp.SerializeToArray(respData.data(), size);
            
            return Rpc_Status{ Rpc_StatusCode::OK, "" };
        });
    m.registerRpcHandler("ping_slow",
        [](const uint8_t* reqData, size_t reqLen, std::vector<uint8_t>& respData) {
            SPDLOG_INFO("Worker received RPC slow ping payload of length {}", reqLen);

            std::this_thread::sleep_for(std::chrono::milliseconds(8000));

            faabric::EmptyResponse resp;
            auto size = resp.ByteSizeLong();
            respData.resize(size);
            resp.SerializeToArray(respData.data(), size);
            
            return Rpc_Status{ Rpc_StatusCode::OK, "" }; 
        });

    // Wait for things to start
    usleep(3000 * 1000);

    // Run the tests
    int result = Catch::Session().run(argc, argv);
    fflush(stdout);

    // Shut down
    SPDLOG_INFO("Shutting down");
    m.shutdown();
    storage::shutdownFaasmS3();

    return result;
}
