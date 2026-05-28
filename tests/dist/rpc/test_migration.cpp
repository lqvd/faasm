#include <catch2/catch.hpp>
#include "fixtures.h"
#include <faabric/scheduler/Scheduler.h>
#include <faabric/util/batch.h>
#include <faabric/transport/common.h>
#include <faabric/rpc/RpcTransportClient.h>

namespace tests {

class RpcDistTestsFixture : public DistTestsFixture
{
  protected:
    void setupRpcMigrationTest(
        std::shared_ptr<faabric::BatchExecuteRequest> req,
        const std::string& hostBefore)
    {
        auto preloadDec = std::make_shared<batch_scheduler::SchedulingDecision>(
            req->appid(), req->groupid());
        preloadDec->addMessage(hostBefore, 0, 0, 0);
        plannerCli.preloadSchedulingDecision(preloadDec);
        setNextEvictedVmIp({ hostBefore });
    }
};

TEST_CASE_METHOD(DistTestsFixture,
                 "Test basic RPC service invocation",
                 "[rpc][service]")
{
    setLocalRemoteSlots(4, 4);

    // Start ping service as long-running SERVICE on host A
    auto svcReq = faabric::util::batchExecFactory("rpc", "PingSvc", 1);
    svcReq->set_type(faabric::BatchExecuteRequest::SERVICE);
    faabric::Message& msg = svcReq->mutable_messages()->at(0);
    msg.set_isrpc(true);
    
    auto svcDec = std::make_shared<batch_scheduler::SchedulingDecision>(
        svcReq->appid(), svcReq->groupid());
    svcDec->addMessage(getDistTestMasterIp(), 0, 0, 0);
    plannerCli.preloadSchedulingDecision(svcDec);
    plannerCli.callFunctions(svcReq);

    // Give the service time to register itself
    std::optional<faabric::planner::ServiceEndpoint> endpoint;
    for (int i = 0; i < 50; i++) {
        endpoint = plannerCli.resolveServiceEndpoint("rpc/PingSvc");
        if (endpoint.has_value()) {
            break;
        }

        SLEEP_MS(100);
    }

    REQUIRE(endpoint.has_value());
    REQUIRE(endpoint->host() == getDistTestMasterIp());

    // Run the client
    auto clientReq = faabric::util::batchExecFactory("rpc", "migrate_rpc", 1);
    faabric::Message& clientMsg = clientReq->mutable_messages()->at(0);
    clientMsg.set_isrpc(true);
    plannerCli.callFunctions(clientReq);

    auto clientResults = waitForBatchResults(clientReq, 1);
    REQUIRE(clientResults->messageresults_size() == 1);
    REQUIRE(clientResults->messageresults(0).returnvalue() == 0);

    // Verify all four pings made it into the output
    const auto& output = clientResults->messageresults(0).outputdata();
    REQUIRE(output.find("Pong: Hello from host A") != std::string::npos);
    REQUIRE(output.find("Pong: Hello from wherever we are now") != std::string::npos);
    REQUIRE(output.find("Pong: Fan-out C") != std::string::npos);
    REQUIRE(output.find("Pong: Fan-out D") != std::string::npos);

     // Tell the service to shut down
    faabric::rpc::RpcTransportClient ctrl(
        endpoint->host(), RPC_ASYNC_PORT, RPC_SYNC_PORT, 5000);

    faabric::RpcShutdownRequest shutdownReq;
    shutdownReq.set_targetappid(endpoint->appid());
    shutdownReq.set_targetmessageid(endpoint->messageid());
    ctrl.asyncSendShutdown(shutdownReq);

    auto svcResults = waitForBatchResults(svcReq, 1);
    REQUIRE(svcResults->messageresults_size() == 1);
    REQUIRE(svcResults->messageresults(0).returnvalue() == 0);
}

} // namespace tests
