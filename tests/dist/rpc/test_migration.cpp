#include <catch2/catch.hpp>
#include "fixtures.h"
#include <faabric/scheduler/Scheduler.h>
#include <faabric/util/batch.h>

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
                 "Test migrating a coroutine RPC function",
                 "[rpc][migration]")
{
    updatePlannerPolicy("spot");
    setLocalRemoteSlots(4, 4);

    auto req = faabric::util::batchExecFactory("rpc", "migrate_rpc", 1);
    faabric::Message& msg = req->mutable_messages()->at(0);
    msg.set_isrpc(true);

    std::string hostBefore = getDistTestMasterIp();
    std::string hostAfter = getDistTestWorkerIp();

    auto preloadDec = std::make_shared<batch_scheduler::SchedulingDecision>(
        req->appid(), req->groupid());
    preloadDec->addMessage(hostBefore, 0, 0, 0);
    plannerCli.preloadSchedulingDecision(preloadDec);

    setNextEvictedVmIp({ hostBefore });

    plannerCli.callFunctions(req);

    // Wait for the single message result
    auto batchResults = waitForBatchResults(req, 1);
    REQUIRE(batchResults->messageresults_size() == 1);

    const auto& result = batchResults->messageresults(0);
    REQUIRE(result.returnvalue() == 0);
    REQUIRE(result.executedhost() == hostAfter);

    updatePlannerPolicy("bin-pack");
}

TEST_CASE_METHOD(RpcDistTestsFixture,
                 "RPC forwarding of in-flight response",
                 "[rpc][migration][forwarding]")
{
    updatePlannerPolicy("spot");
    setLocalRemoteSlots(4, 4);

    auto req = faabric::util::batchExecFactory("rpc", "rpc_forwarding", 1);
    faabric::Message& msg = req->mutable_messages()->at(0);
    msg.set_isrpc(true);

    std::string hostBefore = getDistTestMasterIp();
    std::string hostAfter = getDistTestWorkerIp();

    setupRpcMigrationTest(req, hostBefore);
    plannerCli.callFunctions(req);

    auto batchResults = waitForBatchResults(req, 1);
    REQUIRE(batchResults->messageresults_size() == 1);

    auto result = batchResults->messageresults(0);
    REQUIRE(result.returnvalue() == 0);
    REQUIRE(result.executedhost() == hostAfter);
    REQUIRE(result.executedhost() != hostBefore);

    updatePlannerPolicy("bin-pack");
}

TEST_CASE_METHOD(RpcDistTestsFixture,
                 "RPC pending response survives migration",
                 "[rpc][migration][pending]")
{
    updatePlannerPolicy("spot");
    setLocalRemoteSlots(4, 4);

    auto req = faabric::util::batchExecFactory("rpc", "rpc_pending", 1);
    faabric::Message& msg = req->mutable_messages()->at(0);
    msg.set_isrpc(true);

    std::string hostBefore = getDistTestMasterIp();
    std::string hostAfter = getDistTestWorkerIp();

    setupRpcMigrationTest(req, hostBefore);
    plannerCli.callFunctions(req);

    auto batchResults = waitForBatchResults(req, 1);
    REQUIRE(batchResults->messageresults_size() == 1);

    auto result = batchResults->messageresults(0);
    REQUIRE(result.returnvalue() == 0);
    REQUIRE(result.executedhost() == hostAfter);
    REQUIRE(result.executedhost() != hostBefore);

    updatePlannerPolicy("bin-pack");
}

TEST_CASE_METHOD(RpcDistTestsFixture,
                 "RPC cached response survives migration",
                 "[rpc][migration][cached]")
{
    updatePlannerPolicy("spot");
    setLocalRemoteSlots(4, 4);

    auto req = faabric::util::batchExecFactory("rpc", "rpc_cached", 1);
    faabric::Message& msg = req->mutable_messages()->at(0);
    msg.set_isrpc(true);

    std::string hostBefore = getDistTestMasterIp();
    std::string hostAfter = getDistTestWorkerIp();

    setupRpcMigrationTest(req, hostBefore);
    plannerCli.callFunctions(req);

    auto batchResults = waitForBatchResults(req, 1);
    REQUIRE(batchResults->messageresults_size() == 1);

    auto result = batchResults->messageresults(0);
    REQUIRE(result.returnvalue() == 0);
    REQUIRE(result.executedhost() == hostAfter);
    REQUIRE(result.executedhost() != hostBefore);

    updatePlannerPolicy("bin-pack");
}

} // namespace tests
