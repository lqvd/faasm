#include <catch2/catch.hpp>

#include "fixtures.h"

#include <faabric/scheduler/Scheduler.h>
#include <faabric/util/batch.h>

namespace tests {

#include <catch2/catch.hpp>

#include "fixtures.h"

#include <faabric/scheduler/Scheduler.h>
#include <faabric/util/batch.h>

namespace tests {

TEST_CASE_METHOD(DistTestsFixture,
                 "Test migrating an RPC function with SPOT policy",
                 "[rpc]")
{
    updatePlannerPolicy("spot");

    setLocalRemoteSlots(4, 4);

    std::shared_ptr<faabric::BatchExecuteRequest> req =
      faabric::util::batchExecFactory("rpc", "migrate_rpc", 1);

    faabric::Message& msg = req->mutable_messages()->at(0);
    msg.set_isrpc(true);

    int numLoops = 10000;
    int checkAt = 5;
    msg.set_cmdline(fmt::format("{} {}", checkAt, numLoops));

    std::string hostBefore = getDistTestMasterIp();
    std::string hostAfter = getDistTestWorkerIp();

    auto preloadDec = std::make_shared<batch_scheduler::SchedulingDecision>(
      req->appid(), req->groupid());
    preloadDec->addMessage(hostBefore, 0, 0, 0);
    plannerCli.preloadSchedulingDecision(preloadDec);

    // Force migration away from the initial host under SPOT policy
    setNextEvictedVmIp({ hostBefore });

    plannerCli.callFunctions(req);

    auto batchResults = waitForBatchResults(req, 1);
    REQUIRE(batchResults->messageresults_size() == 1);

    auto result = batchResults->messageresults(0);
    REQUIRE(result.returnvalue() == 0);
    REQUIRE(result.executedhost() == hostAfter);

    updatePlannerPolicy("bin-pack");
}

} // namespace tests

}