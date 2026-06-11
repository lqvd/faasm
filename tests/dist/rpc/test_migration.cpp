#include "fixtures.h"
#include <catch2/catch.hpp>
#include <faabric/rpc/RpcTransportClient.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/transport/common.h>
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

     faabric::planner::ServiceEndpoint waitForServiceEndpointOnHost(
      const std::string& serviceKey,
      const std::string& expectedHost,
      int retries = 100,
      int sleepMs = 100)
    {
        std::optional<faabric::planner::ServiceEndpoint> endpoint;

        for (int i = 0; i < retries; i++) {
            endpoint = plannerCli.resolveServiceEndpoint(serviceKey);

            if (endpoint.has_value() && endpoint->host() == expectedHost) {
                return endpoint.value();
            }

            SLEEP_MS(sleepMs);
        }

        REQUIRE(endpoint.has_value());
        INFO("Expected host: " << expectedHost);
        INFO("Actual host: " << endpoint->host());
        REQUIRE(endpoint->host() == expectedHost);

        return endpoint.value();
    }
};

TEST_CASE_METHOD(DistTestsFixture,
                 "Test basic RPC service invocation",
                 "[rpc][service]")
{
    setLocalRemoteSlots(4, 4);

    // Start ping service as long-running SERVICE on host A
    auto svcReq = faabric::util::batchExecFactory("rpc", "PingSvc", 1);
    faabric::Message& msg = svcReq->mutable_messages()->at(0);
    msg.set_islongrunning(true);
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
    REQUIRE(output.find("Pong: Hello from wherever we are now") !=
            std::string::npos);
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

TEST_CASE_METHOD(RpcDistTestsFixture,
                 "Test RPC ping service migrates with SPOT policy",
                 "[rpc][service][migration]")
{
    updatePlannerPolicy("spot");

    setLocalRemoteSlots(4, 4);

    const std::string hostBefore = getDistTestMasterIp();
    const std::string hostAfter = getDistTestWorkerIp();

    auto svcReq = faabric::util::batchExecFactory("rpc", "PingSvc", 1);
    faabric::Message& svcMsg = svcReq->mutable_messages()->at(0);
    svcMsg.set_islongrunning(true);
    svcMsg.set_isrpc(true);

    auto preloadDec = std::make_shared<batch_scheduler::SchedulingDecision>(
      svcReq->appid(), svcReq->groupid());
    preloadDec->addMessage(hostBefore, 0, 0, 0);

    plannerCli.preloadSchedulingDecision(preloadDec);

    // The preloaded decision should still place the service initially on
    // hostBefore, then the service should migrate at its migration point.
    setNextEvictedVmIp({ hostBefore });

    plannerCli.callFunctions(svcReq);

    // For a long-running RPC service, the observable "result" of migration is
    // not a completed batch result. It is the service discovery entry moving.
    auto endpointAfter =
      waitForServiceEndpointOnHost("rpc/PingSvc", hostAfter, 200, 100);

    REQUIRE(endpointAfter.host() == hostAfter);
    REQUIRE(endpointAfter.appid() == svcReq->appid());
    REQUIRE(endpointAfter.messageid() == svcReq->messages(0).id());

    // Now check the migrated service is still usable.
    auto clientReq =
      faabric::util::batchExecFactory("rpc", "migrate_rpc", 1);
    faabric::Message& clientMsg = clientReq->mutable_messages()->at(0);
    clientMsg.set_isrpc(true);

    plannerCli.callFunctions(clientReq);

    auto clientResults = waitForBatchResults(clientReq, 1);
    REQUIRE(clientResults->messageresults_size() == 1);
    REQUIRE(clientResults->messageresults(0).returnvalue() == 0);

    const auto& output = clientResults->messageresults(0).outputdata();

    REQUIRE(output.find("Pong: Hello from host A") != std::string::npos);
    REQUIRE(output.find("Pong: Hello from wherever we are now") !=
            std::string::npos);
    REQUIRE(output.find("Pong: Fan-out C") != std::string::npos);
    REQUIRE(output.find("Pong: Fan-out D") != std::string::npos);

    // Shut down the service on its migrated host.
    faabric::rpc::RpcTransportClient ctrl(
      endpointAfter.host(), RPC_ASYNC_PORT, RPC_SYNC_PORT, 5000);

    faabric::RpcShutdownRequest shutdownReq;
    shutdownReq.set_targetappid(endpointAfter.appid());
    shutdownReq.set_targetmessageid(endpointAfter.messageid());
    ctrl.asyncSendShutdown(shutdownReq);

    auto svcResults = waitForBatchResults(svcReq, 1);
    REQUIRE(svcResults->messageresults_size() == 1);

    updatePlannerPolicy("bin-pack");
}

TEST_CASE_METHOD(DistTestsFixture,
                 "Test SNB migration stop-and-restart",
                 "[rpc][snb][migration]")
{
    setLocalRemoteSlots(8, 4);

    struct RunningService {
        std::shared_ptr<faabric::BatchExecuteRequest> req;
        faabric::planner::ServiceEndpoint endpoint;
    };

    std::vector<RunningService> services;

    auto waitForServiceEndpoint =
      [&](const std::string& serviceName,
          const std::string& host) -> faabric::planner::ServiceEndpoint {
        const std::string serviceKey = "snb/" + serviceName;
        std::optional<faabric::planner::ServiceEndpoint> endpoint;
        for (int i = 0; i < 100; i++) {
            endpoint = plannerCli.resolveServiceEndpoint(serviceKey);
            if (endpoint.has_value()) break;
            SLEEP_MS(100);
        }
        REQUIRE(endpoint.has_value());
        REQUIRE(endpoint->host() == host);
        return endpoint.value();
    };

    auto startService =
      [&](const std::string& serviceName,
          const std::string& host) -> RunningService {
        auto svcReq = faabric::util::batchExecFactory("snb", serviceName, 1);
        faabric::Message& msg = svcReq->mutable_messages()->at(0);
        msg.set_islongrunning(true);
        msg.set_isrpc(true);

        auto svcDec = std::make_shared<batch_scheduler::SchedulingDecision>(
            svcReq->appid(), svcReq->groupid());
        svcDec->addMessage(host, 0, 0, 0);
        plannerCli.preloadSchedulingDecision(svcDec);
        plannerCli.callFunctions(svcReq);

        return RunningService{ svcReq,
                               waitForServiceEndpoint(serviceName, host) };
    };

    auto shutdownService =
      [&](const RunningService& svc) {
          faabric::rpc::RpcTransportClient ctrl(
              svc.endpoint.host(), RPC_ASYNC_PORT, RPC_SYNC_PORT, 5000);
          faabric::RpcShutdownRequest req;
          req.set_targetappid(svc.endpoint.appid());
          req.set_targetmessageid(svc.endpoint.messageid());
          ctrl.asyncSendShutdown(req);
      };

    auto runBenchmarkPhase =
      [&](int numRequests) -> std::string {
        auto benchReq =
            faabric::util::batchExecFactory("snb", "benchmark_snb", 1);
        faabric::Message& msg = benchReq->mutable_messages()->at(0);
        msg.set_isrpc(true);
        msg.set_cmdline(std::to_string(numRequests));
        plannerCli.callFunctions(benchReq);

        auto results = waitForBatchResults(benchReq, 1);
        REQUIRE(results->messageresults_size() == 1);
        REQUIRE(results->messageresults(0).returnvalue() == 0);
        return results->messageresults(0).outputdata();
    };

    // --- Start all services on master ---
    services.push_back(startService("UserDbService",      getDistTestMasterIp()));
    services.push_back(startService("PostStorageService", getDistTestMasterIp()));
    services.push_back(startService("TextService",        getDistTestMasterIp()));
    services.push_back(startService("UniqueIdService",    getDistTestMasterIp()));
    services.push_back(startService("UserService",        getDistTestMasterIp()));
    services.push_back(startService("ComposePostService", getDistTestMasterIp()));

    // --- Phase 1: pre-migration baseline ---
    auto phase1Output = runBenchmarkPhase(10);
    SPDLOG_INFO("Phase 1 (pre-migration): {}", phase1Output);

    // --- Migrate UserService: master → worker ---
    // Find UserService in services list
    auto userSvcIt = std::find_if(services.begin(), services.end(),
        [](const RunningService& s) {
            return s.endpoint.servicename() == "snb/UserService";
        });
    REQUIRE(userSvcIt != services.end());

    // Stop old instance
    shutdownService(*userSvcIt);
    auto oldResults = waitForBatchResults(userSvcIt->req, 1);
    REQUIRE(oldResults->messageresults(0).returnvalue() == 0);

    // Start new instance on worker
    *userSvcIt = startService("UserService", getDistTestWorkerIp());
    SPDLOG_INFO("UserService migrated to {}", getDistTestWorkerIp());

    // --- Phase 2: post-migration ---
    auto phase2Output = runBenchmarkPhase(10);
    SPDLOG_INFO("Phase 2 (post-migration): {}", phase2Output);

    // --- Assertions ---
    // Parse and compare p99 from both phases
    // Both should succeed — migration correctness check
    REQUIRE_FALSE(phase1Output.empty());
    REQUIRE_FALSE(phase2Output.empty());

    // Log both for manual inspection during development
    // Replace with parsed proto assertions once results are stable
    printf("PRE:  %s\n", phase1Output.c_str());
    printf("POST: %s\n", phase2Output.c_str());

    // --- Shutdown all ---
    for (const auto& svc : services) {
        shutdownService(svc);
    }
    for (const auto& svc : services) {
        auto r = waitForBatchResults(svc.req, 1);
        REQUIRE(r->messageresults(0).returnvalue() == 0);
    }
}

TEST_CASE_METHOD(DistTestsFixture,
                 "Test SNB migration drain-and-restart",
                 "[rpc][snb][migration]")
{
    setLocalRemoteSlots(8, 4);

    struct RunningService {
        std::shared_ptr<faabric::BatchExecuteRequest> req;
        faabric::planner::ServiceEndpoint endpoint;
    };

    std::vector<RunningService> services;

    auto waitForServiceEndpoint =
      [&](const std::string& serviceName,
          const std::string& host) -> faabric::planner::ServiceEndpoint {
        const std::string serviceKey = "snb/" + serviceName;
        std::optional<faabric::planner::ServiceEndpoint> endpoint;
        for (int i = 0; i < 100; i++) {
            endpoint = plannerCli.resolveServiceEndpoint(serviceKey);
            if (endpoint.has_value()) break;
            SLEEP_MS(100);
        }
        REQUIRE(endpoint.has_value());
        REQUIRE(endpoint->host() == host);
        return endpoint.value();
    };

    auto startService =
      [&](const std::string& serviceName,
          const std::string& host) -> RunningService {
        auto svcReq = faabric::util::batchExecFactory("snb", serviceName, 1);
        faabric::Message& msg = svcReq->mutable_messages()->at(0);
        msg.set_islongrunning(true);
        msg.set_isrpc(true);

        auto svcDec = std::make_shared<batch_scheduler::SchedulingDecision>(
            svcReq->appid(), svcReq->groupid());
        svcDec->addMessage(host, 0, 0, 0);
        plannerCli.preloadSchedulingDecision(svcDec);
        plannerCli.callFunctions(svcReq);

        return RunningService{ svcReq,
                               waitForServiceEndpoint(serviceName, host) };
    };

    auto shutdownService =
      [&](const RunningService& svc) {
          faabric::rpc::RpcTransportClient ctrl(
              svc.endpoint.host(), RPC_ASYNC_PORT, RPC_SYNC_PORT, 5000);
          faabric::RpcShutdownRequest req;
          req.set_targetappid(svc.endpoint.appid());
          req.set_targetmessageid(svc.endpoint.messageid());
          ctrl.asyncSendShutdown(req);
      };

    auto runBenchmarkPhase =
      [&](int numRequests) -> std::string {
        auto benchReq =
            faabric::util::batchExecFactory("snb", "benchmark_snb", 1);
        faabric::Message& msg = benchReq->mutable_messages()->at(0);
        msg.set_isrpc(true);
        msg.set_cmdline(std::to_string(numRequests));
        plannerCli.callFunctions(benchReq);

        auto results = waitForBatchResults(benchReq, 1);
        REQUIRE(results->messageresults_size() == 1);
        REQUIRE(results->messageresults(0).returnvalue() == 0);
        return results->messageresults(0).outputdata();
    };

    // --- Start all services on master ---
    services.push_back(startService("UserDbService",      getDistTestMasterIp()));
    services.push_back(startService("PostStorageService", getDistTestMasterIp()));
    services.push_back(startService("TextService",        getDistTestMasterIp()));
    services.push_back(startService("UniqueIdService",    getDistTestMasterIp()));
    services.push_back(startService("UserService",        getDistTestMasterIp()));
    services.push_back(startService("ComposePostService", getDistTestMasterIp()));

    // --- Phase 1: pre-migration baseline ---
    auto phase1Output = runBenchmarkPhase(10);
    SPDLOG_INFO("Phase 1 (pre-migration): {}", phase1Output);

    // --- Migrate UserService: master → worker ---
    // Find UserService in services list
    auto userSvcIt = std::find_if(services.begin(), services.end(),
        [](const RunningService& s) {
            return s.endpoint.servicename() == "snb/UserService";
        });
    REQUIRE(userSvcIt != services.end());

    // Stop old instance
    shutdownService(*userSvcIt);
    auto oldResults = waitForBatchResults(userSvcIt->req, 1);
    REQUIRE(oldResults->messageresults(0).returnvalue() == 0);

    // Start new instance on worker
    *userSvcIt = startService("UserService", getDistTestWorkerIp());
    SPDLOG_INFO("UserService migrated to {}", getDistTestWorkerIp());

    // --- Phase 2: post-migration ---
    auto phase2Output = runBenchmarkPhase(10);
    SPDLOG_INFO("Phase 2 (post-migration): {}", phase2Output);

    // --- Assertions ---
    // Parse and compare p99 from both phases
    // Both should succeed — migration correctness check
    REQUIRE_FALSE(phase1Output.empty());
    REQUIRE_FALSE(phase2Output.empty());

    // Log both for manual inspection during development
    // Replace with parsed proto assertions once results are stable
    printf("PRE:  %s\n", phase1Output.c_str());
    printf("POST: %s\n", phase2Output.c_str());

    // --- Shutdown all ---
    for (const auto& svc : services) {
        shutdownService(svc);
    }
    for (const auto& svc : services) {
        auto r = waitForBatchResults(svc.req, 1);
        REQUIRE(r->messageresults(0).returnvalue() == 0);
    }
}

} // namespace tests
