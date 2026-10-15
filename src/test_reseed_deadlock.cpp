/*
Regression test for the production incident where two (or more) worker
threads landing concurrent /seed POSTs for not-yet-resident seeds would
permanently deadlock inside ThreadPool::reseed() (src/thread_pool.cpp).

Root cause recap: each worker only sets idle_ = true *between* jobs. If
two workers both enter reseed()'s slow path at nearly the same time (both
saw touchSeed() == true before either had reinited anything), each one's
"wait for every other worker to go idle" loop blocks on the *other*
worker, which is itself mid-job inside its own reseed() call and can never
go idle until that call returns -- a permanent mutual wait. In production
this manifested as the daemon staying alive (near-zero CPU) but never
responding to any request again, including GET /info, until it was
manually restarted.

This test drives the real HTTP server (not just the internal classes) so
it exercises the exact code path a burst of concurrent /seed requests
takes in production: Service::run() -> httplib::Server -> ThreadPool
job dispatch -> ThreadPool::reseed().

Design notes:
  - RANDOMX_FLAG_DEFAULT (flags = 0) is used so cache init is cheap
    (Argon2 over a few hundred bytes) and no full dataset is built, so
    this test runs in well under a second once the race itself isn't
    hanging.
  - Every blocking network call (httplib::Client) carries its own short
    socket timeout, so a deadlocked daemon causes the *client* calls to
    fail fast with a timeout rather than hang the test process.
  - A hard outer watchdog thread additionally guarantees the whole test
    process exits (reporting failure) within a fixed wall-clock budget
    no matter what, as defense in depth against any other way the
    daemon could wedge.
  - On detecting a deadlock, the process calls std::_Exit() directly
    instead of returning normally: the daemon's worker threads may be
    unjoinable forever, and letting `Service`/`ThreadPool` destructors
    run against them is unsafe (and pointless -- the whole point of this
    test is to fail loudly and immediately).
*/

#include "service.h"
#include "httplib.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <iostream>
#include <mutex>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#if defined(_WIN32)
#include <process.h>
#define GETPID() _getpid()
#else
#include <unistd.h>
#define GETPID() getpid()
#endif

using namespace std::chrono;

namespace {

	// Total wall-clock budget for the whole test. If we haven't printed a
	// PASS/FAIL and exited by this point, something is wedged in a way
	// the per-request client timeouts didn't catch either -- treat that
	// as a failure too rather than hanging the build/CI forever.
	constexpr int kHardWatchdogSeconds = 30;

	// Per-HTTP-call socket timeout. Comfortably longer than any legitimate
	// cache-only (non-full-mem) reseed should ever take, but far shorter
	// than "hang forever".
	constexpr int kClientTimeoutSeconds = 8;

	constexpr int kWorkerThreads = 3;
	constexpr int kSeedSlots = 2;

	bool waitForServerUp(const char* host, int port, milliseconds timeout) {
		auto deadline = steady_clock::now() + timeout;
		while (steady_clock::now() < deadline) {
			httplib::Client cli(host, port, 1);
			auto res = cli.Get("/info");
			if (res && res->status == 200) {
				return true;
			}
			std::this_thread::sleep_for(milliseconds(25));
		}
		return false;
	}

	std::string makeSeed(int i) {
		// 32 distinct, non-degenerate bytes per worker -- well under the
		// service's 60-byte /seed body cap.
		std::mt19937 rng(0xC0FFEEu + i);
		std::string s(32, '\0');
		for (auto& c : s) {
			c = (char)(rng() & 0xff);
		}
		s[0] = (char)(i + 1); // guarantee distinctness across workers
		return s;
	}

}

int main() {
	std::thread watchdog([] {
		std::this_thread::sleep_for(seconds(kHardWatchdogSeconds));
		std::cerr << "FAIL: hard watchdog fired after " << kHardWatchdogSeconds
			<< "s -- test did not complete (reseed() deadlock?)" << std::endl;
		std::_Exit(3);
	});
	watchdog.detach();

	const char* host = "127.0.0.1";
	int port = 39500 + (int)(GETPID() % 500);

	// flags = 0 (RANDOMX_FLAG_DEFAULT): cache-only, no JIT, no full
	// dataset -- keeps reinitSlot()/reinitDataset() cheap so this test is
	// fast, while still exercising the exact production code path.
	randomx::Service svc(kWorkerThreads, 0, kSeedSlots);

	std::thread serverThread([&] {
		svc.run(host, port);
	});

	if (!waitForServerUp(host, port, milliseconds(5000))) {
		std::cerr << "FAIL: server never came up on " << host << ":" << port << std::endl;
		std::_Exit(2);
	}

	std::vector<std::string> seeds;
	for (int i = 0; i < kWorkerThreads; ++i) {
		seeds.push_back(makeSeed(i));
	}

	// Barrier so all kWorkerThreads /seed POSTs fire as close together as
	// possible, maximizing the pre-fix race window (each sees touchSeed()
	// == true before any of the others has finished reiniting).
	std::mutex startMutex;
	std::condition_variable startCv;
	int readyCount = 0;
	bool go = false;

	std::atomic<int> successCount(0);
	std::vector<std::thread> seedThreads;
	for (int i = 0; i < kWorkerThreads; ++i) {
		seedThreads.emplace_back([&, i] {
			{
				std::unique_lock<std::mutex> lock(startMutex);
				++readyCount;
				startCv.notify_all();
				startCv.wait(lock, [&] { return go; });
			}
			httplib::Client cli(host, port, kClientTimeoutSeconds);
			auto res = cli.Post("/seed", seeds[i], "application/x.randomx+bin");
			if (res && res->status == 204) {
				++successCount;
			}
		});
	}

	{
		std::unique_lock<std::mutex> lock(startMutex);
		startCv.wait(lock, [&] { return readyCount == kWorkerThreads; });
		go = true;
	}
	startCv.notify_all();

	// Each thread is bounded by its own Client's kClientTimeoutSeconds
	// socket timeout, so this join() is bounded even if the daemon is
	// fully wedged server-side.
	for (int i = 0; i < (int)seedThreads.size(); ++i) {
		seedThreads[i].join();
	}

	bool allSeedsOk = (successCount.load() == kWorkerThreads);

	// The definitive check: whether the daemon is still alive and
	// responsive at all afterwards, matching the production symptom
	// exactly ("daemon alive, CPU near-zero, but permanently
	// unresponsive to ALL requests including /info").
	httplib::Client infoClient(host, port, kClientTimeoutSeconds);
	auto infoRes = infoClient.Get("/info");
	bool infoOk = (bool)infoRes && infoRes->status == 200;

	if (!allSeedsOk || !infoOk) {
		std::cerr << "FAIL: reseed() deadlock reproduced -- "
			<< successCount.load() << "/" << kWorkerThreads
			<< " concurrent /seed calls completed, GET /info "
			<< (infoOk ? "responded" : "timed out / unreachable")
			<< " afterwards." << std::endl;
		// Do not attempt to join the server thread or let `svc` be
		// destroyed: its worker threads may be permanently deadlocked,
		// and std::thread's destructor calls std::terminate() on a
		// still-joinable thread -- exit the whole process immediately
		// instead.
		std::_Exit(1);
	}

	std::cout << "PASS: " << successCount.load() << "/" << kWorkerThreads
		<< " concurrent /seed calls to distinct not-yet-resident seeds "
		<< "completed and GET /info still responds -- no reseed() deadlock."
		<< std::endl;
	std::_Exit(0);
}
