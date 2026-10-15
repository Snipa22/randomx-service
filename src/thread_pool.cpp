/*
Copyright (c) 2020, tevador <tevador@gmail.com>

All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:
	* Redistributions of source code must retain the above copyright
	  notice, this list of conditions and the following disclaimer.
	* Redistributions in binary form must reproduce the above copyright
	  notice, this list of conditions and the following disclaimer in the
	  documentation and/or other materials provided with the distribution.
	* Neither the name of the copyright holder nor the
	  names of its contributors may be used to endorse or promote products
	  derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

#include "thread_pool.h"
#include "service_worker.h"
#include "service.h"

namespace randomx {

	ThreadPool::ThreadPool(Service& svc, size_t n) :
		svc_(svc),
		shutdown_(false),
		reseeding_(false)
	{
		workers_.reserve(n);
		for (unsigned i = 0; i < n; ++i) {
			workers_.push_back(std::make_shared<ServiceWorker>(*this, i));
			auto t = std::make_shared<std::thread>(std::ref(*workers_.back()));
			threads_.push_back(t);
		}
	}

	ThreadPool::~ThreadPool()
	{
	}

	void ThreadPool::enqueue(std::function<void(ServiceWorker&)> fn) {
		std::unique_lock<std::mutex> lock(mutex_);
		jobs_.push_back(fn);
		cond_.notify_one();
	}

	void ThreadPool::shutdown() {
		{
			std::unique_lock<std::mutex> lock(mutex_);
			shutdown_ = true;
		}
		cond_.notify_all();
		for (auto t : threads_) {
			t->join();
		}
	}

	void ThreadPool::reseed(ServiceWorker& self, const void* seed, size_t length) {
		//Fast path: if this seed is already resident in one of the service's
		//seed slots, just bump its LRU timestamp and return -- no worker
		//needs to be drained and no cache/dataset touched. This is the
		//actual fix for the original single-seed thrashing problem: two
		//callers alternating between N<=slots distinct seeds never pay a
		//full reseed after the first time each seed is primed.
		size_t victimSlot;
		if (!svc_.touchSeed(seed, length, victimSlot)) {
			return;
		}

		//Slow path: the seed isn't resident in any slot, so a real eviction
		//is required.
		//
		//PRODUCTION INCIDENT FIX: this is a single-flight guard around the
		//"decide a real reseed is needed, drain siblings idle, reinit the
		//victim slot" sequence below. Without it, two worker threads that
		//each pick up an independent /seed request for a not-yet-resident
		//seed at nearly the same instant (exactly what happens when a burst
		//of miners' validate calls each independently re-POST /seed right
		//after a restart) would BOTH reach the "wait until all siblings are
		//idle" loop below concurrently. Each one's loop includes the OTHER
		//worker, which is itself mid-job running its own reseed() call (not
		//idle, and can never become idle until *its* reseed() call
		//returns) -- a permanent two-thread (generalizes to N-thread)
		//mutual deadlock: idle_ never becomes true for either side, so
		//waitIdle() never returns, so reinitSlot() never runs, so the
		//daemon wedges solid (CPU near-zero, unresponsive to every request
		//including /info) until it is forcibly restarted.
		//
		//Naively wrapping the WHOLE function body (including the waitIdle()
		//loop below) in one mutex held by whichever thread gets there first
		//does NOT fix this: the second thread would then be blocked
		//*acquiring* that mutex for the full duration of its /seed job, so
		//from the ThreadPool's point of view it is still "busy" (idle_ is
		//only ever flipped to true when a worker's job function returns --
		//see ServiceWorker::operator()()). The first thread's waitIdle()
		//call on that second, mutex-blocked thread would then never
		//return either -- the exact same deadlock, just one level removed.
		//
		//The fix: a thread that loses the race for reseedMutex_ marks
		//itself idle_ = true *before* blocking on the mutex (so it stops
		//looking "busy" to any sibling's waitIdle() call), blocks, then
		//flips idle_ back to false once it actually acquires the mutex.
		//This breaks the cycle: the winning thread's waitIdle() calls on
		//any losing thread(s) return immediately (they're marked idle),
		//so the winner can finish its drain-and-reinit and release the
		//mutex; only then does a losing thread resume, re-check
		//touchSeed() (which will now see the winner's seed as resident in
		//the common case -- all callers racing to prime the same new block
		//seed -- and return via the fast path immediately), and only fall
		//through to its own drain-and-reinit if it genuinely needs a
		//different seed than what just got primed. Because reseedMutex_ is
		//only ever acquired by a thread's own code (never by a thread
		//waiting on behalf of another), and idle_ transitions are always
		//made under the owning worker's own mutex_ with a notify and
		//re-checked via a predicate in waitIdle()'s cond_.wait(), there is
		//no missed-wakeup window and no new deadlock between reseedMutex_
		//and the existing mutex_/cond_: reseedMutex_ is never held across
		//a wait on mutex_ or cond_, and mutex_ is only ever held for
		//short, non-blocking critical sections (flipping reseeding_).
		std::unique_lock<std::mutex> reseedGuard(reseedMutex_, std::try_to_lock);
		if (!reseedGuard.owns_lock()) {
			self.setIdle(true);
			reseedGuard.lock();
			self.setIdle(false);

			//Someone else just finished a reseed while we were waiting for
			//the guard. Re-check: if it happened to be for exactly the
			//seed we wanted (the common "whole fleet reacting to the same
			//new block seed" case), we're done -- fast path, no redundant
			//reseed.
			if (!svc_.touchSeed(seed, length, victimSlot)) {
				return;
			}
		}

		//At this point exactly one thread (this one) holds reseedMutex_
		//and victimSlot refers to a slot touchSeed() has already marked
		//uninitialized on our behalf. set the reseed variable; this stops
		//pending requests from being processed.
		{
			std::unique_lock<std::mutex> lock(mutex_);
			reseeding_ = true;
		}
		//wait until all workers are idle (except of the worker who is running this code)
		//this guarantees no worker is mid-hash against the victim slot's
		//cache/dataset before we start overwriting its contents below.
		//Because reseedMutex_ serializes concurrent reseed() calls, any
		//sibling worker that is itself inside reseed() right now is either
		//genuinely idle, or has already marked itself idle_ = true while
		//waiting for reseedMutex_ above -- so this loop can never block on
		//a sibling that is deadlocked waiting on us.
		for (auto& worker : workers_) {
			if (&self != worker.get()) {
				worker->waitIdle();
			}
		}
		//reinitialize ONLY the victim slot's cache and dataset; every other
		//slot's already-primed state is left completely untouched.
		svc_.reinitSlot(victimSlot, seed, length);
		//note: worker VMs are NOT proactively refreshed here -- each worker
		//re-points its own VM (cheaply, via randomx_vm_set_cache/dataset)
		//to whichever slot a given /hash or /batch request actually needs,
		//right before computing that request's hash. See
		//Service::bindWorkerToSlot.
		//notify workers
		{
			std::unique_lock<std::mutex> lock(mutex_);
			reseeding_ = false;
		}
		cond_.notify_all();
		//reseedGuard (and therefore reseedMutex_) is released here, as its
		//unique_lock goes out of scope.
	}
}