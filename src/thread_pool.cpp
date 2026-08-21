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
		//is required. set the reseed variable; this stops pending requests
		//from being processed
		{
			std::unique_lock<std::mutex> lock(mutex_);
			reseeding_ = true;
		}
		//wait until all workers are idle (except of the worker who is running this code)
		//this guarantees no worker is mid-hash against the victim slot's
		//cache/dataset before we start overwriting its contents below.
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
		reseeding_ = false;
		cond_.notify_all();
	}
}