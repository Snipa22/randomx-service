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

#pragma once

#include <string>
#include <memory>

#define RANDOMX_SERVICE_VERSION "1.0.2"

namespace httplib {
	struct Request;
	struct Response;
}

struct randomx_vm;

namespace randomx {

	struct ServiceWorker;
	struct ServicePrivate;

	class Service {
	public:
		// seedSlots: number of concurrently-held seed/cache/dataset slots
		// (default ServicePrivate::DefaultSeedSlots, i.e. 3). See -seeds in
		// main.cpp.
		Service(size_t threads, int flags, size_t seedSlots = 0);
		~Service();
		bool run(const char* hostname, int port);
		randomx_vm* createMachine() const;
		void destroyMachine(randomx_vm* machine) const;
		void refreshMachine(randomx_vm* machine) const;

		// Multi-seed slot management (see doc/API.md and service_private.h
		// for the eviction policy). Returns true if seed bytes were not
		// already resident in any slot and reinitSlot() must be called with
		// the returned victim slot index; returns false (no work needed,
		// fast path) if the seed was already primed in an existing slot.
		bool touchSeed(const void* seed, size_t seedSize, size_t& victimSlot);
		// Actually reinitializes ONLY the given slot's cache+dataset with
		// the new seed. Only ever called after touchSeed() returns true and
		// all worker threads have been drained idle by ThreadPool::reseed,
		// so it never races with in-flight hashing against that slot.
		void reinitSlot(size_t slot, const void* seed, size_t seedSize);
		void reinitDataset(size_t slot);

		// Resolves which slot a /hash or /batch request should use. Returns
		// the slot index, or -1 if the request's RandomX-Seed header (if
		// present) doesn't match any currently-primed slot. If no header is
		// present, resolves to the most-recently-activated slot (matches
		// pre-multi-slot behavior for single-seed clients).
		int resolveSlotForRequest(const httplib::Request& req);
		// Points a worker's VM at the given slot's cache/dataset, skipping
		// the (cheap, but non-free) randomx_vm_set_cache/dataset call if the
		// worker's VM is already pointed there.
		void bindWorkerToSlot(ServiceWorker& worker, int slot) const;

		bool checkSeed(const httplib::Request& req);
		void setOrigin(const std::string& origin);
		void enableLog();
		bool allowCors(const char* method, const httplib::Request& req, httplib::Response& res);
		static int getAutoFlags();
		static int getMachineThreads();
		static size_t getDefaultSeedSlots();
		int getFlags() const;
	private:
		std::unique_ptr<ServicePrivate> data_;
	};

}
