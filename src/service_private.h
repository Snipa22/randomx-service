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

#include <cstdint>
#include <atomic>
#include <iostream>
#include <climits>
#include <string>
#include <vector>
#include <mutex>
#include "../RandomX/src/randomx.h"
#include "httplib.h"
#include "thread_pool.h"

namespace randomx {

	class Service;
	class ServiceWorker;

	// A single seed's worth of primed RandomX state. Multiple slots let the
	// service hold several independent seeds concurrently instead of the
	// original single-cache/single-dataset design, which forced a full
	// (expensive, memory-hard) reseed any time a request's seed differed
	// from whatever was currently active.
	struct SeedSlot {
		randomx_cache* cache = nullptr;
		randomx_dataset* dataset = nullptr;
		std::string seedHex;
		bool initialized = false;
		// Monotonically increasing "logical timestamp" of last use, drawn
		// from ServicePrivate::lruClock_. Used to implement least-recently-
		// used eviction across slots. Not a wall-clock value.
		uint64_t lastUsed = 0;
	};

	struct ServicePrivate {
		static const int AutoFlags = INT_MAX;
		// Default number of concurrently-held seed slots when the caller
		// (main.cpp's -seeds flag) doesn't specify one explicitly.
		static const size_t DefaultSeedSlots = 3;

		ServicePrivate(Service& svc, int threads, int flags, size_t seedSlots = DefaultSeedSlots)
			:
			server_([&svc, threads] { return new ThreadPool(svc, threads); }),
			threads_(threads),
			initialized_(false),
			currentSlot_(0),
			lruClock_(0)
		{
			if (seedSlots == 0) {
				seedSlots = 1;
			}

			bool autoFlags = flags == AutoFlags;
			if (autoFlags) {
				flags = randomx_get_flags() | RANDOMX_FLAG_FULL_MEM | RANDOMX_FLAG_LARGE_PAGES;
			}

			// Probe allocation behavior using the first slot; the same flags
			// are then reused (without re-probing) for the remaining slots,
			// since RandomX flags are a service-wide setting, not a per-slot
			// one.
			randomx_cache* probeCache = randomx_alloc_cache((randomx_flags)flags);
			if (autoFlags && probeCache == nullptr) {
				std::cout << "RANDOMX_FLAG_LARGE_PAGES was not successful (randomx_cache)" << std::endl;
				flags &= ~RANDOMX_FLAG_LARGE_PAGES;
				probeCache = randomx_alloc_cache((randomx_flags)flags);
			}
			if (probeCache == nullptr) {
				throw std::runtime_error("randomx_alloc_cache failed");
			}

			randomx_dataset* probeDataset = nullptr;
			if (flags & RANDOMX_FLAG_FULL_MEM) {
				probeDataset = randomx_alloc_dataset((randomx_flags)flags);
				if (probeDataset == nullptr) {
					if (autoFlags) {
						std::cout << "RANDOMX_FLAG_LARGE_PAGES was not successful (randomx_dataset)" << std::endl;
						flags &= ~RANDOMX_FLAG_LARGE_PAGES;
						probeDataset = randomx_alloc_dataset((randomx_flags)flags);
						if (probeDataset == nullptr) {
							std::cout << "RANDOMX_FLAG_FULL_MEM was not successful" << std::endl;
							flags &= ~RANDOMX_FLAG_FULL_MEM;
						}
					}
					else {
						throw std::runtime_error("randomx_alloc_dataset failed");
					}
				}
			}
			flags_ = (randomx_flags)flags;

			slots_.resize(seedSlots);
			slots_[0].cache = probeCache;
			slots_[0].dataset = probeDataset;

			for (size_t i = 1; i < seedSlots; ++i) {
				// Slot 0 (the probe above) may get "lucky" and succeed with
				// RANDOMX_FLAG_LARGE_PAGES even when the kernel's hugepage
				// pool is nearly exhausted (e.g. right after a restart, with
				// another process's pages not yet released). A later slot's
				// allocation can still fail under that same flag. Mirror the
				// probe's own graceful degradation here instead of throwing
				// on the first failure: retry with large pages stripped
				// (and, if the dataset alloc still fails, with full-mem
				// stripped too) before giving up.
				randomx_flags slotFlags = flags_;

				slots_[i].cache = randomx_alloc_cache(slotFlags);
				if (slots_[i].cache == nullptr && (slotFlags & RANDOMX_FLAG_LARGE_PAGES)) {
					std::cout << "RANDOMX_FLAG_LARGE_PAGES was not successful (randomx_cache, seed slot " << i << ")" << std::endl;
					slotFlags = (randomx_flags)(slotFlags & ~RANDOMX_FLAG_LARGE_PAGES);
					slots_[i].cache = randomx_alloc_cache(slotFlags);
				}
				if (slots_[i].cache == nullptr) {
					throw std::runtime_error("randomx_alloc_cache failed for seed slot");
				}

				if (flags_ & RANDOMX_FLAG_FULL_MEM) {
					slots_[i].dataset = randomx_alloc_dataset(slotFlags);
					if (slots_[i].dataset == nullptr && (slotFlags & RANDOMX_FLAG_LARGE_PAGES)) {
						std::cout << "RANDOMX_FLAG_LARGE_PAGES was not successful (randomx_dataset, seed slot " << i << ")" << std::endl;
						slotFlags = (randomx_flags)(slotFlags & ~RANDOMX_FLAG_LARGE_PAGES);
						slots_[i].dataset = randomx_alloc_dataset(slotFlags);
					}
					if (slots_[i].dataset == nullptr) {
						// Even plain-page allocation failed (genuine memory
						// exhaustion, not just a hugepage shortage). Clear
						// RANDOMX_FLAG_FULL_MEM service-wide -- Service::
						// createMachine()/bindWorkerToSlot() branch on the
						// shared flags_ (not a per-slot value) to decide
						// whether to bind a dataset or a cache, so this slot
						// running cache-only must be reflected globally to
						// avoid handing a null dataset to randomx_vm_set_dataset.
						std::cout << "RANDOMX_FLAG_FULL_MEM was not successful for seed slot " << i << std::endl;
						flags_ = (randomx_flags)(flags_ & ~RANDOMX_FLAG_FULL_MEM);
					}
				}
			}
		}

		~ServicePrivate() {
			for (auto& slot : slots_) {
				if (slot.cache != nullptr) {
					randomx_release_cache(slot.cache);
				}
				if (slot.dataset != nullptr) {
					randomx_release_dataset(slot.dataset);
				}
			}
		}

		std::vector<SeedSlot> slots_;
		httplib::Server<ServiceWorker> server_;
		randomx_flags flags_;
		size_t threads_;
		std::string origin_;
		bool initialized_;
		std::atomic<uint64_t> hashes_;

		// Index of the most-recently activated slot. Used for backward
		// compatibility with requests/clients that don't send a
		// RandomX-Seed header (they get "whatever was primed last", exactly
		// matching the pre-multi-slot behavior) and for the legacy `seed`
		// field of GET /info.
		size_t currentSlot_;

		// Logical clock used to timestamp slot usage for LRU eviction.
		// Guarded by slotsMutex_ together with the mutable fields of each
		// SeedSlot (seedHex, initialized, lastUsed).
		uint64_t lruClock_;
		std::mutex slotsMutex_;
	};

}
