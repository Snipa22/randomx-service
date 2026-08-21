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

#include "httplib.h"
#include "service.h"
#include "service_worker.h"
#include "service_private.h"
#include "thread_pool.h"
#include "utility.h"
#include "hex.h"
#include <stdexcept>
#include <locale>
#include <iostream>
#include <array>
#include <sstream>
#include <algorithm>

namespace randomx {

#define SERVICE_ALGORITHM "rx/0"
#define SERVICE_MAX_BATCH_SIZE (256u)
#define HEADER_ACCEPT "Accept"
#define HEADER_CONTENT "Content-Type"
#define HEADER_RANDOMX_SEED "RandomX-Seed"
#define HEADER_ORIGIN "Origin"
#define HEADER_REFERER "Referer"
#define BINARY_FORMAT "application/x.randomx+bin"
#define HEX_FORMAT "application/x.randomx+hex"
#define BINARY_FORMAT_BATCH "application/x.randomx.batch+bin"
#define HEX_FORMAT_BATCH "application/x.randomx.batch+hex"

	using RandomxHash = std::array<char, RANDOMX_HASH_SIZE>;

	bool Service::run(const char* hostname, int port) {
		return data_->server_.listen(hostname, port);
	}

	Service::~Service() {

	}

	randomx_vm* Service::createMachine() const {
		auto* machine = randomx_create_vm(data_->flags_, data_->slots_[0].cache, data_->slots_[0].dataset);
		if (machine == nullptr) {
			throw std::runtime_error("randomx_create_vm failed");
		}
		return machine;
	}

	void Service::destroyMachine(randomx_vm* machine) const {
		randomx_destroy_vm(machine);
	}

	void Service::refreshMachine(randomx_vm* machine) const {
		// NOTE: superseded by bindWorkerToSlot's cheap per-request VM
		// re-pointing (see thread_pool.cpp's ThreadPool::reseed) -- kept
		// only because it's still part of this class's public API surface;
		// no current caller invokes this. Left hardcoded to slot 0 since
		// it has no real caller to reason about beyond that.
		if (data_->flags_ & RANDOMX_FLAG_FULL_MEM) {
			randomx_vm_set_dataset(machine, data_->slots_[0].dataset);
		} else {
			randomx_vm_set_cache(machine, data_->slots_[0].cache);
		}
	}

	void Service::reinitDataset(size_t slot) {
		auto& s = data_->slots_[slot];
		if (data_->flags_ & RANDOMX_FLAG_FULL_MEM) {
			uint32_t datasetItemCount = randomx_dataset_item_count();
			auto threads = data_->threads_;
			if (threads > 1) {
				auto perThread = datasetItemCount / threads;
				auto remainder = datasetItemCount % threads;
				std::vector<std::thread> workers;
				uint32_t startItem = 0;
				for (int i = 0; i < threads; ++i) {
					auto count = perThread + (i == threads - 1 ? remainder : 0);
					workers.push_back(std::thread(&randomx_init_dataset, s.dataset, s.cache, startItem, count));
					startItem += count;
				}
				for (unsigned i = 0; i < workers.size(); ++i) {
					workers[i].join();
				}
			}
			else {
				randomx_init_dataset(s.dataset, s.cache, 0, datasetItemCount);
			}
		}
	}

	void Service::setOrigin(const std::string& origin) {
		data_->origin_ = origin;
	}

	int Service::getFlags() const {
		return data_->flags_;
	}

	bool Service::allowCors(const char* method, const httplib::Request& req, httplib::Response& res) {
		if (req.has_header(HEADER_ORIGIN) && req.get_header_value(HEADER_ORIGIN) == data_->origin_) {
			res.set_header("Access-Control-Allow-Origin", data_->origin_);
			res.set_header("Access-Control-Allow-Methods", method);
			res.set_header("Access-Control-Allow-Headers", HEADER_ACCEPT ", " HEADER_CONTENT ", " HEADER_RANDOMX_SEED);
			res.set_header("Access-Control-Max-Age", "120");
			return true;
		}
		return false;
	}

	int Service::getAutoFlags() {
		return ServicePrivate::AutoFlags;
	}

	int Service::getMachineThreads() {
		return std::thread::hardware_concurrency();
	}

	size_t Service::getDefaultSeedSlots() {
		return ServicePrivate::DefaultSeedSlots;
	}

	bool readRequestBody(const httplib::Request& req, httplib::Response& res, std::vector<char>& body) {
		if (!req.has_header(HEADER_CONTENT)) {
			res.status = 415;
			return false;
		}
		const auto ct = req.get_header_value(HEADER_CONTENT);
		const auto& input = req.body;
		if (ct == HEX_FORMAT) {
			body.resize(input.size() / 2);
			if (!hex2bin(input.data(), input.size(), body.data())) {
				res.status = 400;
				return false;
			}
			return true;
		}
		if (ct == BINARY_FORMAT) {
			body.resize(input.size());
			memcpy(body.data(), input.data(), input.size());
			return true;
		}
		res.status = 415;
		return false;
	}

	bool readRequestBatch(const httplib::Request& req, httplib::Response& res, std::vector<std::vector<char>>& batch) {
		if (!req.has_header(HEADER_CONTENT)) {
			res.status = 415;
			return false;
		}
		const auto ct = req.get_header_value(HEADER_CONTENT);
		const auto& input = req.body;
		size_t pos = 0;
		if (ct == HEX_FORMAT_BATCH) {
			while (pos < input.size()) {
				std::vector<char> body;
				auto space = input.find(' ', pos);
				if (space == std::string::npos) {
					space = input.size();
				}
				auto segmentSize = space - pos;
				body.resize(segmentSize / 2);
				if (!hex2bin(input.data() + pos, segmentSize, body.data())) {
					res.status = 400;
					return false;
				}
				batch.push_back(body);
				pos = space + 1;
			}
		}
		else if (ct == BINARY_FORMAT_BATCH) {
			while (pos < input.size()) {
				std::vector<char> body;
				int segmentSize = input[pos];
				if (segmentSize < 0) {
					res.status = 400;
					return false;
				}
				pos++;
				if (pos + segmentSize > input.size()) {
					res.status = 400;
					return false;
				}
				body.resize(segmentSize);
				memcpy(body.data(), input.data() + pos, segmentSize);
				batch.push_back(body);
				pos += segmentSize;
			}
		}
		else {
			res.status = 415;
			return false;
		}
		if (batch.empty()) {
			res.status = 400;
			return false;
		}
		if (batch.size() > SERVICE_MAX_BATCH_SIZE) {
			res.status = 413;
			return false;
		}
		return true;
	}

	bool Service::checkSeed(const httplib::Request& req) {
		return resolveSlotForRequest(req) >= 0;
	}

	// touchSeed / reinitSlot implement the actual multi-seed-slot logic.
	//
	// Eviction policy: Least Recently Used (LRU) across the N configured
	// slots, tracked via a simple monotonic logical clock (lruClock_) that
	// is bumped every time a slot is matched/activated. An empty
	// (never-yet-initialized) slot is always preferred over evicting an
	// already-primed one, so the first N distinct seeds fill all slots
	// without evicting each other; only the (N+1)th distinct seed causes a
	// real eviction. This is the standard, simplest-to-reason-about policy
	// for a small, fixed number of slots and matches the "2 slots enables a
	// clean old/new handoff, 3 avoids edge-case thrashing" rationale this
	// feature was built for.
	bool Service::touchSeed(const void* seed, size_t seedSize, size_t& victimSlot) {
		std::string seedHex = bin2hex((const char*)seed, seedSize);
		std::lock_guard<std::mutex> lock(data_->slotsMutex_);
		for (size_t i = 0; i < data_->slots_.size(); ++i) {
			auto& slot = data_->slots_[i];
			if (slot.initialized && slot.seedHex == seedHex) {
				// Fast path: seed is already resident in its own slot.
				// No reinit needed -- this is the entire point of the
				// feature: switching between already-primed seeds must not
				// trigger an expensive full reseed.
				slot.lastUsed = ++data_->lruClock_;
				data_->currentSlot_ = i;
				data_->initialized_ = true;
				return false;
			}
		}

		// Not resident anywhere: pick an eviction victim. Prefer an empty
		// slot; otherwise evict whichever initialized slot has the smallest
		// lastUsed value (the LRU one).
		size_t victim = 0;
		bool foundEmpty = false;
		uint64_t oldest = UINT64_MAX;
		for (size_t i = 0; i < data_->slots_.size(); ++i) {
			if (!data_->slots_[i].initialized) {
				victim = i;
				foundEmpty = true;
				break;
			}
			if (data_->slots_[i].lastUsed < oldest) {
				oldest = data_->slots_[i].lastUsed;
				victim = i;
			}
		}
		(void)foundEmpty;
		// Mark the victim invalid immediately so no other request can be
		// routed to (or believe it matches) this slot while the caller
		// performs the actual (expensive, lock-free) reinit work below.
		data_->slots_[victim].initialized = false;
		victimSlot = victim;
		return true;
	}

	void Service::reinitSlot(size_t slot, const void* seed, size_t seedSize) {
		auto& s = data_->slots_[slot];
		randomx_init_cache(s.cache, seed, seedSize);
		reinitDataset(slot);
		std::string seedHex = bin2hex((const char*)seed, seedSize);
		{
			std::lock_guard<std::mutex> lock(data_->slotsMutex_);
			s.seedHex = seedHex;
			s.initialized = true;
			s.lastUsed = ++data_->lruClock_;
			data_->currentSlot_ = slot;
			data_->initialized_ = true;
		}
	}

	int Service::resolveSlotForRequest(const httplib::Request& req) {
		std::lock_guard<std::mutex> lock(data_->slotsMutex_);
		if (req.has_header(HEADER_RANDOMX_SEED)) {
			auto seed = req.get_header_value(HEADER_RANDOMX_SEED);
			for (char& c : seed) {
				c = std::tolower(c);
			}
			for (size_t i = 0; i < data_->slots_.size(); ++i) {
				auto& slot = data_->slots_[i];
				if (slot.initialized && slot.seedHex == seed) {
					slot.lastUsed = ++data_->lruClock_;
					return (int)i;
				}
			}
			return -1;
		}
		// No RandomX-Seed header: fall back to whichever slot was most
		// recently activated, matching the pre-multi-slot behavior where
		// there was only ever one seed to fall back to.
		auto& current = data_->slots_[data_->currentSlot_];
		if (current.initialized) {
			return (int)data_->currentSlot_;
		}
		return -1;
	}

	void Service::bindWorkerToSlot(ServiceWorker& worker, int slot) const {
		if (worker.currentSlot_ == slot) {
			return;
		}
		auto& s = data_->slots_[slot];
		if (data_->flags_ & RANDOMX_FLAG_FULL_MEM) {
			randomx_vm_set_dataset(worker.vm_, s.dataset);
		} else {
			randomx_vm_set_cache(worker.vm_, s.cache);
		}
		worker.currentSlot_ = slot;
	}

	void Service::enableLog() {
		data_->server_.set_logger([](ServiceWorker& w, const httplib::Request& req, const httplib::Response& res) {
			std::cout << "W" << w.id_ << " " << req.remote_addr << " \"" << req.method << " " << req.path << "\"" << " " << res.status << " " << res.body.size() << " ";
			if (req.has_header(HEADER_REFERER)) {
				std::cout << "\"" << req.get_header_value(HEADER_REFERER) << "\"";
			}
			else {
				std::cout << "\"-\"";
			}
			std::cout << std::endl;
		});
	}

	inline void outputContentType(bool outputHex, httplib::Response& res) {
		res.set_header(HEADER_CONTENT, outputHex ? HEX_FORMAT : BINARY_FORMAT);
	}

	template<bool separator>
	void outputSingleHash(bool outputHex, const RandomxHash& hash, httplib::Response& res) {
		if (outputHex) {
			res.body += bin2hex(hash.data(), hash.size());
			if (separator) {
				res.body += ' ';
			}
		}
		else {
			if (separator) {
				res.body += (char)(hash.size() & 0xff);
			}
			res.body += std::string(hash.data(), hash.size());
		}
	}

	bool getOutputFormat(const httplib::Request& req, const char* binary) {
		bool outputHex = true;
		size_t accepts;
		if (accepts = req.get_header_value_count(HEADER_ACCEPT)) {
			for (size_t i = 0; i < accepts; ++i) {
				if (req.get_header_value(HEADER_ACCEPT, i) == binary) {
					outputHex = false;
					break;
				}
			}
		}
		return outputHex;
	}

	void outputBody(const httplib::Request& req, httplib::Response& res, RandomxHash& hash) {
		bool outputHex = getOutputFormat(req, BINARY_FORMAT);
		outputContentType(outputHex, res);
		outputSingleHash<false>(outputHex, hash, res);
	}

	void outputBody(const httplib::Request& req, httplib::Response& res, std::vector<RandomxHash>& batch) {
		bool outputHex = getOutputFormat(req, BINARY_FORMAT_BATCH);
		outputContentType(outputHex, res);
		for (const auto& hash : batch) {
			outputSingleHash<true>(outputHex, hash, res);
		}
	}

	Service::Service(size_t threads, int flags, size_t seedSlots) :
		data_(new ServicePrivate(*this, threads, flags, seedSlots == 0 ? ServicePrivate::DefaultSeedSlots : seedSlots))
	{
		auto options = [&](ServiceWorker& w, const httplib::Request& req, httplib::Response& res) {
			if (!allowCors("POST", req, res)) {
				res.status = 403;
			}
			else {
				res.status = 204;
			}
		};

		data_->server_
			.Get("/info", [&](ServiceWorker& w, const httplib::Request& req, httplib::Response& res) {
				allowCors("GET", req, res);
				std::stringstream info;
				info << "{\n";
				info << "	\"randomx_service\": \"v" RANDOMX_SERVICE_VERSION "\",\n";
				info << "	\"algorithm\": \"" SERVICE_ALGORITHM "\",\n";
				info << "	\"threads\": " << data_->threads_ << ",\n";
				info << "	\"seed\": ";
				// Backward-compatible: "seed" keeps reporting a single hex
				// string (the most-recently-activated slot's seed) or null,
				// exactly like the pre-multi-slot wire format, so existing
				// clients (e.g. go-xmr-lib's RXVerifier.Info()) keep
				// working unchanged.
				if (data_->initialized_) {
					info << "\"" << data_->slots_[data_->currentSlot_].seedHex << "\"";
				}
				else {
					info << "null";
				}
				// New, additive field: every currently-primed seed across
				// all slots, in most-recently-used-first order. New
				// clients can use this to discover multi-seed support;
				// old clients simply ignore the unknown field.
				info << ",\n	\"seeds\": [";
				{
					std::vector<size_t> order;
					for (size_t i = 0; i < data_->slots_.size(); ++i) {
						if (data_->slots_[i].initialized) {
							order.push_back(i);
						}
					}
					std::sort(order.begin(), order.end(), [&](size_t a, size_t b) {
						return data_->slots_[a].lastUsed > data_->slots_[b].lastUsed;
					});
					for (size_t i = 0; i < order.size(); ++i) {
						if (i > 0) {
							info << ", ";
						}
						info << "\"" << data_->slots_[order[i]].seedHex << "\"";
					}
				}
				info << "],\n";
				info << "	\"seed_slots\": " << data_->slots_.size() << ",\n";
				info << "	\"hashes\": " << data_->hashes_.load() << "\n";
				info << "}\n";
				res.set_content(info.str(), "application/json");
			})
			.Post("/seed", [&](ServiceWorker& w, const httplib::Request& req, httplib::Response& res) {
				allowCors("POST", req, res);
				std::vector<char> body;
				if (!readRequestBody(req, res, body)) {
					return;
				}
				if (body.size() > 60) {
					res.status = 413;
					return;
				}
				w.pool_.reseed(w, body.data(), body.size());
				res.status = 204;
			})
			.Post("/hash", [&](ServiceWorker& w, const httplib::Request& req, httplib::Response& res) {
				allowCors("POST", req, res);
				if (!data_->initialized_) {
					res.status = 403;
					return;
				}
				std::vector<char> body;
				if (!readRequestBody(req, res, body)) {
					return;
				}
				int slot = resolveSlotForRequest(req);
				if (slot < 0) {
					res.status = 422;
					return;
				}
				bindWorkerToSlot(w, slot);
				RandomxHash hash;
				randomx_calculate_hash(w.vm_, body.data(), body.size(), hash.data());
				data_->hashes_.fetch_add(1);
				outputBody(req, res, hash);
			})
			.Post("/batch", [&](ServiceWorker& w, const httplib::Request& req, httplib::Response& res) {
				allowCors("POST", req, res);
				if (!data_->initialized_) {
					res.status = 403;
					return;
				}
				std::vector<std::vector<char>> batch;
				if (!readRequestBatch(req, res, batch)) {
					return;
				}
				int slot = resolveSlotForRequest(req);
				if (slot < 0) {
					res.status = 422;
					return;
				}
				bindWorkerToSlot(w, slot);
				std::vector<RandomxHash> hashes;
				hashes.resize(batch.size());
				randomx_calculate_hash_first(w.vm_, batch[0].data(), batch[0].size());
				for (int i = 1; i < batch.size(); ++i) {
					randomx_calculate_hash_next(w.vm_, batch[i].data(), batch[i].size(), hashes[i - 1].data());
				}
				randomx_calculate_hash_last(w.vm_, hashes.back().data());
				data_->hashes_.fetch_add(hashes.size());
				outputBody(req, res, hashes);
			})
			.Options("/seed", options)
			.Options("/hash", options)
			.Options("/batch", options);
	}
}
