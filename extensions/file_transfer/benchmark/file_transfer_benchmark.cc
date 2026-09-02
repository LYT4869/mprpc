#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <memory>
#include <mutex>
#include <limits>
#include <sstream>
#include <string>
#include <sys/resource.h>
#include <thread>
#include <vector>

#include "mprpcapplication.h"
#include "parallel_file_uploader.h"

namespace
{
struct BenchmarkConfig
{
    std::size_t size_mib = 8;
    uint32_t window = 8;
    uint32_t concurrency = 1;
    uint32_t samples = 10;
    uint32_t warmup = 3;
    bool cold_connections = false;
    bool percentiles_valid = false;
    bool allow_failures = false;
};

struct CommandLine
{
    std::string profile = "smoke";
    std::filesystem::path work_directory =
        std::filesystem::temp_directory_path() / "mprpc_benchmark";
    std::filesystem::path output_path;
    bool cold_connections = false;
    bool valid = true;
    bool size_set = false;
    bool window_set = false;
    bool concurrency_set = false;
    bool samples_set = false;
    bool warmup_set = false;
    std::size_t size_mib = 8;
    uint32_t window = 8;
    uint32_t concurrency = 1;
    uint32_t samples = 10;
    uint32_t warmup = 3;
};

struct RunAggregate
{
    std::mutex mutex;
    std::vector<double> latencies_ms;
    uint32_t successes = 0;
    uint32_t failures = 0;
    uint64_t bytes_uploaded = 0;
    uint64_t retries = 0;
    uint64_t retry_exhausted = 0;
    uint64_t chunk_attempts = 0;
    uint64_t duplicate_chunks = 0;
    uint64_t queue_rejected = 0;
    uint32_t max_in_flight = 0;
    RpcMetricValues rpc;
};

std::atomic<uint64_t> g_sequence{0};

bool ParseUnsigned(const std::string& text, uint64_t* value)
{
    try {
        std::size_t consumed = 0;
        const unsigned long long parsed = std::stoull(text, &consumed);
        if (consumed != text.size()) return false;
        *value = static_cast<uint64_t>(parsed);
        return true;
    } catch (...) {
        return false;
    }
}

CommandLine ParseCommandLine(int argc, char** argv)
{
    CommandLine command;
    bool work_directory_set = false;
    for (int i = 1; i < argc; ++i) {
        const std::string argument = argv[i];
        if (argument == "-i" && i + 1 < argc) {
            ++i;
        } else if (argument == "--profile" && i + 1 < argc) {
            command.profile = argv[++i];
        } else if (argument == "--output" && i + 1 < argc) {
            command.output_path = argv[++i];
        } else if (argument == "--connection-mode" && i + 1 < argc) {
            const std::string mode = argv[++i];
            command.valid = command.valid &&
                (mode == "warm" || mode == "cold");
            command.cold_connections = mode == "cold";
        } else if ((argument == "--size-mib" ||
                    argument == "--window" ||
                    argument == "--concurrency" ||
                    argument == "--samples" ||
                    argument == "--warmup") && i + 1 < argc) {
            uint64_t parsed = 0;
            command.valid = command.valid &&
                ParseUnsigned(argv[++i], &parsed);
            if (argument == "--size-mib") {
                command.size_set = true;
                command.size_mib = static_cast<std::size_t>(parsed);
            } else if (argument == "--window") {
                command.window_set = true;
                command.valid = command.valid &&
                    parsed <= std::numeric_limits<uint32_t>::max();
                if (command.valid) {
                    command.window = static_cast<uint32_t>(parsed);
                }
            } else if (argument == "--concurrency") {
                command.concurrency_set = true;
                command.valid = command.valid &&
                    parsed <= std::numeric_limits<uint32_t>::max();
                if (command.valid) {
                    command.concurrency = static_cast<uint32_t>(parsed);
                }
            } else if (argument == "--samples") {
                command.samples_set = true;
                command.valid = command.valid &&
                    parsed <= std::numeric_limits<uint32_t>::max();
                if (command.valid) {
                    command.samples = static_cast<uint32_t>(parsed);
                }
            } else {
                command.warmup_set = true;
                command.valid = command.valid &&
                    parsed <= std::numeric_limits<uint32_t>::max();
                if (command.valid) {
                    command.warmup = static_cast<uint32_t>(parsed);
                }
            }
        } else if (argument.rfind("--", 0) == 0) {
            command.valid = false;
        } else if (!work_directory_set) {
            command.work_directory = argument;
            work_directory_set = true;
        } else {
            command.valid = false;
        }
    }
    command.valid = command.valid && command.size_mib > 0 &&
        command.window > 0 && command.concurrency > 0 &&
        command.samples > 0;
    return command;
}

std::vector<BenchmarkConfig> BuildProfile(const CommandLine& command)
{
    std::vector<BenchmarkConfig> configs;
    const auto append = [&configs, &command](
        std::size_t size, uint32_t window, uint32_t concurrency,
        uint32_t samples, uint32_t warmup, bool percentiles_valid,
        bool allow_failures = false) {
        configs.push_back(BenchmarkConfig{
            command.size_set ? command.size_mib : size,
            command.window_set ? command.window : window,
            command.concurrency_set ? command.concurrency : concurrency,
            command.samples_set ? command.samples : samples,
            command.warmup_set ? command.warmup : warmup,
            command.cold_connections,
            command.samples_set ? command.samples >= 100
                                : percentiles_valid,
            allow_failures});
    };

    if (command.profile == "smoke") {
        for (std::size_t size : {1U, 8U}) {
            for (uint32_t window : {1U, 8U}) {
                append(size, window, 1, 2, 0, false);
                append(size, window, 2, 4, 0, false);
            }
        }
    } else if (command.profile == "latency") {
        for (uint32_t concurrency : {1U, 4U, 8U}) {
            append(1, 8, concurrency, 100, 3, true);
        }
    } else if (command.profile == "window") {
        for (uint32_t window : {1U, 2U, 4U, 8U, 16U}) {
            append(16, window, 1, 10, 3, false);
        }
    } else if (command.profile == "saturation") {
        for (uint32_t concurrency : {1U, 2U, 4U, 8U}) {
            append(64, 8, concurrency, 16, 3, false, true);
        }
    } else if (command.profile == "custom") {
        append(8, 8, 1, 10, 3, command.samples >= 100);
    }
    return configs;
}

bool CreateInputFile(const std::filesystem::path& path,
                     std::size_t size_bytes)
{
    std::ofstream output(path, std::ios::binary | std::ios::trunc);
    std::string block(64U * 1024U, '\0');
    for (std::size_t i = 0; i < block.size(); ++i) {
        block[i] = static_cast<char>((i * 31U + 17U) & 0xFFU);
    }
    while (size_bytes > 0 && output) {
        const std::size_t count = std::min(size_bytes, block.size());
        output.write(block.data(), static_cast<std::streamsize>(count));
        size_bytes -= count;
    }
    return static_cast<bool>(output);
}

double Percentile(std::vector<double> values, double percentile)
{
    if (values.empty()) return 0.0;
    std::sort(values.begin(), values.end());
    const std::size_t rank = static_cast<std::size_t>(
        std::ceil(percentile * static_cast<double>(values.size())));
    return values[std::max<std::size_t>(1, rank) - 1];
}

void AddRpcValues(RpcMetricValues* target,
                  const RpcMetricValues& values)
{
#define ADD_FIELD(name) target->name += values.name
    ADD_FIELD(started);
    ADD_FIELD(active);
    ADD_FIELD(success);
    ADD_FIELD(timeout);
    ADD_FIELD(cancelled);
    ADD_FIELD(network_error);
    ADD_FIELD(channel_closed);
    ADD_FIELD(framework_error);
    ADD_FIELD(callback_rejected);
#undef ADD_FIELD
}

RpcMetricValues SubtractRpcValues(const RpcMetricValues& after,
                                  const RpcMetricValues& before)
{
    RpcMetricValues result;
#define SUB_FIELD(name) result.name = after.name - before.name
    SUB_FIELD(started);
    SUB_FIELD(active);
    SUB_FIELD(success);
    SUB_FIELD(timeout);
    SUB_FIELD(cancelled);
    SUB_FIELD(network_error);
    SUB_FIELD(channel_closed);
    SUB_FIELD(framework_error);
    SUB_FIELD(callback_rejected);
#undef SUB_FIELD
    return result;
}

double CpuSeconds()
{
    rusage usage{};
    if (::getrusage(RUSAGE_SELF, &usage) != 0) return 0.0;
    return usage.ru_utime.tv_sec + usage.ru_utime.tv_usec / 1000000.0 +
           usage.ru_stime.tv_sec + usage.ru_stime.tv_usec / 1000000.0;
}

uint64_t CurrentRssKiB()
{
    std::ifstream status("/proc/self/status");
    std::string key;
    while (status >> key) {
        if (key == "VmRSS:") {
            uint64_t value = 0;
            status >> value;
            return value;
        }
        std::string rest;
        std::getline(status, rest);
    }
    return 0;
}

uint64_t PeakRssKiB()
{
    rusage usage{};
    return ::getrusage(RUSAGE_SELF, &usage) == 0
        ? static_cast<uint64_t>(usage.ru_maxrss) : 0;
}

void RecordResult(RunAggregate* aggregate,
                  const mprpc::file::UploadFileResult& result,
                  double latency_ms)
{
    std::lock_guard<std::mutex> lock(aggregate->mutex);
    aggregate->latencies_ms.push_back(latency_ms);
    if (result.ok) {
        ++aggregate->successes;
    } else {
        ++aggregate->failures;
        std::cerr << "benchmark upload failed: "
                  << result.error_message << '\n';
    }
    aggregate->bytes_uploaded += result.bytes_uploaded;
    aggregate->retries += result.retries;
    aggregate->retry_exhausted += result.retry_exhausted;
    aggregate->chunk_attempts += result.chunk_attempts;
    aggregate->duplicate_chunks += result.duplicate_chunks;
    aggregate->queue_rejected += result.queue_rejected;
    aggregate->max_in_flight = std::max(
        aggregate->max_in_flight, result.max_in_flight);
}

mprpc::file::UploadFileResult UploadOnce(
    mprpc::file::ParallelFileUploader* uploader,
    const std::filesystem::path& input,
    const BenchmarkConfig& config)
{
    mprpc::file::UploadOptions options;
    options.window_size = config.window;
    return uploader->Upload(
        input, "benchmark-" + std::to_string(g_sequence++) + ".bin",
        options);
}

bool RunWarmup(const BenchmarkConfig& config,
               const std::filesystem::path& input,
               std::vector<std::unique_ptr<
                   mprpc::file::ParallelFileUploader>>* uploaders)
{
    // Warm each persistent connection without turning warmup into a load test.
    for (uint32_t worker = 0; worker < config.concurrency; ++worker) {
        for (uint32_t attempt = 0;
             attempt < config.warmup;
             ++attempt) {
            std::unique_ptr<mprpc::file::ParallelFileUploader> cold;
            auto* uploader = (*uploaders)[worker].get();
            if (config.cold_connections) {
                cold = std::make_unique<
                    mprpc::file::ParallelFileUploader>();
                uploader = cold.get();
            }
            const auto result = UploadOnce(uploader, input, config);
            if (!result.ok) {
                std::cerr << "benchmark warmup failed for uploader "
                          << worker << ", attempt " << attempt + 1
                          << ": " << result.error_message << '\n';
                return false;
            }
        }
    }
    std::cerr << "benchmark warmup complete: uploaders="
              << config.concurrency
              << " uploads_per_uploader=" << config.warmup
              << " total=" << config.concurrency * config.warmup
              << '\n';
    return true;
}

void RunPhase(const BenchmarkConfig& config,
              const std::filesystem::path& input,
              uint32_t count,
              std::vector<std::unique_ptr<
                  mprpc::file::ParallelFileUploader>>* uploaders,
              RunAggregate* aggregate)
{
    std::atomic<uint32_t> next{0};
    std::vector<std::thread> workers;
    for (uint32_t worker = 0; worker < config.concurrency; ++worker) {
        workers.emplace_back([&, worker] {
            while (true) {
                const uint32_t index = next.fetch_add(1);
                if (index >= count) break;
                std::unique_ptr<mprpc::file::ParallelFileUploader> cold;
                auto* uploader = (*uploaders)[worker].get();
                if (config.cold_connections) {
                    cold = std::make_unique<
                        mprpc::file::ParallelFileUploader>();
                    uploader = cold.get();
                }
                const auto start = std::chrono::steady_clock::now();
                const auto result = UploadOnce(uploader, input, config);
                const double latency =
                    std::chrono::duration<double, std::milli>(
                        std::chrono::steady_clock::now() - start).count();
                RecordResult(aggregate, result, latency);
                if (cold) {
                    std::lock_guard<std::mutex> lock(aggregate->mutex);
                    AddRpcValues(
                        &aggregate->rpc,
                        cold->GetRpcMetricsSnapshot().total);
                }
            }
        });
    }
    for (auto& worker : workers) worker.join();
}

bool RunConfiguration(const BenchmarkConfig& config,
                      const std::filesystem::path& input,
                      std::string* result_row)
{
    std::vector<std::unique_ptr<mprpc::file::ParallelFileUploader>>
        uploaders;
    uploaders.reserve(config.concurrency);
    for (uint32_t worker = 0; worker < config.concurrency; ++worker) {
        uploaders.push_back(std::make_unique<
            mprpc::file::ParallelFileUploader>());
    }

    if (!RunWarmup(config, input, &uploaders)) {
        return false;
    }
    std::vector<RpcMetricValues> baselines;
    for (const auto& uploader : uploaders) {
        baselines.push_back(uploader->GetRpcMetricsSnapshot().total);
    }

    RunAggregate aggregate;
    const double cpu_start = CpuSeconds();
    const auto wall_start = std::chrono::steady_clock::now();
    RunPhase(config, input, config.samples, &uploaders, &aggregate);
    const double wall_seconds = std::chrono::duration<double>(
        std::chrono::steady_clock::now() - wall_start).count();
    const double cpu_seconds = CpuSeconds() - cpu_start;

    if (!config.cold_connections) {
        for (std::size_t i = 0; i < uploaders.size(); ++i) {
            AddRpcValues(
                &aggregate.rpc,
                SubtractRpcValues(
                    uploaders[i]->GetRpcMetricsSnapshot().total,
                    baselines[i]));
        }
    }

    const double successful_goodput = wall_seconds > 0.0
        ? static_cast<double>(aggregate.successes * config.size_mib) /
              wall_seconds
        : 0.0;
    std::ostringstream row;
    row << config.size_mib << ',' << config.window << ','
        << config.concurrency << ',' << config.samples << ','
        << config.warmup << ','
        << (config.cold_connections ? "cold" : "warm") << ','
        << aggregate.successes << ',' << aggregate.failures << ','
        << std::fixed << std::setprecision(2) << successful_goodput << ','
        << Percentile(aggregate.latencies_ms, 0.50) << ','
        << Percentile(aggregate.latencies_ms, 0.95) << ','
        << Percentile(aggregate.latencies_ms, 0.99) << ','
        << (config.percentiles_valid ? 1 : 0) << ','
        << aggregate.retries << ',' << aggregate.retry_exhausted << ','
        << aggregate.chunk_attempts << ',' << aggregate.duplicate_chunks
        << ',' << aggregate.queue_rejected
        << ',' << aggregate.max_in_flight
        << ',' << aggregate.rpc.started << ',' << aggregate.rpc.timeout
        << ',' << aggregate.rpc.network_error << ','
        << aggregate.rpc.cancelled << ','
        << aggregate.rpc.callback_rejected << ','
        << aggregate.rpc.active << ',' << cpu_seconds << ','
        << CurrentRssKiB() << ',' << PeakRssKiB();
    *result_row = row.str();
    return true;
}
} // namespace

int main(int argc, char** argv)
{
    const CommandLine command = ParseCommandLine(argc, argv);
    const auto configs = BuildProfile(command);
    if (!command.valid || configs.empty()) {
        std::cerr << "usage: file_transfer_benchmark -i <config> "
                     "[work-dir] [--profile smoke|latency|window|"
                     "saturation|custom] [--size-mib N] [--window N] "
                     "[--concurrency N] [--samples N] [--warmup N] "
                     "[--connection-mode warm|cold] [--output CSV]\n";
        return 1;
    }

    MprpcApplication::Init(argc, argv);
    std::error_code error;
    std::filesystem::create_directories(command.work_directory, error);
    if (error) {
        std::cerr << "failed to create benchmark directory: "
                  << error.message() << '\n';
        return 1;
    }

    std::ofstream output_file;
    if (!command.output_path.empty()) {
        output_file.open(command.output_path,
                         std::ios::out | std::ios::trunc);
        if (!output_file) {
            std::cerr << "failed to open benchmark CSV\n";
            return 1;
        }
    }
    std::ostream& output = output_file ? output_file : std::cout;
    output << "size_mib,window,concurrency,samples,warmup_per_uploader,"
              "connection_mode,"
              "success,failed,successful_goodput_mib_s,p50_ms,p95_ms,p99_ms,"
              "percentiles_valid,retries,retry_exhausted,chunk_attempts,"
              "duplicate_chunks,queue_rejected,max_in_flight,rpc_started,"
              "rpc_timeout,"
              "rpc_network_error,"
              "rpc_cancelled,callback_rejected,rpc_active_end,cpu_seconds,"
              "rss_kib,peak_rss_kib\n";

    std::size_t current_size = 0;
    std::filesystem::path input;
    bool all_succeeded = true;
    for (const auto& config : configs) {
        if (config.size_mib != current_size) {
            current_size = config.size_mib;
            input = command.work_directory /
                ("input-" + std::to_string(current_size) + "mib.bin");
            if (!CreateInputFile(
                    input, current_size * 1024U * 1024U)) {
                std::cerr << "failed to create benchmark input\n";
                return 1;
            }
        }
        std::string row;
        if (!RunConfiguration(config, input, &row)) {
            std::cerr << "benchmark configuration aborted during warmup\n";
            return 2;
        }
        output << row << '\n';
        output.flush();
        std::istringstream parser(row);
        std::string column;
        for (int i = 0; i <= 7; ++i) std::getline(parser, column, ',');
        all_succeeded = all_succeeded &&
            (config.allow_failures || column == "0");
    }

    std::filesystem::remove(input, error);
    return all_succeeded ? 0 : 2;
}
