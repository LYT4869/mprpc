#include <algorithm>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "mprpcapplication.h"
#include "parallel_file_uploader.h"

namespace
{
double Percentile(std::vector<double> values, double percentile)
{
    if (values.empty()) return 0.0;
    std::sort(values.begin(), values.end());
    const std::size_t index = static_cast<std::size_t>(
        percentile * static_cast<double>(values.size() - 1));
    return values[index];
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

std::filesystem::path FindWorkDirectory(int argc, char** argv)
{
    for (int i = 1; i < argc; ++i) {
        const std::string argument = argv[i];
        if (argument == "-i") {
            ++i;
        } else {
            return argument;
        }
    }
    return std::filesystem::temp_directory_path() / "mprpc_benchmark";
}
} // namespace

int main(int argc, char** argv)
{
    MprpcApplication::Init(argc, argv);
    const auto work_directory = FindWorkDirectory(argc, argv);
    std::error_code error;
    std::filesystem::create_directories(work_directory, error);
    if (error) {
        std::cerr << "failed to create benchmark directory: "
                  << error.message() << '\n';
        return 1;
    }

    constexpr int kRepetitions = 2;
    const std::vector<std::size_t> sizes_mib{1, 8};
    const std::vector<uint32_t> windows{1, 4, 8, 16};
    const std::vector<uint32_t> concurrencies{1, 2};
    std::atomic<uint64_t> sequence{0};

    std::cout << "size_mib,window,concurrency,success,total,"
                 "throughput_mib_s,p50_ms,p95_ms,p99_ms\n";
    for (std::size_t size_mib : sizes_mib) {
        const auto input = work_directory /
            ("input-" + std::to_string(size_mib) + "mib.bin");
        const std::size_t size_bytes = size_mib * 1024U * 1024U;
        if (!CreateInputFile(input, size_bytes)) {
            std::cerr << "failed to create benchmark input\n";
            return 1;
        }

        for (uint32_t window : windows) {
            for (uint32_t concurrency : concurrencies) {
                std::vector<double> latencies_ms;
                std::mutex result_mutex;
                uint32_t successes = 0;
                double total_wall_seconds = 0.0;

                for (int repetition = 0; repetition < kRepetitions;
                     ++repetition) {
                    std::vector<std::thread> threads;
                    const auto wall_start = std::chrono::steady_clock::now();
                    for (uint32_t worker = 0; worker < concurrency; ++worker) {
                        threads.emplace_back([&, worker] {
                            mprpc::file::ParallelFileUploader uploader;
                            mprpc::file::UploadOptions options;
                            options.window_size = window;
                            const auto start = std::chrono::steady_clock::now();
                            const auto result = uploader.Upload(
                                input,
                                "benchmark-" + std::to_string(sequence++) +
                                    ".bin",
                                options);
                            const double latency =
                                std::chrono::duration<double, std::milli>(
                                    std::chrono::steady_clock::now() - start)
                                    .count();
                            std::lock_guard<std::mutex> lock(result_mutex);
                            latencies_ms.push_back(latency);
                            if (result.ok) {
                                ++successes;
                            } else {
                                std::cerr << "benchmark upload failed: "
                                          << result.error_message << '\n';
                            }
                        });
                    }
                    for (auto& thread : threads) thread.join();
                    total_wall_seconds += std::chrono::duration<double>(
                        std::chrono::steady_clock::now() - wall_start).count();
                }

                const uint32_t total = concurrency * kRepetitions;
                const double throughput = total_wall_seconds > 0.0
                    ? static_cast<double>(successes * size_mib) /
                          total_wall_seconds
                    : 0.0;
                std::cout << size_mib << ',' << window << ',' << concurrency
                          << ',' << successes << ',' << total << ','
                          << std::fixed << std::setprecision(2) << throughput
                          << ',' << Percentile(latencies_ms, 0.50)
                          << ',' << Percentile(latencies_ms, 0.95)
                          << ',' << Percentile(latencies_ms, 0.99) << '\n';
            }
        }
    }
    return 0;
}
