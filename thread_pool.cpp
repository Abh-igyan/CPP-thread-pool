#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <functional>
#include <future>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <queue>
#include <stdexcept>
#include <string>
#include <thread>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

class ThreadPool {
public:
    struct Stats {
        std::uint64_t tasks_submitted;
        std::uint64_t tasks_completed;
        std::uint64_t task_wrapper_failures;
        std::size_t peak_queue_size;
    };

    ThreadPool(std::size_t thread_count, std::size_t max_queue_size)
        : max_queue_size_(max_queue_size) {
        if (thread_count == 0) {
            throw std::invalid_argument("ThreadPool requires at least one worker thread");
        }

        if (max_queue_size == 0) {
            throw std::invalid_argument("ThreadPool requires max_queue_size > 0");
        }

        workers_.reserve(thread_count);

        try {
            for (std::size_t i = 0; i < thread_count; ++i) {
                workers_.emplace_back([this] {
                    worker_loop();
                });
            }
        } catch (...) {
            shutdown();
            throw;
        }
    }

    ThreadPool(const ThreadPool&) = delete;
    ThreadPool& operator=(const ThreadPool&) = delete;

    ThreadPool(ThreadPool&&) = delete;
    ThreadPool& operator=(ThreadPool&&) = delete;

    ~ThreadPool() {
        shutdown();
    }

    template <typename F, typename... Args>
    auto enqueue(F&& f, Args&&... args)
        -> std::future<std::invoke_result_t<F, Args...>> {
        using ReturnType = std::invoke_result_t<F, Args...>;

        auto bound_task = make_bound_task(
            std::forward<F>(f),
            std::forward<Args>(args)...
        );

        auto task = std::make_shared<std::packaged_task<ReturnType()>>(
            std::move(bound_task)
        );

        std::future<ReturnType> result = task->get_future();

        {
            std::unique_lock<std::mutex> lock(queue_mutex_);

            // Backpressure policy:
            // enqueue() blocks when the bounded queue is full. This is usually
            // preferable in systems where dropping work is not acceptable. It
            // also prevents producer bursts from becoming unbounded memory growth.
            space_available_.wait(lock, [this] {
                return stopping_ || tasks_.size() < max_queue_size_;
            });

            if (stopping_) {
                throw std::runtime_error("enqueue called on stopped ThreadPool");
            }

            // User exceptions thrown by the callable are captured by
            // std::packaged_task and rethrown through future::get().
            tasks_.emplace([task] {
                (*task)();
            });

            ++tasks_submitted_;

            if (tasks_.size() > peak_queue_size_) {
                peak_queue_size_ = tasks_.size();
            }
        }

        // Wake one worker. Waking all workers for one task increases unnecessary
        // context switches and cache churn.
        task_available_.notify_one();
        return result;
    }

    Stats stats() const {
        std::lock_guard<std::mutex> lock(queue_mutex_);

        return Stats{
            tasks_submitted_.load(std::memory_order_relaxed),
            tasks_completed_.load(std::memory_order_relaxed),
            task_wrapper_failures_.load(std::memory_order_relaxed),
            peak_queue_size_
        };
    }

private:
    template <typename F, typename... Args>
    static auto make_bound_task(F&& f, Args&&... args) {
        using ReturnType = std::invoke_result_t<F, Args...>;
        using Fn = std::decay_t<F>;
        using Tuple = std::tuple<std::decay_t<Args>...>;

        return [func = Fn(std::forward<F>(f)),
                args_tuple = Tuple(std::forward<Args>(args)...)]() mutable
            -> ReturnType {
            return std::apply(
                [&func](auto&&... unpacked_args) -> ReturnType {
                    return std::invoke(
                        std::move(func),
                        std::forward<decltype(unpacked_args)>(unpacked_args)...
                    );
                },
                std::move(args_tuple)
            );
        };
    }

    void worker_loop() {
        while (true) {
            std::function<void()> task;

            {
                std::unique_lock<std::mutex> lock(queue_mutex_);

                // condition_variable avoids busy waiting. Idle workers sleep
                // instead of repeatedly polling the queue and burning CPU.
                //
                // The predicate is required for spurious wakeups and races
                // between notification and worker wakeup.
                task_available_.wait(lock, [this] {
                    return stopping_ || !tasks_.empty();
                });

                if (stopping_ && tasks_.empty()) {
                    return;
                }

                // Contention point:
                // Producers and consumers briefly serialize here to mutate the
                // shared FIFO queue. Keep this critical section small; run user
                // code after releasing the mutex.
                task = std::move(tasks_.front());
                tasks_.pop();
            }

            // A producer may be blocked because the bounded queue was full.
            space_available_.notify_one();

            try {
                task();
                tasks_completed_.fetch_add(1, std::memory_order_relaxed);
            } catch (const std::exception& ex) {
                // std::packaged_task captures normal user-code exceptions in
                // the associated future, so this catch is defensive: it keeps a
                // malformed wrapper or unexpected std::function failure from
                // terminating the worker thread and silently shrinking the pool.
                task_wrapper_failures_.fetch_add(1, std::memory_order_relaxed);
                std::cerr << "ThreadPool worker caught task wrapper exception: "
                          << ex.what() << '\n';
            } catch (...) {
                task_wrapper_failures_.fetch_add(1, std::memory_order_relaxed);
                std::cerr << "ThreadPool worker caught unknown task wrapper exception\n";
            }
        }
    }

    void shutdown() noexcept {
        {
            std::lock_guard<std::mutex> lock(queue_mutex_);

            if (stopping_) {
                return;
            }

            stopping_ = true;
        }

        task_available_.notify_all();
        space_available_.notify_all();

        for (std::thread& worker : workers_) {
            if (worker.joinable()) {
                worker.join();
            }
        }
    }

private:
    std::vector<std::thread> workers_;
    std::queue<std::function<void()>> tasks_;

    const std::size_t max_queue_size_;

    mutable std::mutex queue_mutex_;
    std::condition_variable task_available_;
    std::condition_variable space_available_;

    bool stopping_ = false;

    std::atomic<std::uint64_t> tasks_submitted_{0};
    std::atomic<std::uint64_t> tasks_completed_{0};
    std::atomic<std::uint64_t> task_wrapper_failures_{0};

    // Protected by queue_mutex_ because it is updated while observing tasks_.size().
    std::size_t peak_queue_size_ = 0;
};

static std::uint64_t cpu_bound_work(std::uint64_t iterations) {
    std::uint64_t value = 0;

    for (std::uint64_t i = 0; i < iterations; ++i) {
        value += (i * 2654435761ULL) ^ (value >> 3);
        value ^= (value << 7);
    }

    return value;
}

struct Workload {
    std::string name;
    std::uint64_t iterations_per_task;
    std::size_t task_count;
};

struct BenchmarkResult {
    std::string workload_name;
    std::size_t thread_count;
    std::size_t task_count;
    std::uint64_t iterations_per_task;
    double serial_ms;
    double parallel_ms;
    double speedup;
    double throughput_tasks_per_sec;
    ThreadPool::Stats stats;
};

template <typename Fn>
static double measure_ms(Fn&& fn) {
    const auto start = std::chrono::steady_clock::now();
    fn();
    const auto end = std::chrono::steady_clock::now();

    return std::chrono::duration<double, std::milli>(end - start).count();
}

static std::uint64_t run_serial(const Workload& workload) {
    std::uint64_t checksum = 0;

    for (std::size_t i = 0; i < workload.task_count; ++i) {
        checksum += cpu_bound_work(workload.iterations_per_task);
    }

    return checksum;
}

static BenchmarkResult run_parallel(
    const Workload& workload,
    std::size_t thread_count,
    double serial_ms,
    std::uint64_t serial_checksum
) {
    // Bounded queue size is a latency/throughput tradeoff. Larger queues absorb
    // bursts, but also allow more waiting work to accumulate and may hide
    // downstream saturation.
    const std::size_t max_queue_size = std::max<std::size_t>(thread_count * 4, 16);

    ThreadPool pool(thread_count, max_queue_size);
    std::vector<std::future<std::uint64_t>> futures;
    futures.reserve(workload.task_count);

    std::uint64_t parallel_checksum = 0;

    const double parallel_ms = measure_ms([&] {
        for (std::size_t i = 0; i < workload.task_count; ++i) {
            futures.emplace_back(
                pool.enqueue(cpu_bound_work, workload.iterations_per_task)
            );
        }

        for (auto& future : futures) {
            parallel_checksum += future.get();
        }
    });

    if (parallel_checksum != serial_checksum) {
        throw std::runtime_error("checksum mismatch between serial and parallel runs");
    }

    const double seconds = parallel_ms / 1000.0;

    return BenchmarkResult{
        workload.name,
        thread_count,
        workload.task_count,
        workload.iterations_per_task,
        serial_ms,
        parallel_ms,
        serial_ms / parallel_ms,
        workload.task_count / seconds,
        pool.stats()
    };
}

static void print_header() {
    std::cout
        << std::left
        << std::setw(10) << "workload"
        << std::right
        << std::setw(8) << "threads"
        << std::setw(10) << "tasks"
        << std::setw(14) << "iters/task"
        << std::setw(14) << "serial_ms"
        << std::setw(14) << "pool_ms"
        << std::setw(10) << "speedup"
        << std::setw(16) << "tasks/sec"
        << std::setw(12) << "submitted"
        << std::setw(12) << "completed"
        << std::setw(12) << "failures"
        << std::setw(12) << "peak_q"
        << '\n';

    std::cout << std::string(146, '-') << '\n';
}

static void print_result(const BenchmarkResult& result) {
    std::cout
        << std::left
        << std::setw(10) << result.workload_name
        << std::right
        << std::setw(8) << result.thread_count
        << std::setw(10) << result.task_count
        << std::setw(14) << result.iterations_per_task
        << std::setw(14) << std::fixed << std::setprecision(2) << result.serial_ms
        << std::setw(14) << std::fixed << std::setprecision(2) << result.parallel_ms
        << std::setw(10) << std::fixed << std::setprecision(2) << result.speedup
        << std::setw(16) << std::fixed << std::setprecision(2)
        << result.throughput_tasks_per_sec
        << std::setw(12) << result.stats.tasks_submitted
        << std::setw(12) << result.stats.tasks_completed
        << std::setw(12) << result.stats.task_wrapper_failures
        << std::setw(12) << result.stats.peak_queue_size
        << '\n';
}

int main() {
    const std::size_t hardware_threads =
        std::max<std::size_t>(1, std::thread::hardware_concurrency());

    const std::vector<Workload> workloads{
        // Small tasks emphasize scheduling overhead, queue contention, wakeups,
        // and context-switch cost. They often scale poorly because overhead can
        // dominate useful work.
        {"small", 20'000, 2'000},

        // Medium tasks usually show healthier scaling because useful CPU work
        // amortizes enqueue/dequeue and synchronization costs.
        {"medium", 200'000, 512},

        // Large tasks tend to scale best until CPU cores saturate. Beyond that,
        // adding threads can hurt due to context switching and cache pressure.
        {"large", 1'000'000, 128}
    };

    std::cout << "hardware_concurrency: " << hardware_threads << "\n\n";

    print_header();

    for (const Workload& workload : workloads) {
        std::uint64_t serial_checksum = 0;

        const double serial_ms = measure_ms([&] {
            serial_checksum = run_serial(workload);
        });

        for (std::size_t threads = 1; threads <= hardware_threads; ++threads) {
            const BenchmarkResult result =
                run_parallel(workload, threads, serial_ms, serial_checksum);

            print_result(result);
        }

        std::cout << '\n';
    }

    std::cout
        << "Scaling notes:\n"
        << "- The mutex-protected queue is simple, portable, and reliable, but it is a shared contention point.\n"
        << "- condition_variable lets workers sleep instead of busy-spinning, reducing CPU waste under light load.\n"
        << "- Very small tasks may scale poorly because enqueue/dequeue, wakeups, and context switches dominate work.\n"
        << "- Larger tasks usually scale better because synchronization overhead is amortized over more computation.\n"
        << "- Lock-free queues can reduce blocking in some producer/consumer patterns, but correctness, memory reclamation,\n"
        << "  fairness, and shutdown semantics are substantially harder than with a mutex-based queue.\n"
        << "- Work stealing, per-worker queues, batching, task priorities, and NUMA-aware placement are common next steps\n"
        << "  for production thread pools with highly variable workloads.\n";

    return 0;
}
