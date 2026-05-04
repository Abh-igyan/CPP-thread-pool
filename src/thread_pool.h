#ifndef THREAD_POOL_H
#define THREAD_POOL_H

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <queue>
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

    ThreadPool(std::size_t thread_count, std::size_t max_queue_size);

    ThreadPool(const ThreadPool&) = delete;
    ThreadPool& operator=(const ThreadPool&) = delete;

    ThreadPool(ThreadPool&&) = delete;
    ThreadPool& operator=(ThreadPool&&) = delete;

    ~ThreadPool();

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
            // enqueue() blocks when the bounded queue is full. This prevents
            // producer bursts from becoming unbounded memory growth.
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

    Stats stats() const;

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

    void worker_loop();
    void shutdown() noexcept;

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

#endif
