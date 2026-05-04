#include "thread_pool.h"

#include <exception>
#include <iostream>
#include <stdexcept>

ThreadPool::ThreadPool(std::size_t thread_count, std::size_t max_queue_size)
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

ThreadPool::~ThreadPool() {
    shutdown();
}

ThreadPool::Stats ThreadPool::stats() const {
    std::lock_guard<std::mutex> lock(queue_mutex_);

    return Stats{
        tasks_submitted_.load(std::memory_order_relaxed),
        tasks_completed_.load(std::memory_order_relaxed),
        task_wrapper_failures_.load(std::memory_order_relaxed),
        peak_queue_size_
    };
}

void ThreadPool::worker_loop() {
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
            // std::packaged_task captures normal user-code exceptions in the
            // associated future, so this catch is defensive: it keeps a malformed
            // wrapper or unexpected std::function failure from terminating the
            // worker thread and silently shrinking the pool.
            task_wrapper_failures_.fetch_add(1, std::memory_order_relaxed);
            std::cerr << "ThreadPool worker caught task wrapper exception: "
                      << ex.what() << '\n';
        } catch (...) {
            task_wrapper_failures_.fetch_add(1, std::memory_order_relaxed);
            std::cerr << "ThreadPool worker caught unknown task wrapper exception\n";
        }
    }
}

void ThreadPool::shutdown() noexcept {
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
