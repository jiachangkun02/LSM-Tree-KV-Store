#pragma once
#include <thread>
#include <mutex>
#include <condition_variable>
#include <queue>
#include <functional>
#include "../util/status.h"

namespace lsmkv {

class CompactionManager {
public:
    enum TaskType { kFlush, kCompact };
    struct Task { TaskType type; std::function<Status()> run; };

    CompactionManager() : stop_(false) { worker_ = std::thread([this]{ this->Run(); }); }
    ~CompactionManager() {
        {
            std::lock_guard<std::mutex> lg(mu_);
            stop_ = true;
            cv_.notify_all();
        }
        if (worker_.joinable()) worker_.join();
    }

    void Schedule(Task&& t) {
        std::lock_guard<std::mutex> lg(mu_);
        q_.push(std::move(t));
        cv_.notify_one();
    }

private:
    void Run() {
        while (true) {
            Task t;
            {
                std::unique_lock<std::mutex> lk(mu_);
                cv_.wait(lk, [&]{ return stop_ || !q_.empty(); });
                if (stop_ && q_.empty()) return;
                t = std::move(q_.front()); q_.pop();
            }
            t.run(); // ignore status for now
        }
    }
    std::mutex mu_;
    std::condition_variable cv_;
    std::queue<Task> q_;
    std::thread worker_;
    bool stop_;
};

} // namespace lsmkv
