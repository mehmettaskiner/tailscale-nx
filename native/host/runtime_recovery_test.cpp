// Real runtime/engine and local sockets. OS-boundary faults stay in this TU.
#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>
#include <algorithm>
#include <atomic>
#include <cassert>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <cstdarg>
#include <ctime>
#include <vector>
#include "runtime.hpp"
#include "tsnx_app.h"
#include "vsock.hpp"

static std::string mode;
static bool inject_fault = true;
static int damaged_rx = -1;
static std::atomic<unsigned> socket_attempts{0}, closes{0};
static std::atomic<bool> fail_open{false}, fail_send{false};
static int fault_socket(int domain, int type, int protocol) {
    ++socket_attempts;
    if (fail_open) { errno = ENFILE; return -1; }
    return socket(domain, type, protocol);
}
static int fault_close(int fd) {
    if (fd >= 0) ++closes;
    if (fd == damaged_rx) damaged_rx = -1;
    return close(fd);
}
static int fault_fcntl(int fd, int cmd, ...) {
    va_list args;
    va_start(args, cmd);
    int value = va_arg(args, int);
    va_end(args);
    if (mode == "nonblocking" && cmd == F_SETFL) { errno = EPERM; return -1; }
    return fcntl(fd, cmd, value);
}
static ssize_t fault_send(int fd, const void *buf, size_t len, int flags) {
    if (fail_send.exchange(false)) { errno = EIO; return -1; }
    return send(fd, buf, len, flags);
}
static ssize_t fault_recv(int fd, void *buf, size_t len, int flags) {
    if (mode == "receive" && inject_fault) {
        inject_fault = false;
        errno = EIO;
        return -1;
    }
    return recv(fd, buf, len, flags);
}
static int fault_poll(struct pollfd *fds, nfds_t count, int timeout) {
    if (mode == "poll") { errno = EIO; return -1; }
    if (inject_fault && count && mode != "normal" && mode != "send" && mode != "receive") {
        inject_fault = false;
        damaged_rx = fds[count - 1].fd;
        if (mode == "invalid") {
            close(damaged_rx); // Real POLLNVAL.
        } else if (mode != "error") {
            // A pipe without a writer gives a real persistent POLLHUP.
            int p[2];
            assert(pipe(p) == 0);
            assert(dup2(p[0], damaged_rx) >= 0);
            close(p[0]);
            close(p[1]);
        }
    }
    if (mode == "error" && damaged_rx >= 0) {
        for (nfds_t i = 0; i < count; ++i) fds[i].revents = fds[i].fd == damaged_rx ? POLLERR : 0;
        return 1;
    }
    return poll(fds, count, timeout);
}
#define fcntl fault_fcntl
#define socket fault_socket
#define close fault_close
#define send fault_send
#define recv fault_recv
#define poll fault_poll
#include "../runtime/runtime.cpp"
#undef fcntl
#undef socket
#undef close
#undef send
#undef recv
#undef poll

using namespace std::chrono_literals;
template<class F> static bool Eventually(F predicate, std::chrono::milliseconds budget = 2500ms) {
    const auto deadline = std::chrono::steady_clock::now() + budget;
    while (!predicate()) {
        if (std::chrono::steady_clock::now() >= deadline) return false;
        std::this_thread::sleep_for(5ms);
    }
    return true;
}

int main(int argc, char **argv) {
    assert(argc == 2);
    mode = argv[1];
    uint8_t seed[32] = {1}, machine[32] = {2}, node[32] = {3}, disco[32] = {4};
    tsnx_seed_rng(seed);
    TsnxConfig cfg{};
    cfg.control_url = "https://127.0.0.1:9";
    cfg.hostname = "runtime-recovery-test";
    cfg.machine_key = machine;
    cfg.node_key = node;
    cfg.disco_key = disco;
    auto *engine = tsnx_engine_new(&cfg, tsnx_driver_now_ns(), time(nullptr));
    assert(engine);
    auto *driver = tsnx_driver_new(engine, nullptr, nullptr);
    assert(driver);
    std::string error;
    auto rt = tsnx::Runtime::Adopt(engine, driver, &error);
    if (mode == "nonblocking") {
        assert(!rt && !error.empty());
        assert(socket_attempts == 2 && closes == 2);
        tsnx_driver_free(driver);
        tsnx_engine_free(engine);
        std::puts("PASS runtime nonblocking");
        return 0;
    }
    assert(rt);
    std::atomic<unsigned> ticks{0}, retired{0}, reopened{0}, failed{0}, poll_errors{0};
    rt->SetDiagnosticHook([&](const char *message, int) {
        if (strstr(message, "poll failed")) ++poll_errors;
        if (strstr(message, "retiring")) ++retired;
        if (strstr(message, "reopened")) ++reopened;
        if (strstr(message, "reopen failed")) ++failed;
    });
    if (mode == "retry" || mode == "suspend") fail_open = true;
    std::thread loop([&] { rt->Run([&](TsnxDriver *) { ++ticks; }); });
    assert(Eventually([&] { return ticks.load() > 0; }));
    if (mode == "send") fail_send = true;
    if (mode == "normal" || mode == "send" || mode == "receive") {
        // An overlay call wakes the actual engine poll, including after recovery.
        const auto before = ticks.load();
        rt->With([](TsnxEngine *) {});
        assert(Eventually([&] { return ticks.load() > before; }));
    }
    std::this_thread::sleep_for(250ms);
    assert(ticks.load() < 50 && "persistent wake failure must not spin the engine loop");
    if (mode == "poll") {
        assert(poll_errors.load() == 1); // Diagnostic is rate limited as well.
        const unsigned before = ticks.load();
        std::atomic<bool> ready{false};
        std::thread waiter([&] {
            rt->WaitFor([&](TsnxEngine *) { return ticks.load() > before + 1; }, nullptr);
            ready = true;
        });
        const bool notified = Eventually([&] { return ready.load(); }, 600ms);
        if (!notified) rt->Stop(); // Ensure cleanup even when testing the broken code.
        waiter.join();
        assert(notified && "failed polls must still notify engine waiters");
        fail_send = true;
        rt->With([](TsnxEngine *) {});
        assert(Eventually([&] { return retired.load() == 1; }, 600ms));
        assert(Eventually([&] { return reopened.load() == 1; }));
    } else if (mode == "stop" || mode == "stop-suspended") {
        assert(Eventually([&] { return retired.load() == 1; }));
        if (mode == "stop-suspended") assert(rt->Suspend(tsnx::Runtime::kPaused, 1500ms));
        rt->Stop();
        loop.join();
        assert(socket_attempts.load() == 2); // Stopping never resurrects the pair.
    } else if (mode != "normal") {
        assert(Eventually([&] { return retired.load() == 1; }));
        if (mode == "suspend") {
            assert(rt->Suspend(tsnx::Runtime::kPaused, 1500ms));
            const auto attempts = socket_attempts.load();
            std::this_thread::sleep_for(1200ms);
            assert(socket_attempts.load() == attempts); // No recovery while asleep/paused.
            fail_open = false;
            rt->Resume(tsnx::Runtime::kPaused);
        } else if (mode == "retry") {
            assert(Eventually([&] { return failed.load() > 0; }));
            const auto attempts = socket_attempts.load();
            std::this_thread::sleep_for(250ms);
            assert(socket_attempts.load() == attempts); // Failed creation is rate limited.
            fail_open = false;
        }
        assert(Eventually([&] { return reopened.load() == 1; }, 3500ms));
        const auto before = ticks.load();
        rt->With([](TsnxEngine *) {});
        assert(Eventually([&] { return ticks.load() > before; }, 500ms));
        assert(retired.load() == 1);
    } else {
        assert(retired.load() == 0);
        assert(socket_attempts.load() == 2);
    }
    rt->Stop();
    if (loop.joinable()) loop.join();
    rt.reset();
    tsnx_driver_free(driver);
    tsnx_engine_free(engine);
    std::printf("PASS runtime %s (%u ticks)\n", mode.c_str(), ticks.load());
}
