// Socket lifecycle tests with real local sockets and the real Rust engine.
// Fault injection is confined to this translation unit, not the driver API.
#include <assert.h>
#include <errno.h>
#include <stdio.h>
#include <sys/socket.h>
#include <time.h>

static int fail_socket, socket_attempts;
static time_t clock_offset;
static int test_socket(int domain, int type, int protocol) {
    ++socket_attempts;
    if (fail_socket) { errno = ENFILE; return -1; }
    return socket(domain, type, protocol);
}
static int test_clock_gettime(clockid_t clock, struct timespec *ts) {
    int rc = clock_gettime(clock, ts);
    if (rc == 0) ts->tv_sec += clock_offset;
    return rc;
}
#define socket test_socket
#define clock_gettime test_clock_gettime
#include "../driver/tsnx_driver.c"
#undef socket
#undef clock_gettime

static int recovery_logs;
static void event(void *ctx, const TsnxEvent *ev) {
    (void)ctx;
    if (ev->kind == TSNX_EVENT_LOG && strstr(ev->text, "UDP")) ++recovery_logs;
}
static TsnxDriver *driver(void) {
    uint8_t seed[32] = {1}, machine[32] = {2}, node[32] = {3}, disco[32] = {4};
    tsnx_seed_rng(seed);
    TsnxConfig cfg = {.control_url = "https://127.0.0.1:9", .hostname = "driver-recovery-test",
        .machine_key = machine, .node_key = node, .disco_key = disco};
    TsnxEngine *e = tsnx_engine_new(&cfg, tsnx_driver_now_ns(), (uint64_t)time(NULL));
    assert(e);
    TsnxDriver *d = tsnx_driver_new(e, event, NULL);
    assert(d);
    tsnx_driver_pump(d);
    return d;
}
static void destroy(TsnxDriver *d) {
    TsnxEngine *e = d->engine;
    tsnx_driver_free(d);
    tsnx_engine_free(e);
}
static int prepare(TsnxDriver *d, struct pollfd *fds) {
    int wait;
    tsnx_driver_prepare(d, fds, TSNX_DRIVER_MAX_FDS, 1000, &wait);
    assert(wait >= 0 && wait <= 1000);
    return wait;
}
static void persistent_error(void) {
    TsnxDriver *d = driver();
    assert(tsnx_driver_open_udp(d, 0) == 0);
    int pipefd[2];
    assert(pipe(pipefd) == 0);
    close(d->udp_fd);
    d->udp_fd = pipefd[0];
    close(pipefd[1]); // Real, persistently readable HUP, as in the reproduction.
    struct pollfd fds[TSNX_DRIVER_MAX_FDS];
    prepare(d, fds);
    assert(poll(&fds[d->poll_udp], 1, 0) == 1);
    assert(fds[d->poll_udp].revents & POLLHUP);
    tsnx_driver_dispatch(d, fds);
    assert(d->udp_fd == -1); // Bad descriptor must leave the poll set immediately.
    prepare(d, fds);
    assert(d->poll_udp == -1);
    assert(recovery_logs > 0);
    clock_offset += 2;
    prepare(d, fds);
    assert(d->udp_fd >= 0);
    struct pollfd idle = {.fd = d->udp_fd, .events = POLLIN};
    uint64_t start = tsnx_driver_now_ns();
    assert(poll(&idle, 1, 25) == 0);
    assert(tsnx_driver_now_ns() - start >= 20000000);
    destroy(d);
}
static void error_flags(void) {
    const short flags[] = {POLLERR, POLLHUP, POLLNVAL};
    for (size_t i = 0; i < sizeof flags / sizeof *flags; ++i) {
        TsnxDriver *d = driver();
        assert(tsnx_driver_open_udp(d, 0) == 0);
        struct pollfd fds[TSNX_DRIVER_MAX_FDS];
        prepare(d, fds);
        if (flags[i] == POLLNVAL) close(d->udp_fd);
        fds[d->poll_udp].revents = flags[i];
        tsnx_driver_dispatch(d, fds);
        assert(d->udp_fd == -1);
        destroy(d);
    }
}
static void failed_open(void) {
    TsnxDriver *d = driver();
    fail_socket = 1;
    assert(tsnx_driver_open_udp(d, 0) == -1);
    struct pollfd fds[TSNX_DRIVER_MAX_FDS];
    clock_offset += 2;
    prepare(d, fds);
    int attempts = socket_attempts;
    for (int i = 0; i < 1000; ++i) prepare(d, fds);
    assert(socket_attempts == attempts); // No tight retry loop while resources are unavailable.
    fail_socket = 0;
    clock_offset += 2;
    prepare(d, fds);
    assert(d->udp_fd >= 0); // Includes failure of the initial port=0 open.
    destroy(d);
}
static void suspended_retry(void) {
    TsnxDriver *d = driver();
    fail_socket = 1;
    assert(tsnx_driver_open_udp(d, 0) == -1);
    tsnx_driver_suspend(d);
    fail_socket = 0;
    clock_offset += 2;
    struct pollfd fds[TSNX_DRIVER_MAX_FDS];
    prepare(d, fds);
    assert(d->udp_fd == -1);
    assert(tsnx_driver_resume(d) == 0);
    assert(d->udp_fd >= 0);
    destroy(d);
}
static void stale_poll(void) {
    TsnxDriver *d = driver();
    assert(tsnx_driver_open_udp(d, 0) == 0);
    struct pollfd fds[TSNX_DRIVER_MAX_FDS];
    prepare(d, fds);
    int old = d->udp_fd;
    fds[d->poll_udp].revents = POLLHUP;
    tsnx_driver_suspend(d);
    assert(tsnx_driver_resume(d) == 0);
    if (d->udp_fd != old) {
        assert(dup2(d->udp_fd, old) == old);
        close(d->udp_fd);
        d->udp_fd = old;
    }
    tsnx_driver_dispatch(d, fds); // Same descriptor number, different socket lifetime.
    assert(d->udp_fd == old);
    assert(fcntl(old, F_GETFD) >= 0);
    destroy(d);
}
static void port_fallback(void) {
    TsnxDriver *d = driver();
    assert(tsnx_driver_open_udp(d, 0) == 0);
    uint16_t port = d->udp_port;
    tsnx_driver_suspend(d);
    int occupied = socket(AF_INET, SOCK_DGRAM, 0);
    struct sockaddr_in sin = {.sin_family = AF_INET, .sin_port = htons(port)};
    assert(bind(occupied, (struct sockaddr *)&sin, sizeof sin) == 0);
    assert(tsnx_driver_resume(d) == 0);
    assert(d->udp_port != port);
    close(occupied);
    destroy(d);
}
static void tcp_invalid(void) {
    TsnxDriver *d = driver();
    struct pollfd fds[TSNX_DRIVER_MAX_FDS];
    prepare(d, fds);
    assert(d->poll_conns > 0); // Real control connection created by the engine.
    Conn *c = d->poll_owners[0];
    uint32_t id = c->id;
    close(c->fd);
    assert(poll(fds, d->poll_conns, 0) > 0);
    assert(fds[0].revents & POLLNVAL);
    tsnx_driver_dispatch(d, fds);
    assert(c->id != id); // Invalid connection must leave the poll set.
    destroy(d);
}
int main(int argc, char **argv) {
    assert(argc == 2);
    if (!strcmp(argv[1], "persistent")) persistent_error();
    else if (!strcmp(argv[1], "flags")) error_flags();
    else if (!strcmp(argv[1], "retry")) failed_open();
    else if (!strcmp(argv[1], "suspend")) suspended_retry();
    else if (!strcmp(argv[1], "stale")) stale_poll();
    else if (!strcmp(argv[1], "port")) port_fallback();
    else if (!strcmp(argv[1], "tcp-invalid")) tcp_invalid();
    else abort();
    printf("PASS: %s\n", argv[1]);
}
