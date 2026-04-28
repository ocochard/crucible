// Copyright 2015 Intel Corporation
//
// Permission is hereby granted, free of charge, to any person obtaining a
// copy of this software and associated documentation files (the "Software"),
// to deal in the Software without restriction, including without limitation
// the rights to use, copy, modify, merge, publish, distribute, sublicense,
// and/or sell copies of the Software, and to permit persons to whom the
// Software is furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice (including the next
// paragraph) shall be included in all copies or substantial portions of the
// Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.  IN NO EVENT SHALL
// THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
// FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
// IN THE SOFTWARE.

/// \file
/// \brief The runner's dispatcher process

#include <limits.h>
#include <signal.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#include <fcntl.h>
#include <unistd.h>
#include <sys/epoll.h>
#include <sys/mman.h>
#include <sys/signalfd.h>
#include <sys/types.h>
#include <sys/wait.h>

#include <libxml/tree.h>

#include "framework/test/test.h"
#include "framework/test/test_def.h"

#include "util/log.h"
#include "util/string.h"

#include "dispatcher.h"
#include "runner.h"
#include "runner_vk.h"
#include "worker.h"

typedef struct worker worker_t;
typedef struct worker_pipe worker_pipe_t;

struct worker_pipe {
    union {
        int fd[2];

        struct {
            int read_fd;
            int write_fd;
        };
    };

    /// A reference to the pipe's containing worker. Used by epoll handlers.
    worker_t *worker;
};

/// \brief A worker process's proxy in the dispatcher process.
///
/// The struct is valid if and only if worker::pid != 0.
struct worker {
    /// Cases:
    ///   * if 0: the proxy is not connected to a process,
    ///           and the struct is invalid
    ///   * if > 0: the dispatcher forked the worker and has not yet reaped it
    pid_t pid;

    struct {
        uint32_t len;
        const test_def_t *data[256];
        uint64_t timeout[256];
    } tests;

    worker_pipe_t dispatch_pipe;
    worker_pipe_t result_pipe;

    /// Each worker process's stdout and stderr are connected to a pipe in the
    /// dispatcher process. This prevents concurrently running workers from
    /// corrupting the dispatcher's stdout and stderr with interleaved output.
    worker_pipe_t stdout_pipe;
    worker_pipe_t stderr_pipe;

    /// Number of tests dispatched to the worker over its lifetime.
    uint32_t lifetime_test_count;

    bool recvd_sentinel;
    bool is_dead;
};

static struct dispatcher {
    atomic_bool sigint_flag;
    atomic_bool goto_next_phase;

    int epoll_fd;
    int signal_fd;

    /// Count of currently dispatched tests.
    uint32_t cur_dispatched_tests;

    /// Maximum allowed count of currently dispatched tests.
    uint32_t max_dispatched_tests;

    uint32_t num_tests;
    uint32_t num_pass;
    uint32_t num_fail;
    uint32_t num_skip;
    uint32_t num_lost;

    uint32_t num_workers;
    worker_t workers[64];

    uint32_t num_vulkan_queues;

    uint64_t test_case_timeout_ns;
    int epoll_timeout_ms;

    struct {
        char *filepath;
        FILE *file;
        xmlDocPtr doc;
        xmlNodePtr testsuite_node;
    } junit;

} dispatcher = {
    .epoll_fd = -1,
    .signal_fd = -1,
};

static uint32_t dispatcher_get_num_ran_tests(void);
static void dispatcher_print_header(void);
static void dispatcher_gather_vulkan_info(void);
static void dispatcher_enter_dispatch_phase(void);
static void dispatcher_enter_cleanup_phase(void);
static void dispatcher_print_summary(void);

static void dispatcher_dispatch_loop_no_fork(void);
static void dispatcher_dispatch_loop_with_fork(void);

static void dispatcher_dispatch_test(const test_def_t *def,
                                     uint32_t queue_num);
static worker_t * dispatcher_get_open_worker(void);
static worker_t * dispatcher_get_new_worker(void);
static worker_t * dispatcher_find_unborn_worker(void);
static void dispatcher_cleanup_dead_worker(worker_t *worker);

static void dispatcher_collect_result(int timeout_ms);

static void dispatcher_report_result(const test_def_t *def, uint32_t queue_num,
                                     pid_t pid, test_result_t result);
static bool dispatcher_send_packet(worker_t *worker,
                                   const dispatch_packet_t *pk);

static void dispatcher_kill_all_workers(void);

static void dispatcher_init_epoll(void);
static void dispatcher_finish_epoll(void);
static bool dispatcher_epoll_add_worker_pipe(worker_pipe_t *pipe, int rw);

static void dispatcher_handle_epoll_event(const struct epoll_event *event);
static void dispatcher_handle_pipe_event(const struct epoll_event *event);
static void dispatcher_handle_signal_event(const struct epoll_event *event);
static void dispatcher_handle_sigchld(void);
static void dispatcher_handle_sigint(int sig);
static void dispatcher_yield_to_sigint(void);

static bool worker_is_open(const worker_t *worker);
static int32_t worker_find_test(worker_t *worker, const test_def_t *def);
static bool worker_insert_test(worker_t *worker, const test_def_t *def);
static void worker_rm_test(worker_t *worker, const test_def_t *def);

static bool worker_start_test(worker_t *worker, const test_def_t *def,
                              uint32_t queue_num);
static void worker_send_sentinel(worker_t *worker);
static void worker_drain_result_pipe(worker_t *worker);

static bool worker_pipe_init(worker_t *worker, worker_pipe_t *pipe);
static void worker_pipe_finish(worker_pipe_t *pipe);
static bool worker_pipe_become_reader(worker_pipe_t *pipe);
static bool worker_pipe_become_writer(worker_pipe_t *pipe);
static void worker_pipe_drain_to_fd(worker_pipe_t *pipe, int fd);

static worker_t *find_worker_by_pid(pid_t pid);

#define dispatcher_for_each_worker_slot(s)                                   \
    for ((s) = dispatcher.workers;                                           \
         (s) < dispatcher.workers + ARRAY_LENGTH(dispatcher.workers);        \
         (s) = (worker_t *) (s) + 1)

/// Convert (const char *) to (const unsigned char *).
///
/// We need this annoying function because libxml2 strings are not (char *) but
/// (xmlChar *), which is a typedef for (unsigned char *).
static inline const unsigned char *
u(const char *str)
{
    return (const unsigned char *) str;
}

static void
junit_xml_error_handler(void *xml_ctx, const char *msg, ...)
{
    va_list va;

    va_start(va, msg);
    log_abort_v(msg, va);
    va_end(va);
}

/* glibc spells the signal-handler function pointer as `sighandler_t`, but
 * FreeBSD (and POSIX in general) does not. Use a portable local typedef. */
typedef void (*cru_sighandler_t)(int);

static void
set_sigint_handler(cru_sighandler_t handler)
{
    const struct sigaction sa = {
        .sa_handler = handler,
    };

    if (sigaction(SIGINT, &sa, NULL) == -1) {
        loge("test runner failed to set SIGINT handler");
        abort();
    }
}

static bool
junit_init(void)
{
    if (!runner_opts.junit_xml_filepath)
        return true;

    xmlSetGenericErrorFunc(/*ctx*/ NULL, junit_xml_error_handler);

    dispatcher.junit.filepath = xstrdup(runner_opts.junit_xml_filepath);
    dispatcher.junit.file = fopen(dispatcher.junit.filepath, "w");
    if (!dispatcher.junit.file) {
        loge("failed to open junit xml file: %s", dispatcher.junit.filepath);
        free(dispatcher.junit.filepath);
        return false;
    }

    xmlDocPtr doc = xmlNewDoc(u("1.0"));
    doc->encoding = u(strdup("UTF-8"));

    xmlNodePtr root_node = xmlNewNode(NULL, u("testsuites"));
    xmlDocSetRootElement(doc, root_node);

    xmlNodePtr testsuite_node = xmlNewChild(root_node, /*namespace*/ NULL,
                                            /*name*/ u("testsuite"),
                                            /*context*/ NULL);
    xmlNewProp(testsuite_node, u("name"), u("crucible"));

    dispatcher.junit.doc = doc;
    dispatcher.junit.testsuite_node = testsuite_node;

    return true;
}

static void
junit_add_result(const char *name, test_result_t result)
{
    if (!dispatcher.junit.doc)
        return;

    xmlNodePtr testcase_node = xmlNewNode(NULL, u("testcase"));

    // Insert the node's "status" attribute before the "name" attribute because
    // that makes it easier to visually parse the results in the formatted XML.
    // If formatted well, each status will be left-aligned like this:
    //
    //   <testcase status="pass" name="cheddar"/>
    //   <testcase status="pass" name="mozarella"/>
    //   <testcase status="fail" name="blue-cheese"/>
    xmlNewProp(testcase_node, u("status"), u(test_result_to_string(result)));
    xmlNewProp(testcase_node, u("name"), u(name));

    switch (result) {
    case TEST_RESULT_PASS:
        break;
    case TEST_RESULT_FAIL:
        // In JUnit, a testcase "failure" occurs when the test intentionally
        // fails, for example, by calling t_fail() or t_assert(...). Crashes
        // are not failures.
        //
        // FINISHME: Capture the tests's reason for failure in the JUnit XML by
        // capturing its stdout and stderr.
        xmlNewChild(testcase_node, NULL, u("failure"), NULL);
        break;
    case TEST_RESULT_SKIP:
        xmlNewChild(testcase_node, NULL, u("skipped"), NULL);
        break;
    case TEST_RESULT_LOST: {
            // In JUnit, as testcase "error" occurs when a test unintentionally
            // fails, for example, by crashing. An "error" is more extreme than
            // a "failure".
            //
            // FINISHME: Report exit code of lost test, when possible.
            xmlNodePtr error_node = xmlNewChild(testcase_node, NULL, u("error"),
                                                NULL);
            xmlNewProp(error_node, u("type"), u("lost"));
            xmlNewProp(error_node, u("message"), u("test was lost, it likely crashed"));
            break;
    }
    }

    xmlAddChild(dispatcher.junit.testsuite_node, testcase_node);
}

static bool
junit_finish(void)
{
    bool rc = true;

    xmlDocPtr doc = dispatcher.junit.doc;
    if (!doc)
        return rc;

    string_t buf = STRING_INIT;
    xmlNodePtr root_node = xmlDocGetRootElement(doc);
    xmlNodePtr testsuite_node = dispatcher.junit.testsuite_node;

    string_printf(&buf, "%u", dispatcher_get_num_ran_tests());
    xmlNewProp(root_node, u("tests"), u(string_data(&buf)));
    xmlNewProp(testsuite_node, u("tests"), u(string_data(&buf)));

    string_printf(&buf, "%u", dispatcher.num_fail);
    xmlNewProp(root_node, u("failures"), u(string_data(&buf)));
    xmlNewProp(testsuite_node, u("failures"), u(string_data(&buf)));

    string_printf(&buf, "%u", dispatcher.num_lost);
    xmlNewProp(root_node, u("errors"), u(string_data(&buf)));
    xmlNewProp(testsuite_node, u("errors"), u(string_data(&buf)));

    string_printf(&buf, "%u", dispatcher.num_skip);
    xmlNewProp(root_node, u("disabled"), u(string_data(&buf)));
    xmlNewProp(testsuite_node, u("disabled"), u(string_data(&buf)));

    if (xmlDocFormatDump(dispatcher.junit.file, doc,
                         /*format*/ 1) == -1) {
        loge("failed to write junit xml file: %s", dispatcher.junit.filepath);
        rc = false;
    }

    if (fclose(dispatcher.junit.file) == -1) {
        loge("failed to close junit xml file: %s", dispatcher.junit.filepath);
        rc = false;
    }

    free(dispatcher.junit.filepath);
    xmlFreeDoc(dispatcher.junit.doc);
    memset(&dispatcher.junit, 0, sizeof(dispatcher.junit));

    return rc;
}

bool
dispatcher_run(uint32_t num_tests)
{
    dispatcher.num_tests = num_tests;
    dispatcher.max_dispatched_tests =
        CLAMP(runner_opts.jobs, 1, ARRAY_LENGTH(dispatcher.workers));
    dispatcher.test_case_timeout_ns = 1000000000ull * runner_opts.timeout_s;
    dispatcher.epoll_timeout_ms =
        runner_opts.timeout_s ?
        runner_opts.timeout_s * 100 /* 1/10th the timeout time */ :
        -1 /* no timeout, so wait forever */;

    dispatcher_gather_vulkan_info();
    if (dispatcher.goto_next_phase)
        return false;

    if (!junit_init())
        return false;

    dispatcher_init_epoll();
    set_sigint_handler(dispatcher_handle_sigint);

    dispatcher_print_header();
    dispatcher_enter_dispatch_phase();
    dispatcher_enter_cleanup_phase();
    dispatcher_print_summary();

    set_sigint_handler(SIG_DFL);
    dispatcher_finish_epoll();

    if (!junit_finish())
        return false;

    return dispatcher.num_pass + dispatcher.num_skip == dispatcher.num_tests;
}

static uint32_t
dispatcher_get_num_ran_tests(void)
{
    return dispatcher.num_pass + dispatcher.num_fail + dispatcher.num_skip +
           dispatcher.num_lost;
}

static void
dispatcher_print_header(void)
{
    log_align_tags(true);
    logi("running %u tests", dispatcher.num_tests);
    logi("================================");

}

static void
dispatcher_print_summary(void)
{
    // A big, and perhaps unneeded, hammer.
    fflush(stdout);
    fflush(stderr);

    logi("================================");
    logi("ran %u tests", dispatcher_get_num_ran_tests());
    logi("pass %u", dispatcher.num_pass);
    logi("fail %u", dispatcher.num_fail);
    logi("skip %u", dispatcher.num_skip);
    logi("lost %u", dispatcher.num_lost);
}

static void
dispatcher_gather_vulkan_info(void)
{
    uint32_t num_vulkan_queues;

    if (runner_opts.no_fork) {
        if (!runner_get_vulkan_queue_count(&dispatcher.num_vulkan_queues))
            dispatcher.goto_next_phase = true;
        return;
    }
    worker_pipe_t pipe;
    worker_pipe_init(NULL, &pipe);
    int pid = fork();

    if (pid == -1) {
        loge("test runner failed to fork process to gather vulkan info");
        goto fail;
    }

    if (pid == 0) {
        // Send any child process (driver) output to /dev/null while
        // reading the number of queues.
        int devnull = open("/dev/null", O_WRONLY);
        dup2(devnull, 1);
        dup2(devnull, 2);

        // Read the number of queues and send it through the pipe
        worker_pipe_become_writer(&pipe);
        if (!runner_get_vulkan_queue_count(&num_vulkan_queues)) {
            exit(EXIT_FAILURE);
        } else {
            if (write(pipe.write_fd, &num_vulkan_queues,
                      sizeof(num_vulkan_queues)) != sizeof(num_vulkan_queues))
                exit(EXIT_FAILURE);
        }
        exit(EXIT_SUCCESS);
    } else {
        // Read the number of queues from the pipe
        worker_pipe_become_reader(&pipe);
        if (read(pipe.read_fd, &num_vulkan_queues,
                 sizeof(num_vulkan_queues)) != sizeof(num_vulkan_queues))
            goto fail;
    }

    int result;
    waitpid(pid, &result, 0);
    result = WIFEXITED(result) ? WEXITSTATUS(result) : EXIT_FAILURE;
    if (result != 0)
        goto fail;

    worker_pipe_finish(&pipe);
    dispatcher.num_vulkan_queues = num_vulkan_queues;
    return;

 fail:
    worker_pipe_finish(&pipe);
    loge("test runner failed to gather vulkan info");
    dispatcher.goto_next_phase = true;
}

static void
dispatcher_enter_dispatch_phase(void)
{
    if (runner_opts.no_fork) {
        dispatcher_dispatch_loop_no_fork();
    } else {
        dispatcher_dispatch_loop_with_fork();
    }
}

static void
dispatcher_enter_cleanup_phase(void)
{
    worker_t *worker;

    if (runner_opts.no_fork)
        return;

    // Tell each worker that it will receive no more tests.
    dispatcher_for_each_worker_slot(worker) {
        if (!worker->pid)
            continue;

        worker_send_sentinel(worker);
        if (dispatcher.goto_next_phase)
            return;
    }

    while (dispatcher.num_workers > 0) {
        dispatcher_collect_result(dispatcher.epoll_timeout_ms);
        if (dispatcher.goto_next_phase)
            return;
    }
}

/// Run all tests in the dispatcher process.
static void
dispatcher_dispatch_loop_no_fork(void)
{
    const test_def_t *def;

    cru_foreach_test_def(def) {
        uint32_t queue_start, queue_end;
        if (def->priv.queue_num == NO_QUEUE_NUM_PREF) {
            queue_start = 0;
            queue_end = dispatcher.num_vulkan_queues;
        } else {
            queue_start = def->priv.queue_num;
            queue_end = def->priv.queue_num + 1;
        }

        for (uint32_t qi = queue_start; qi < queue_end; qi++) {
            test_result_t result;

            if (!def->priv.enable)
                continue;

            if (qi >= dispatcher.num_vulkan_queues) {
                logi("queue-family-index %d does not exist", qi);
                dispatcher_report_result(def, qi, 0, TEST_RESULT_SKIP);
                continue;
            }

            if (def->skip) {
                dispatcher_report_result(def, qi, 0, TEST_RESULT_SKIP);
                continue;
            }

            log_tag("start", 0, "%s.q%d", def->name, qi);
            result = run_test_def(def, qi);
            dispatcher_report_result(def, qi, 0, result);
        }
    }
}

/// Dispatch tests to worker processes.
static void
dispatcher_dispatch_loop_with_fork(void)
{
    const test_def_t *def;

    cru_foreach_test_def(def) {
        uint32_t queue_start, queue_end;
        if (def->priv.queue_num == NO_QUEUE_NUM_PREF) {
            queue_start = 0;
            queue_end = dispatcher.num_vulkan_queues;
        } else {
            queue_start = def->priv.queue_num;
            queue_end = def->priv.queue_num + 1;
        }

        for (uint32_t qi = queue_start; qi < queue_end; qi++) {
            if (!def->priv.enable)
                continue;

            if (qi >= dispatcher.num_vulkan_queues) {
                logi("queue-family-index %d does not exist", qi);
                dispatcher_report_result(def, qi, 0, TEST_RESULT_SKIP);
                continue;
            }

            if (def->skip) {
                dispatcher_report_result(def, qi, 0, TEST_RESULT_SKIP);
                continue;
            }

            dispatcher_dispatch_test(def, qi);
            if (dispatcher.goto_next_phase)
                return;

            dispatcher_collect_result(0);
            if (dispatcher.goto_next_phase)
                return;
        }
    }
}

static uint64_t
gettime_ns()
{
    struct timespec current;
    int ret = clock_gettime(CLOCK_MONOTONIC, &current);
    assert(ret >= 0);
    if (ret < 0)
        return 0;

    return (uint64_t) current.tv_sec * 1000000000ULL + current.tv_nsec;
}

static void
dispatcher_dispatch_test(const test_def_t *def, uint32_t queue_num)
{
    worker_t *worker = NULL;

    assert(dispatcher.cur_dispatched_tests <= dispatcher.max_dispatched_tests);

    dispatcher_yield_to_sigint();
    if (dispatcher.goto_next_phase)
        return;

    while (dispatcher.cur_dispatched_tests == dispatcher.max_dispatched_tests) {
        dispatcher_collect_result(0);
        if (dispatcher.goto_next_phase)
            return;
    }

    while (!worker) {
        dispatcher_yield_to_sigint();
        if (dispatcher.goto_next_phase)
            return;

        worker = dispatcher_get_open_worker();
        if (dispatcher.goto_next_phase)
            return;
    }

    worker_start_test(worker, def, queue_num);
}

static worker_t *
dispatcher_get_open_worker(void)
{
    for (;;) {
        worker_t *worker = NULL;

        if (dispatcher.goto_next_phase)
            return NULL;

        dispatcher_for_each_worker_slot(worker) {
            if (worker_is_open(worker)) {
                return worker;
            }
        }

        switch (runner_opts.isolation_mode) {
        case RUNNER_ISOLATION_MODE_PROCESS:
            if (dispatcher.num_workers < dispatcher.max_dispatched_tests) {
                return dispatcher_get_new_worker();
            }
            break;
        case RUNNER_ISOLATION_MODE_THREAD:
            if (dispatcher.num_workers == 0) {
                return dispatcher_get_new_worker();
            }
            break;
        }

        // All workers are busy. Wait for a test to finish, then try again.
        dispatcher_collect_result(dispatcher.epoll_timeout_ms);
    }
}

static worker_t *
dispatcher_get_new_worker(void)
{
    worker_t *worker;

    if (dispatcher.goto_next_phase)
        return NULL;

    worker = dispatcher_find_unborn_worker();
    if (!worker)
        return NULL;

    assert(!worker->pid);
    *worker = (worker_t) {0};

    if (!worker_pipe_init(worker, &worker->dispatch_pipe))
        goto fail;
    if (!worker_pipe_init(worker, &worker->result_pipe))
        goto fail;
    if (!worker_pipe_init(worker, &worker->stdout_pipe))
        goto fail;
    if (!worker_pipe_init(worker, &worker->stderr_pipe))
        goto fail;

    // Flush standard out and error before forking.  Otherwise, both the
    // child and parent processes will have the same queue and, when that
    // gets flushed, we'll end up with duplicate data in the output.
    fflush(stdout);
    fflush(stderr);

    worker->pid = fork();

    if (worker->pid == -1) {
        worker->pid = 0;
        loge("test runner failed to fork worker process");
        goto fail;
    }

    if (worker->pid == 0) {
        // Before the worker duplicates stdout and stderr, write only to the
        // debug log. This avoids corrupting the dispatcher's stdout and stderr
        // with interleaved output during concurrent test runs.
        if (!(dup2(worker->stdout_pipe.write_fd, STDOUT_FILENO) != -1 &&
              dup2(worker->stderr_pipe.write_fd, STDERR_FILENO) != -1)) {
            logd("runner failed to dup worker's stdout and stderr");
            exit(EXIT_FAILURE);
        }

        worker_pipe_finish(&worker->stdout_pipe);
        worker_pipe_finish(&worker->stderr_pipe);

        set_sigint_handler(SIG_DFL);
        dispatcher_finish_epoll();

        if (!worker_pipe_become_reader(&worker->dispatch_pipe))
            exit(EXIT_FAILURE);
        if (!worker_pipe_become_writer(&worker->result_pipe))
            exit(EXIT_FAILURE);

        worker_run(worker->dispatch_pipe.read_fd, worker->result_pipe.write_fd);

        exit(EXIT_SUCCESS);
    }

    if (!worker_pipe_become_writer(&worker->dispatch_pipe))
        goto fail;
    if (!worker_pipe_become_reader(&worker->result_pipe))
        goto fail;
    if (!worker_pipe_become_reader(&worker->stdout_pipe))
        goto fail;
    if (!worker_pipe_become_reader(&worker->stderr_pipe))
        goto fail;

    if (fcntl(worker->result_pipe.read_fd, F_SETFL, O_NONBLOCK) == -1)
        goto fail;
    if (fcntl(worker->stdout_pipe.read_fd, F_SETFL, O_NONBLOCK) == -1)
        goto fail;
    if (fcntl(worker->stderr_pipe.read_fd, F_SETFL, O_NONBLOCK) == -1)
        goto fail;

    if (!dispatcher_epoll_add_worker_pipe(&worker->result_pipe, 0))
        goto fail;
    if (!dispatcher_epoll_add_worker_pipe(&worker->stdout_pipe, 0))
        goto fail;
    if (!dispatcher_epoll_add_worker_pipe(&worker->stderr_pipe, 0))
        goto fail;

    ++dispatcher.num_workers;

    return worker;

fail:
    loge("runner failed to initialize worker process");

    // If we can't create workers, we should proceed to the result summary.
    dispatcher.goto_next_phase = true;

    return NULL;
}

static void
dispatcher_cleanup_dead_worker(worker_t *worker)
{
    int err;

    assert(worker->pid);
    assert(worker->is_dead);

    worker_drain_result_pipe(worker);
    worker_pipe_drain_to_fd(&worker->stdout_pipe, STDOUT_FILENO);
    worker_pipe_drain_to_fd(&worker->stderr_pipe, STDERR_FILENO);

    // Any remaining tests owned by the worker are lost.
    for (uint32_t i = 0; i < worker->tests.len; ++i) {
        const test_def_t *def = worker->tests.data[i];
        dispatcher_report_result(def, 0, worker->pid, TEST_RESULT_LOST);
    }

    assert(dispatcher.cur_dispatched_tests >= worker->tests.len);
    dispatcher.cur_dispatched_tests -= worker->tests.len;
    worker->tests.len = 0;

    err = epoll_ctl(dispatcher.epoll_fd, EPOLL_CTL_DEL,
                    worker->result_pipe.read_fd, NULL);
    if (err == -1) {
        loge("runner failed to remove worker process's pipe from epoll "
             "fd; abort!");
        abort();
    }

    worker_pipe_finish(&worker->dispatch_pipe);
    worker_pipe_finish(&worker->result_pipe);
    worker_pipe_finish(&worker->stdout_pipe);
    worker_pipe_finish(&worker->stderr_pipe);

    worker->pid = 0;
    --dispatcher.num_workers;
}

static worker_t *
dispatcher_find_unborn_worker(void)
{
    worker_t *worker;

    dispatcher_for_each_worker_slot(worker) {
        if (!worker->pid) {
            return worker;
        }
    }

    return NULL;
}

static void
dispatcher_check_timeout()
{
    worker_t *worker = NULL;

    if (dispatcher.goto_next_phase || runner_opts.timeout_s == 0)
        return;

    int err;
    uint64_t timenow = gettime_ns();
    dispatcher_for_each_worker_slot(worker) {
        for (int i = 0; i < worker->tests.len; i++) {
            if (timenow > worker->tests.timeout[i]) {
                err = kill(worker->pid, SIGINT);
                if (err) {
                    loge("runner failed to kill child process %d", worker->pid);
                    abort();
                }
            }
        }
    }
}

static void
dispatcher_collect_result(int timeout_ms)
{
    struct epoll_event event;

    dispatcher_yield_to_sigint();
    if (dispatcher.goto_next_phase)
        return;

    dispatcher_check_timeout();

    if (epoll_wait(dispatcher.epoll_fd, &event, 1, timeout_ms) <= 0)
        return;

    dispatcher_handle_epoll_event(&event);
}

static void
dispatcher_report_result(const test_def_t *def, uint32_t queue_num,
                         pid_t pid, test_result_t result)
{
    string_t name = STRING_INIT;
    string_printf(&name, "%s.q%d", def->name, queue_num);
    log_tag(test_result_to_string(result), pid, "%s", string_data(&name));
    fflush(stdout);

    switch (result) {
    case TEST_RESULT_PASS: dispatcher.num_pass++; break;
    case TEST_RESULT_FAIL: dispatcher.num_fail++; break;
    case TEST_RESULT_SKIP: dispatcher.num_skip++; break;
    case TEST_RESULT_LOST: dispatcher.num_lost++; break;
    }

    junit_add_result(string_data(&name), result);
    string_finish(&name);
}

static bool
dispatcher_send_packet(worker_t *worker, const dispatch_packet_t *pk)
{
    bool result = false;
    const struct sigaction ignore_sa = { .sa_handler = SIG_IGN };
    struct sigaction old_sa;
    int err;

    // If the worker process died, then writing to its dispatch pipe will emit
    // SIGPIPE. Ignore it, because the dispatcher should never die.
    err = sigaction(SIGPIPE, &ignore_sa, &old_sa);
    if (err == -1) {
        loge("test runner failed to disable SIGPIPE");
        abort();
    }

    static_assert(sizeof(*pk) <= PIPE_BUF, "dispatch packets will not be read "
                  "and written atomically");
    if (write(worker->dispatch_pipe.write_fd, pk, sizeof(*pk)) != sizeof(*pk))
        goto cleanup;

    result = true;

cleanup:
    err = sigaction(SIGPIPE, &old_sa, NULL);
    if (err == -1) {
        loge("test runner failed to re-enable SIGPIPE");
        abort();
    }

    return result;
}

static void
dispatcher_kill_all_workers(void)
{
    int err;
    worker_t *worker;

    dispatcher_for_each_worker_slot(worker) {
        if (!worker->pid)
            continue;

        err = kill(worker->pid, SIGINT);
        if (err) {
            loge("runner failed to kill child process %d", worker->pid);
            abort();
        }
    }
}

static void
dispatcher_init_epoll(void)
{
    sigset_t sigset;
    int err;

    assert(dispatcher.signal_fd == -1);
    assert(dispatcher.epoll_fd == -1);

    sigemptyset(&sigset);
    sigaddset(&sigset, SIGCHLD);

    dispatcher.signal_fd = signalfd(-1, &sigset, SFD_CLOEXEC);
    if (dispatcher.signal_fd == -1)
        goto fail;

    err = sigprocmask(SIG_BLOCK, &sigset, NULL);
    if (err == -1)
        goto fail;

    dispatcher.epoll_fd = epoll_create1(EPOLL_CLOEXEC);
    if (dispatcher.epoll_fd == -1)
        goto fail;

    err = epoll_ctl(dispatcher.epoll_fd, EPOLL_CTL_ADD, dispatcher.signal_fd,
                    &(struct epoll_event) {
                        .events = EPOLLIN,
                        .data = {
                            .ptr = &dispatcher.signal_fd,
                        },
                    });
    if (err == -1)
        goto fail;

    return;

fail:
    loge("runner failed to setup epoll fd");
    dispatcher.goto_next_phase = true;
}

static void
dispatcher_finish_epoll(void)
{
    sigset_t sigset;

    assert(dispatcher.signal_fd >= 0);
    assert(dispatcher.epoll_fd >= 0);

    close(dispatcher.signal_fd);
    close(dispatcher.epoll_fd);

    sigemptyset(&sigset);
    sigaddset(&sigset, SIGCHLD);
    sigprocmask(SIG_UNBLOCK, &sigset, NULL);
}

static bool
dispatcher_epoll_add_worker_pipe(worker_pipe_t *pipe, int rw)
{
    int err;

    assert(rw == 0 || rw == 1);

    err = epoll_ctl(dispatcher.epoll_fd, EPOLL_CTL_ADD, pipe->fd[rw],
                    &(struct epoll_event) {
                        .events = EPOLLIN,
                        .data = { .ptr = pipe },
                    });
    if (err == -1) {
        loge("runner failed to add a worker pipe to epoll fd");
        return false;
    }

    return true;
}

static void
dispatcher_handle_epoll_event(const struct epoll_event *event)
{
    if (event->data.ptr == &dispatcher.signal_fd) {
        dispatcher_handle_signal_event(event);
    } else {
        dispatcher_handle_pipe_event(event);
    }
}

static void
dispatcher_handle_pipe_event(const struct epoll_event *event)
{
    worker_pipe_t *pipe = event->data.ptr;

    assert(event->data.ptr != &dispatcher.signal_fd);

    switch ((void*) pipe - (void*) pipe->worker) {
    case offsetof(worker_t, result_pipe):
        worker_drain_result_pipe(pipe->worker);
        break;
    case offsetof(worker_t, stdout_pipe):
        worker_pipe_drain_to_fd(pipe, STDOUT_FILENO);
        break;
    case offsetof(worker_t, stderr_pipe):
        worker_pipe_drain_to_fd(pipe, STDERR_FILENO);
        break;
    default:
        log_internal_error("invalid worker pipe in epoll event");
        break;
    }
}

static void
dispatcher_handle_signal_event(const struct epoll_event *event)
{
    struct signalfd_siginfo siginfo;
    int n;

    assert(event->data.ptr == &dispatcher.signal_fd);

    n = read(dispatcher.signal_fd, &siginfo, sizeof(siginfo));
    if (n != sizeof(siginfo))
        log_abort("runner failed to read from signal fd");

    switch (siginfo.ssi_signo) {
    case SIGCHLD:
        dispatcher_handle_sigchld();
        break;
    default:
        log_abort("runner caught unexpected signal %d", siginfo.ssi_signo);
        break;
    }
}

static void
dispatcher_handle_sigchld(void)
{
    pid_t pid;

    while ((pid = waitpid(-1, /*status*/ NULL, WNOHANG)) > 0) {
        worker_t *worker;

        worker = find_worker_by_pid(pid);
        if (!worker) {
            loge("runner caught unexpected pid");
            dispatcher.goto_next_phase = true;
            return;
        }

        worker->is_dead = true;
        dispatcher_cleanup_dead_worker(worker);
    }
}

static void
dispatcher_handle_sigint(int sig)
{
    assert(sig == SIGINT);
    atomic_store(&dispatcher.sigint_flag, true);
}

/// Take actions triggered by any previously received SIGINT.
static void
dispatcher_yield_to_sigint(void)
{
    if (!atomic_exchange(&dispatcher.sigint_flag, false))
        return;

    dispatcher_kill_all_workers();

    // A second SIGINT, if received before the runner resumes dispatching
    // tests, halts the testrun. Give the user a short window in which to send
    // the second SIGINT.
    nanosleep(&(struct timespec) { .tv_nsec = 500000000 }, NULL);

    if (!atomic_exchange(&dispatcher.sigint_flag, false))
        return;

    // The runner received the second SIGINT. Halt the testrun.
    dispatcher.goto_next_phase = true;
}

/// Is the worker accepting new tests?
static bool
worker_is_open(const worker_t *worker)
{
    if (!worker->pid)
        return false;

    if (worker->is_dead)
        return false;

    switch (runner_opts.isolation_mode) {
    case RUNNER_ISOLATION_MODE_PROCESS:
        // The dispatcher sends each worker exactly one test.
        return worker->lifetime_test_count == 0;
    case RUNNER_ISOLATION_MODE_THREAD:
        return worker->tests.len < ARRAY_LENGTH(worker->tests.data);
    }

    return false;
}

static int32_t
worker_find_test(worker_t *worker, const test_def_t *def)
{
    for (uint32_t i = 0; i < worker->tests.len; ++i) {
        if (worker->tests.data[i] == def) {
            return i;
        }
    }

    return -1;
}

static bool
worker_insert_test(worker_t *worker, const test_def_t *def)
{
    if (worker->is_dead)
        return false;

    if (worker->tests.len >= ARRAY_LENGTH(worker->tests.data))
        return false;

    size_t test_idx = worker->tests.len++;
    worker->tests.data[test_idx] = def;
    worker->tests.timeout[test_idx] =
        dispatcher.test_case_timeout_ns ?
        gettime_ns() + dispatcher.test_case_timeout_ns :
        UINT64_MAX;
    ++dispatcher.cur_dispatched_tests;

    return true;
}

static void
worker_rm_test(worker_t *worker, const test_def_t *def)
{
    int32_t i;

    i = worker_find_test(worker, def);
    if (i < 0) {
        loge("worker cannot remove test it doesn't own");
        return;
    }

    assert(worker->tests.len >= 1);
    assert(dispatcher.cur_dispatched_tests >= 1);

    --worker->tests.len;
    --dispatcher.cur_dispatched_tests;

    memmove(worker->tests.data + i, worker->tests.data + i + 1,
            worker->tests.len);
}

static bool
worker_start_test(worker_t *worker, const test_def_t *def,
                 uint32_t queue_num)
{
    const dispatch_packet_t pk = {
        .test_def = def,
        .queue_num = queue_num,
    };

    if (!def)
        return false;

    if (!worker->pid)
        return false;

    if (dispatcher.cur_dispatched_tests >= dispatcher.max_dispatched_tests)
        return false;

    if (!worker_insert_test(worker, def))
        return false;

    log_tag("start", worker->pid, "%s.q%d", def->name, 0);

    if (!dispatcher_send_packet(worker, &pk)) {
        worker_rm_test(worker, def);
        return false;
    }

    ++worker->lifetime_test_count;

    switch (runner_opts.isolation_mode) {
    case RUNNER_ISOLATION_MODE_PROCESS:
        // The dispatcher sends each worker exactly one test.
        worker_send_sentinel(worker);
        break;
    case RUNNER_ISOLATION_MODE_THREAD:
        // The dispatcher may send the worker multiple tests. The dispatcher
        // will tell the worker to expect no more tests by later sending it a
        // NULL test.
        break;
    }

    return true;
}

static void
worker_send_sentinel(worker_t *worker)
{
    assert(worker->pid);

    if (worker->recvd_sentinel || worker->is_dead)
        return;

    dispatcher_send_packet(worker, &(dispatch_packet_t) { .test_def = NULL });
    worker->recvd_sentinel = true;
}

static void
worker_drain_result_pipe(worker_t *worker)
{
    for (;;) {
        result_packet_t pk;

        static_assert(sizeof(pk) <= PIPE_BUF, "result packets will not be "
                      "read and written atomically");

        // To avoid deadlock between dispatcher and worker, this read must be
        // non-blocking.
        if (read(worker->result_pipe.read_fd, &pk, sizeof(pk)) != sizeof(pk))
            return;

        worker_rm_test(worker, pk.test_def);
        dispatcher_report_result(pk.test_def, pk.queue_num, worker->pid,
                                 pk.result);
    }
}

static worker_t *
find_worker_by_pid(pid_t pid)
{
    worker_t *worker;

    dispatcher_for_each_worker_slot(worker) {
        if (worker->pid == pid) {
            return worker;
        }
    }

    return NULL;
}

static bool
worker_pipe_init(worker_t *worker, worker_pipe_t *pipe)
{
    int err;

    err = pipe2(pipe->fd, O_CLOEXEC);
    if (err) {
        loge("failed to create pipe");
        return false;
    }

    pipe->worker = worker;

    return true;
}

static void
worker_pipe_finish(worker_pipe_t *pipe)
{
    for (int i = 0; i < 2; ++i) {
        if (pipe->fd[i] != -1) {
            close(pipe->fd[i]);
        }
    }
}

static bool
worker_pipe_become_reader(worker_pipe_t *pipe)
{
    int err;

    assert(pipe->read_fd != -1);
    assert(pipe->write_fd != -1);

    err = close(pipe->write_fd);
    if (err == -1) {
        loge("runner failed to close pipe's write fd");
        return false;
    }

    pipe->write_fd = -1;

    return true;
}

static bool
worker_pipe_become_writer(worker_pipe_t *pipe)
{
    int err;

    assert(pipe->read_fd != -1);
    assert(pipe->write_fd != -1);

    err = close(pipe->read_fd);
    if (err == -1) {
        loge("runner failed to close pipe's read fd");
        return false;
    }

    pipe->read_fd = -1;

    return true;
}

static void
worker_pipe_drain_to_fd(worker_pipe_t *pipe, int fd)
{
    char buf[4096];

    for (;;) {
        ssize_t nread = 0;
        ssize_t nwrite = 0;

        if (dispatcher.goto_next_phase)
            return;

        nread = read(pipe->read_fd, buf, sizeof(buf));
        if (nread <= 0)
            return;

        while (nread > 0) {
            if (dispatcher.goto_next_phase)
                return;

            nwrite = write(fd, buf, nread);
            if (nwrite < 0) {
                // Even on write errors, we must continue to drain the worker's
                // pipe. Otherwise the worker may block on a full pipe.
                continue;
            }

            nread -= nwrite;
        }
    }
}
