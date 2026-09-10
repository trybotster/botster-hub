/* Darwin raw evidence and optional totals for a verified fixed baseline. */
#include <errno.h>
#include <inttypes.h>
#include <limits.h>
#include <libproc.h>
#include <mach/mach_time.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/event.h>
#include <sys/proc_info.h>
#include <sys/resource.h>
#include <time.h>
#include <unistd.h>

typedef struct {
    pid_t pid;
    const char *role;
    uint64_t start;
} Root;

typedef struct {
    pid_t pid;
    struct proc_bsdinfo bsd;
    struct rusage_info_v2 usage;
    uint64_t read_begin, read_end;
    size_t root;
    bool selected;
} Row;

typedef struct {
    Row row;
    bool seen, missing, exit_observed;
} Tracked;

typedef struct {
    Root *roots;
    size_t root_count;
    Tracked *tracked;
    size_t tracked_count;
    mach_timebase_info_data_t timebase;
    uint64_t sample, interval_ns;
    bool invalid, turnover, footprint, discovery_incomplete;
} Sampler;

static bool same_process(const Row *a, const Row *b);

static void json_string(const char *text) {
    putchar('"');
    for (const unsigned char *p = (const unsigned char *)text; *p; ++p) {
        if (*p == '"' || *p == '\\') printf("\\%c", *p);
        else if (*p < 32 || *p >= 127) printf("\\u%04x", *p);
        else putchar(*p);
    }
    putchar('"');
}

static void event(Sampler *s, const char *name) {
    printf("{\"version\":1,\"sample\":%" PRIu64 ",\"event\":", s->sample);
    json_string(name);
}

static void error_event(Sampler *s, pid_t pid, const char *operation, int code) {
    s->invalid = true;
    event(s, "sampling_error");
    printf(",\"pid\":%d,\"operation\":", pid);
    json_string(operation);
    printf(",\"errno\":%d}\n", code);
}

/* Mach ticks use the reported rational timebase. Division rounds down. */
static bool ticks_ns(uint64_t ticks, mach_timebase_info_data_t base, uint64_t *ns) {
    if (!base.denom || !base.numer) return false;
    __uint128_t value = (__uint128_t)ticks * base.numer / base.denom;
    if (value > UINT64_MAX) return false;
    *ns = (uint64_t)value;
    return true;
}

static bool stable_identity(pid_t pid, const struct proc_bsdinfo *bsd,
                            const struct rusage_info_v2 *before,
                            const struct rusage_info_v2 *after) {
    return bsd->pbi_pid == (uint32_t)pid && before->ri_proc_start_abstime != 0 &&
           before->ri_proc_start_abstime == after->ri_proc_start_abstime;
}

static bool read_row(Sampler *s, pid_t pid, Row *row) {
    struct rusage_info_v2 before = {0}, after = {0};
    struct proc_bsdinfo bsd = {0};
    row->read_begin = mach_absolute_time();
    if (proc_pid_rusage(pid, RUSAGE_INFO_V2, (rusage_info_t *)&before) != 0) {
        error_event(s, pid, "rusage_before", errno);
        return false;
    }
    errno = 0;
    int bytes = proc_pidinfo(pid, PROC_PIDTBSDINFO, 0, &bsd, sizeof(bsd));
    int metadata_error = bytes == (int)sizeof(bsd) ? 0 : (errno ? errno : EIO);
    if (proc_pid_rusage(pid, RUSAGE_INFO_V2, (rusage_info_t *)&after) != 0) {
        error_event(s, pid, "rusage_after", errno);
        return false;
    }
    row->read_end = mach_absolute_time();
    if (metadata_error) {
        error_event(s, pid, "bsd_metadata", metadata_error);
        return false;
    }
    if (!stable_identity(pid, &bsd, &before, &after)) {
        s->turnover = true;
        error_event(s, pid, "identity_changed_during_read", 0);
        return false;
    }
    row->pid = pid;
    row->bsd = bsd;
    row->usage = after;
    return true;
}

static void *resize(Sampler *s, void *old, size_t count, size_t size) {
    if (count > SIZE_MAX / size) {
        error_event(s, 0, "allocation_size_overflow", EOVERFLOW);
        free(old);
        return NULL;
    }
    void *result = realloc(old, count * size);
    if (!result) {
        error_event(s, 0, "allocation", ENOMEM);
        free(old);
    }
    return result;
}

static void identity_error(Sampler *s, const Row *expected, const Row *observed) {
    s->invalid = s->turnover = true;
    event(s, "sampling_error");
    printf(",\"pid\":%d,\"operation\":\"process_identity_changed\","
           "\"expected_start_ticks\":%" PRIu64 ",\"observed_start_ticks\":%" PRIu64 "}\n",
           expected->pid, expected->usage.ri_proc_start_abstime,
           observed->usage.ri_proc_start_abstime);
}

static bool append_row(Sampler *s, Row **rows, size_t *count, Row row) {
    for (size_t i = 0; i < *count; ++i) {
        if ((*rows)[i].pid != row.pid) continue;
        if (!same_process(&(*rows)[i], &row)) identity_error(s, &(*rows)[i], &row);
        return true;
    }
    Row *next = resize(s, *rows, *count + 1, sizeof(**rows));
    *rows = next;
    if (!next) { *count = 0; return false; }
    row.selected = true;
    next[(*count)++] = row;
    return true;
}

/* libproc returns a PID count. A zero result can also carry errno. */
static bool list_children(Sampler *s, pid_t parent, pid_t **pids, size_t *count) {
    *pids = NULL;
    *count = 0;
    errno = 0;
    int estimate = proc_listchildpids(parent, NULL, 0);
    if (estimate < 0 || (estimate == 0 && errno)) {
        s->discovery_incomplete = true;
        error_event(s, parent, "child_list_size", errno);
        return false;
    }
    if (!estimate) return true;
    size_t capacity = (size_t)estimate + 1;
    for (;;) {
        if (capacity > INT_MAX / sizeof(**pids)) {
            s->discovery_incomplete = true;
            error_event(s, parent, "child_list_size_overflow", EOVERFLOW);
            free(*pids);
            *pids = NULL;
            return false;
        }
        *pids = resize(s, *pids, capacity, sizeof(**pids));
        if (!*pids) return false;
        errno = 0;
        int found = proc_listchildpids(parent, *pids, (int)(capacity * sizeof(**pids)));
        if (found < 0 || (found == 0 && errno)) {
            s->discovery_incomplete = true;
            error_event(s, parent, "child_list", errno);
            free(*pids);
            *pids = NULL;
            return false;
        }
        if ((size_t)found < capacity) {
            *count = (size_t)found;
            return true;
        }
        capacity *= 2;
    }
}

static bool collect_children(Sampler *s, Row **rows, size_t *count, size_t index) {
    Row parent = (*rows)[index], before = {0}, after = {0};
    if (!read_row(s, parent.pid, &before)) {
        s->discovery_incomplete = true;
        return true;
    }
    if (!same_process(&parent, &before)) {
        identity_error(s, &parent, &before);
        s->discovery_incomplete = true;
        return true;
    }
    pid_t *pids = NULL;
    size_t child_count = 0;
    if (!list_children(s, parent.pid, &pids, &child_count)) return true;
    Row *children = NULL;
    if (child_count) {
        children = resize(s, NULL, child_count, sizeof(*children));
        if (!children) { free(pids); return false; }
    }
    size_t accepted = 0;
    for (size_t i = 0; i < child_count; ++i) {
        Row child = {0};
        if (pids[i] <= 0 || !read_row(s, pids[i], &child)) {
            s->discovery_incomplete = true;
            continue;
        }
        if (child.bsd.pbi_ppid != (uint32_t)parent.pid ||
            child.usage.ri_proc_start_abstime < before.usage.ri_proc_start_abstime) {
            s->discovery_incomplete = s->turnover = true;
            error_event(s, child.pid, "candidate_ancestry_changed", 0);
            continue;
        }
        child.root = parent.root;
        children[accepted++] = child;
    }
    free(pids);
    /* Publish child candidates only after the parent survives all child reads. */
    if (!read_row(s, parent.pid, &after)) {
        s->discovery_incomplete = true;
        free(children);
        return true;
    }
    if (!same_process(&parent, &after)) {
        identity_error(s, &parent, &after);
        s->discovery_incomplete = true;
        free(children);
        return true;
    }
    after.root = parent.root;
    after.selected = true;
    (*rows)[index] = after;
    for (size_t i = 0; i < accepted; ++i) {
        if (!append_row(s, rows, count, children[i])) { free(children); return false; }
    }
    free(children);
    return true;
}

static bool collect(Sampler *s, Row **rows, size_t *count) {
    *rows = NULL;
    *count = 0;
    for (size_t root = 0; root < s->root_count; ++root) {
        Row row = {0};
        if (!read_row(s, s->roots[root].pid, &row)) continue;
        if (s->sample != 0 && row.usage.ri_proc_start_abstime != s->roots[root].start) {
            Row expected = {.pid = row.pid,
                            .usage = {.ri_proc_start_abstime = s->roots[root].start}};
            identity_error(s, &expected, &row);
            continue;
        }
        row.root = root;
        if (!append_row(s, rows, count, row)) return false;
    }
    for (size_t i = 0; i < s->tracked_count; ++i) {
        const Tracked *tracked = &s->tracked[i];
        if (tracked->missing && tracked->exit_observed) continue;
        bool already_read = false;
        for (size_t j = 0; j < *count; ++j)
            if ((*rows)[j].pid == tracked->row.pid) already_read = true;
        if (already_read) continue;
        Row row = {0};
        if (!read_row(s, tracked->row.pid, &row)) continue;
        if (!same_process(&tracked->row, &row)) continue;
        row.root = tracked->row.root;
        if (!append_row(s, rows, count, row)) return false;
    }
    /* Appending children extends this traversal without a global process census. */
    for (size_t i = 0; i < *count; ++i)
        if (!collect_children(s, rows, count, i)) return false;
    return true;
}

static bool same_process(const Row *a, const Row *b) {
    return a->pid == b->pid &&
           a->usage.ri_proc_start_abstime == b->usage.ri_proc_start_abstime;
}

static void identity_fields(const Row *row) {
    printf(",\"pid\":%d,\"ppid\":%u,\"start_ticks\":%" PRIu64,
           row->pid, row->bsd.pbi_ppid, row->usage.ri_proc_start_abstime);
}

static void counter(Sampler *s, const char *name, uint64_t ticks) {
    uint64_t ns;
    printf(",\"%s_ticks\":%" PRIu64 ",\"%s_ns\":", name, ticks, name);
    bool valid = ticks_ns(ticks, s->timebase, &ns);
    if (valid) printf("%" PRIu64, ns);
    else { printf("null"); s->invalid = true; }
    printf(",\"%s_conversion_valid\":%s", name, valid ? "true" : "false");
}

static void emit_row(Sampler *s, const Row *row) {
    event(s, "process");
    identity_fields(row);
    printf(",\"root_pid\":%d,\"role\":", s->roots[row->root].pid);
    json_string(s->roots[row->root].role);
    printf(",\"read_begin_ticks\":%" PRIu64 ",\"read_end_ticks\":%" PRIu64
           ",\"exit_ticks\":%" PRIu64 ",\"bsd_status\":%u",
           row->read_begin, row->read_end, row->usage.ri_proc_exit_abstime,
           row->bsd.pbi_status);
    counter(s, "own_user", row->usage.ri_user_time);
    counter(s, "own_system", row->usage.ri_system_time);
    counter(s, "reaped_child_user", row->usage.ri_child_user_time);
    counter(s, "reaped_child_system", row->usage.ri_child_system_time);
    counter(s, "reaped_child_elapsed", row->usage.ri_child_elapsed_abstime);
    printf(",\"rss_bytes\":%" PRIu64, row->usage.ri_resident_size);
    if (s->footprint) printf(",\"physical_footprint_bytes\":%" PRIu64,
                            row->usage.ri_phys_footprint);
    puts("}");
}

static bool observe(Sampler *s, Row *rows, size_t count) {
    for (size_t i = 0; i < s->tracked_count; ++i) s->tracked[i].seen = false;
    for (size_t root = 0; root < s->root_count; ++root) {
        bool live = false;
        for (size_t i = 0; i < count; ++i) {
            if (rows[i].pid != s->roots[root].pid) continue;
            if (s->sample == 0) s->roots[root].start = rows[i].usage.ri_proc_start_abstime;
            if (rows[i].usage.ri_proc_start_abstime != s->roots[root].start) continue;
            rows[i].root = root;
            rows[i].selected = true;
            live = rows[i].usage.ri_proc_exit_abstime == 0;
        }
        if (!live) {
            s->turnover = true;
            error_event(s, s->roots[root].pid, "owned_root_not_live", 0);
        }
    }
    /* Keep observed descendants after reparenting, using the full identity. */
    for (size_t i = 0; i < count; ++i) {
        for (size_t j = 0; j < s->tracked_count; ++j) {
            if (same_process(&rows[i], &s->tracked[j].row)) {
                rows[i].selected = true;
                rows[i].root = s->tracked[j].row.root;
            }
        }
    }
    bool changed;
    do {
        changed = false;
        for (size_t i = 0; i < count; ++i) {
            if (rows[i].selected) continue;
            for (size_t j = 0; j < count; ++j) {
                if (rows[j].selected && rows[i].bsd.pbi_ppid == (uint32_t)rows[j].pid &&
                    rows[i].usage.ri_proc_start_abstime >= rows[j].usage.ri_proc_start_abstime) {
                    rows[i].selected = true;
                    rows[i].root = rows[j].root;
                    changed = true;
                    break;
                }
            }
        }
    } while (changed);
    for (size_t i = 0; i < count; ++i) {
        Row *row = &rows[i];
        if (!row->selected) continue;
        size_t j;
        for (j = 0; j < s->tracked_count; ++j)
            if (same_process(row, &s->tracked[j].row)) break;
        if (j == s->tracked_count) {
            Tracked *next = resize(s, s->tracked, j + 1, sizeof(*next));
            s->tracked = next;
            if (!next) { s->tracked_count = 0; return false; }
            s->tracked_count++;
            s->tracked[j] = (Tracked){.row = *row};
            if (s->sample != 0) s->turnover = s->invalid = true;
            event(s, "observed_birth");
            identity_fields(row);
            printf(",\"first_sample\":%s}\n", s->sample == 0 ? "true" : "false");
        } else {
            const Row *previous = &s->tracked[j].row;
            if (previous->bsd.pbi_ppid != row->bsd.pbi_ppid) {
                event(s, "observed_reparenting");
                identity_fields(row);
                printf(",\"previous_ppid\":%u}\n", previous->bsd.pbi_ppid);
                s->turnover = s->invalid = true;
            }
            if (previous->usage.ri_child_user_time != row->usage.ri_child_user_time ||
                previous->usage.ri_child_system_time != row->usage.ri_child_system_time) {
                s->turnover = s->invalid = true;
                event(s, "unresolved_reaped_child_accounting");
                identity_fields(row);
                puts("}");
            }
            if (s->tracked[j].missing) {
                event(s, "observed_return");
                identity_fields(row);
                puts("}");
            }
        }
        if (row->usage.ri_proc_exit_abstime && !s->tracked[j].exit_observed) {
            s->turnover = s->invalid = true;
            event(s, "observed_exit");
            identity_fields(row);
            printf(",\"exit_ticks\":%" PRIu64 "}\n", row->usage.ri_proc_exit_abstime);
            s->tracked[j].exit_observed = true;
        }
        s->tracked[j].row = *row;
        s->tracked[j].seen = true;
        s->tracked[j].missing = false;
        emit_row(s, row);
    }
    for (size_t i = 0; i < s->tracked_count; ++i) {
        Tracked *tracked = &s->tracked[i];
        if (tracked->seen || tracked->missing) continue;
        event(s, "observed_disappearance");
        identity_fields(&tracked->row);
        printf(",\"exit_was_observed\":%s}\n", tracked->exit_observed ? "true" : "false");
        tracked->missing = true;
        if (!tracked->exit_observed) s->turnover = s->invalid = true;
    }
    return true;
}

typedef struct {
    Row identity;
    uint64_t attempt;
} ProcessWatch;

typedef struct {
    Sampler *sampler;
    int queue;
    ProcessWatch *watches;
    size_t watch_count;
    uint64_t attempt;
    bool started, failed, dirty, already_watched;
} StableBaseline;

typedef struct {
    uint64_t begin, end, rss;
} StableSample;

static void stable_error(StableBaseline *stable, pid_t pid, const char *operation, int code) {
    error_event(stable->sampler, pid, operation, code);
    stable->dirty = true;
    if (stable->started) stable->failed = true;
}

/* A zero-time call that returns no events establishes the end of each drain. */
static bool stable_drain(StableBaseline *stable) {
    const struct timespec zero = {0};
    struct kevent batch[32];
    for (;;) {
        int count = kevent(stable->queue, NULL, 0, batch, 32, &zero);
        if (count < 0) {
            stable_error(stable, 0, "lifecycle_event_drain", errno);
            return false;
        }
        if (!count) return true;
        for (int i = 0; i < count; ++i) {
            const struct kevent *item = &batch[i];
            stable->dirty = true;
            if (stable->started) stable->failed = true;
            event(stable->sampler, "lifecycle_event");
            printf(",\"pid\":%" PRIuPTR ",\"filter\":%d,\"flags\":%u,\"fflags\":%u,"
                   "\"data\":%" PRIdPTR ",\"read_ticks\":%" PRIu64 ",\"baseline_active\":%s}\n",
                   item->ident, item->filter, item->flags, item->fflags, item->data,
                   mach_absolute_time(), stable->started ? "true" : "false");
        }
    }
}

static bool stable_watch(StableBaseline *stable, Row *row) {
    Sampler *s = stable->sampler;
    if (row->usage.ri_proc_exit_abstime) {
        stable_error(stable, row->pid, "participant_not_live", 0);
        return false;
    }
    for (size_t i = 0; i < stable->watch_count; ++i) {
        ProcessWatch *watch = &stable->watches[i];
        if (!same_process(row, &watch->identity)) continue;
        if (watch->attempt >= stable->attempt) stable->already_watched = false;
        return true;
    }
    if (stable->started) {
        stable_error(stable, row->pid, "unwatched_baseline_participant", 0);
        return false;
    }
    stable->already_watched = false;
    struct kevent change, receipt;
    const struct timespec zero = {0};
    EV_SET(&change, (uintptr_t)row->pid, EVFILT_PROC,
           EV_ADD | EV_ENABLE | EV_CLEAR | EV_RECEIPT,
           NOTE_FORK | NOTE_EXEC | NOTE_EXIT, 0, NULL);
    memset(&receipt, 0, sizeof(receipt));
    int count = kevent(stable->queue, &change, 1, &receipt, 1, &zero);
    int registration_error = errno;
    bool registered = count == 1 && receipt.ident == (uintptr_t)row->pid &&
                      receipt.filter == EVFILT_PROC && (receipt.flags & EV_ERROR) &&
                      receipt.data == 0;
    event(s, "watch_registration");
    identity_fields(row);
    printf(",\"attempt\":%" PRIu64 ",\"receipt_count\":%d,\"receipt_flags\":%u,"
           "\"receipt_data\":%" PRIdPTR ",\"registered\":%s}\n",
           stable->attempt, count, receipt.flags, receipt.data, registered ? "true" : "false");
    if (!registered) {
        stable_error(stable, row->pid, "watch_registration", count < 0 ? registration_error : (int)receipt.data);
        return false;
    }
    Row after = {0};
    if (!read_row(s, row->pid, &after)) return false;
    if (!same_process(row, &after)) {
        identity_error(s, row, &after);
        return false;
    }
    ProcessWatch *next = resize(s, stable->watches, stable->watch_count + 1, sizeof(*next));
    stable->watches = next;
    if (!next) { stable->watch_count = 0; return false; }
    next[stable->watch_count++] = (ProcessWatch){.identity = after, .attempt = stable->attempt};
    return true;
}

static bool stable_append(StableBaseline *stable, Row **rows, size_t *count, Row row) {
    if (!stable_watch(stable, &row)) return false;
    return append_row(stable->sampler, rows, count, row);
}

static bool stable_discover(StableBaseline *stable, Row **rows, size_t *count) {
    Sampler *s = stable->sampler;
    *rows = NULL;
    *count = 0;
    for (size_t root = 0; root < s->root_count; ++root) {
        Row row = {0};
        if (!read_row(s, s->roots[root].pid, &row)) return false;
        if (!s->roots[root].start) s->roots[root].start = row.usage.ri_proc_start_abstime;
        if (s->roots[root].start != row.usage.ri_proc_start_abstime) {
            stable_error(stable, row.pid, "root_identity_changed", 0);
            return false;
        }
        row.root = root;
        if (!stable_append(stable, rows, count, row)) return false;
    }
    for (size_t index = 0; index < *count; ++index) {
        Row parent = (*rows)[index], before = {0}, after = {0};
        if (!read_row(s, parent.pid, &before)) return false;
        if (!same_process(&parent, &before)) {
            identity_error(s, &parent, &before);
            return false;
        }
        pid_t *pids = NULL;
        size_t child_count = 0;
        if (!list_children(s, parent.pid, &pids, &child_count)) return false;
        Row *children = NULL;
        if (child_count) {
            children = resize(s, NULL, child_count, sizeof(*children));
            if (!children) { free(pids); return false; }
        }
        bool valid = true;
        for (size_t i = 0; i < child_count; ++i) {
            children[i] = (Row){0};
            if (pids[i] <= 0 || !read_row(s, pids[i], &children[i])) { valid = false; break; }
            if (children[i].bsd.pbi_ppid != (uint32_t)parent.pid ||
                children[i].usage.ri_proc_start_abstime < before.usage.ri_proc_start_abstime) {
                stable_error(stable, children[i].pid, "candidate_ancestry_changed", 0);
                valid = false;
                break;
            }
            children[i].root = parent.root;
        }
        free(pids);
        if (valid && !read_row(s, parent.pid, &after)) valid = false;
        if (valid && !same_process(&parent, &after)) {
            identity_error(s, &parent, &after);
            valid = false;
        }
        if (!valid) { free(children); return false; }
        after.root = parent.root;
        after.selected = true;
        (*rows)[index] = after;
        for (size_t i = 0; i < child_count; ++i) {
            if (!stable_append(stable, rows, count, children[i])) { free(children); return false; }
        }
        free(children);
    }
    return !s->invalid;
}

static bool reaped_equal(const struct rusage_info_v2 *a, const struct rusage_info_v2 *b) {
    return a->ri_child_user_time == b->ri_child_user_time &&
           a->ri_child_system_time == b->ri_child_system_time &&
           a->ri_child_pkg_idle_wkups == b->ri_child_pkg_idle_wkups &&
           a->ri_child_interrupt_wkups == b->ri_child_interrupt_wkups &&
           a->ri_child_pageins == b->ri_child_pageins &&
           a->ri_child_elapsed_abstime == b->ri_child_elapsed_abstime;
}

static bool stable_same_set(StableBaseline *stable, const Row *expected, size_t expected_count,
                            const Row *actual, size_t actual_count) {
    if (expected_count != actual_count) {
        stable_error(stable, 0, "participant_set_changed", 0);
        return false;
    }
    for (size_t i = 0; i < expected_count; ++i) {
        const Row *match = NULL;
        for (size_t j = 0; j < actual_count; ++j)
            if (same_process(&expected[i], &actual[j])) match = &actual[j];
        if (!match || match->bsd.pbi_ppid != expected[i].bsd.pbi_ppid ||
            match->usage.ri_proc_exit_abstime || !reaped_equal(&expected[i].usage, &match->usage)) {
            stable_error(stable, expected[i].pid, "participant_endpoint_changed", 0);
            return false;
        }
    }
    return true;
}

static bool stable_rss(Sampler *s, const Row *rows, size_t count, uint64_t *sum) {
    *sum = 0;
    for (size_t i = 0; i < count; ++i) {
        if (rows[i].usage.ri_resident_size > UINT64_MAX - *sum) {
            error_event(s, rows[i].pid, "rss_sum_overflow", EOVERFLOW);
            return false;
        }
        *sum += rows[i].usage.ri_resident_size;
    }
    return true;
}

static bool stable_cpu(Sampler *s, const Row *first, const Row *last, size_t count,
                       uint64_t *sum_ticks, uint64_t *sum_ns, bool emit) {
    *sum_ticks = 0;
    for (size_t i = 0; i < count; ++i) {
        const Row *end = NULL;
        for (size_t j = 0; j < count; ++j)
            if (same_process(&first[i], &last[j])) end = &last[j];
        if (!end || end->usage.ri_user_time < first[i].usage.ri_user_time ||
            end->usage.ri_system_time < first[i].usage.ri_system_time) {
            error_event(s, first[i].pid, "own_cpu_counter_decreased", 0);
            return false;
        }
        uint64_t user = end->usage.ri_user_time - first[i].usage.ri_user_time;
        uint64_t system = end->usage.ri_system_time - first[i].usage.ri_system_time;
        uint64_t ns;
        if (user > UINT64_MAX - system || user + system > UINT64_MAX - *sum_ticks ||
            !ticks_ns(user + system, s->timebase, &ns)) {
            error_event(s, first[i].pid, "own_cpu_delta_overflow", EOVERFLOW);
            return false;
        }
        *sum_ticks += user + system;
        if (emit) {
            event(s, "stable_own_cpu_delta");
            identity_fields(&first[i]);
            printf(",\"own_user_delta_ticks\":%" PRIu64 ",\"own_system_delta_ticks\":%" PRIu64
                   ",\"own_cpu_delta_ns\":%" PRIu64 ",\"baseline_read_begin_ticks\":%" PRIu64
                   ",\"baseline_read_end_ticks\":%" PRIu64 ",\"final_read_begin_ticks\":%" PRIu64
                   ",\"final_read_end_ticks\":%" PRIu64 "}\n",
                   user, system, ns, first[i].read_begin, first[i].read_end,
                   end->read_begin, end->read_end);
        }
    }
    if (!ticks_ns(*sum_ticks, s->timebase, sum_ns)) {
        error_event(s, 0, "own_cpu_total_overflow", EOVERFLOW);
        return false;
    }
    return true;
}

static int run_stable(Sampler *s, uint64_t samples, uint64_t setup_attempts) {
    StableBaseline stable = {.sampler = s, .queue = -1};
    Row *participants = NULL, *first = NULL, *last = NULL;
    size_t participant_count = 0, first_count = 0, last_count = 0;
    StableSample *measurements = NULL;
    uint64_t completed = 0, cpu_ticks = 0, cpu_ns = 0, final_drain_ticks = 0;
    bool ready = false;
    event(s, "configuration");
    printf(",\"mode\":\"stable_baseline\",\"timebase_numer\":%u,\"timebase_denom\":%u,"
           "\"interval_ns\":%" PRIu64 ",\"requested_samples\":%" PRIu64
           ",\"max_setup_attempts\":%" PRIu64 ",\"cpu_raw_unit\":\"mach_ticks\","
           "\"coverage\":\"current_rooted_baseline_set\",\"historical_descendants_complete\":false,"
           "\"known_orphans_require_explicit_roots\":true,\"setup_passes_establish_historical_completeness\":false,"
           "\"snapshot_atomic\":false,\"rss_peak\":false,\"roots\":[",
           s->timebase.numer, s->timebase.denom, s->interval_ns, samples, setup_attempts);
    for (size_t i = 0; i < s->root_count; ++i) {
        if (i) putchar(',');
        printf("{\"pid\":%d,\"role\":", s->roots[i].pid);
        json_string(s->roots[i].role);
        putchar('}');
    }
    puts("]}");
    stable.queue = kqueue();
    if (stable.queue < 0) { stable_error(&stable, 0, "kqueue_create", errno); goto finish; }
    for (stable.attempt = 1; stable.attempt <= setup_attempts; ++stable.attempt) {
        if (!stable_drain(&stable)) goto finish;
        stable.dirty = false;
        stable.already_watched = true;
        s->invalid = s->discovery_incomplete = false;
        free(participants);
        participants = NULL;
        bool complete = stable_discover(&stable, &participants, &participant_count);
        bool drained = stable_drain(&stable);
        ready = complete && drained && !stable.dirty && !s->invalid && stable.already_watched;
        event(s, "stable_setup_attempt");
        printf(",\"attempt\":%" PRIu64 ",\"participants\":%zu,\"already_watched\":%s,"
               "\"quiet_complete_pass\":%s}\n", stable.attempt, participant_count,
               stable.already_watched ? "true" : "false", ready ? "true" : "false");
        if (ready) break;
        if (!drained) goto finish;
        if (stable.attempt == setup_attempts) break;
    }
    if (!ready) { stable_error(&stable, 0, "unable_to_stabilize", 0); goto finish; }
    if (samples > SIZE_MAX / sizeof(*measurements)) {
        stable_error(&stable, 0, "sample_storage_overflow", EOVERFLOW);
        goto finish;
    }
    measurements = resize(s, NULL, (size_t)samples, sizeof(*measurements));
    if (!measurements) goto finish;
    stable.started = true;
    for (s->sample = 0; s->sample < samples; ++s->sample) {
        if (!stable_drain(&stable)) break;
        Row *rows = NULL;
        size_t count = 0;
        StableSample measurement = {.begin = mach_absolute_time()};
        bool complete = stable_discover(&stable, &rows, &count);
        measurement.end = mach_absolute_time();
        bool same = complete && stable_same_set(&stable, participants, participant_count, rows, count);
        if (!same || !stable_rss(s, rows, count, &measurement.rss) || s->invalid) stable.failed = true;
        measurements[completed++] = measurement;
        for (size_t i = 0; i < count; ++i) emit_row(s, &rows[i]);
        if (s->sample == 0) { first = rows; first_count = count; }
        else { free(last); last = rows; last_count = count; }
        event(s, "stable_sample_complete");
        printf(",\"read_begin_ticks\":%" PRIu64 ",\"read_end_ticks\":%" PRIu64
               ",\"participants\":%zu,\"interval_verified\":false}\n",
               measurement.begin, measurement.end, count);
        if (fflush(stdout) || ferror(stdout)) { stable.failed = true; break; }
        if (!complete || s->sample == samples - 1) break;
        struct timespec delay = {(time_t)(s->interval_ns / 1000000000),
                                 (long)(s->interval_ns % 1000000000)};
        while (nanosleep(&delay, &delay) < 0) {
            if (errno != EINTR) { stable_error(&stable, 0, "nanosleep", errno); break; }
        }
    }
    if (completed != samples || first_count != participant_count || last_count != participant_count)
        stable.failed = true;
    if (!stable.failed && !stable_cpu(s, first, last, participant_count, &cpu_ticks, &cpu_ns, false))
        stable.failed = true;
    /* No counter, identity, parent, or enumeration read follows this final drain. */
    if (!stable_drain(&stable)) stable.failed = true;
    final_drain_ticks = mach_absolute_time();
    if (s->invalid) stable.failed = true;
    if (!stable.failed) {
        stable_cpu(s, first, last, participant_count, &cpu_ticks, &cpu_ns, true);
        for (uint64_t i = 0; i < completed; ++i) {
            event(s, "stable_rss_sample");
            printf(",\"sample_index\":%" PRIu64 ",\"read_begin_ticks\":%" PRIu64
                   ",\"read_end_ticks\":%" PRIu64 ",\"rss_sum_bytes\":%" PRIu64
                   ",\"atomic\":false,\"peak\":false}\n",
                   i, measurements[i].begin, measurements[i].end, measurements[i].rss);
        }
    }
finish:
    event(s, "stable_summary");
    bool valid = ready && stable.started && !stable.failed && !s->invalid && completed == samples;
    printf(",\"interval_valid\":%s,\"participants\":%zu,\"completed_samples\":%" PRIu64
           ",\"final_drain_ticks\":%" PRIu64 ",\"own_cpu_delta_ticks\":",
           valid ? "true" : "false", participant_count, completed, final_drain_ticks);
    if (valid) printf("%" PRIu64, cpu_ticks); else printf("null");
    printf(",\"own_cpu_delta_ns\":");
    if (valid) printf("%" PRIu64, cpu_ns); else printf("null");
    if (valid) printf(",\"baseline_collection_begin_ticks\":%" PRIu64
                      ",\"baseline_collection_end_ticks\":%" PRIu64
                      ",\"final_collection_begin_ticks\":%" PRIu64
                      ",\"final_collection_end_ticks\":%" PRIu64,
                      measurements[0].begin, measurements[0].end,
                      measurements[completed - 1].begin, measurements[completed - 1].end);
    puts(",\"atomic_interval\":false,\"rss_peak\":false,\"historical_descendants_complete\":false}");
    if (stable.queue >= 0) close(stable.queue);
    free(stable.watches);
    free(participants);
    free(first);
    free(last);
    free(measurements);
    free(s->roots);
    return (!valid || fflush(stdout) || ferror(stdout)) ? 2 : 0;
}

static bool number(const char *text, uint64_t *value, const char **end) {
    if (*text < '0' || *text > '9') return false;
    char *tail;
    errno = 0;
    unsigned long long parsed = strtoull(text, &tail, 10);
    if (errno || !parsed) return false;
    *value = parsed;
    *end = tail;
    return true;
}

int main(int argc, char **argv) {
    Sampler s = {0};
    uint64_t samples = 0;
    uint64_t setup_attempts = 8;
    bool stable_mode = false, setup_option = false;
    s.roots = calloc((size_t)argc, sizeof(*s.roots));
    if (!s.roots) return 2;
    for (int i = 1; i < argc; ++i) {
        if (!strcmp(argv[i], "--physical-footprint")) { s.footprint = true; continue; }
        if (!strcmp(argv[i], "--stable-baseline")) { stable_mode = true; continue; }
        if (i + 1 >= argc) goto usage;
        const char *option = argv[i++], *end;
        uint64_t value;
        if (!number(argv[i], &value, &end)) goto usage;
        if (!strcmp(option, "--owned-root")) {
            if (value > INT_MAX || *end != ':' || !end[1]) goto usage;
            /* Roles are ASCII identifiers, not executable names. */
            for (const char *p = end + 1; *p; ++p)
                if (!((*p >= 'a' && *p <= 'z') || (*p >= 'A' && *p <= 'Z') ||
                      (*p >= '0' && *p <= '9') || strchr("_.-", *p))) goto usage;
            for (size_t j = 0; j < s.root_count; ++j)
                if (s.roots[j].pid == (pid_t)value) goto usage;
            s.roots[s.root_count++] = (Root){.pid = (pid_t)value, .role = end + 1};
        } else if (!strcmp(option, "--setup-attempts") && !*end) {
            setup_attempts = value;
            setup_option = true;
        } else if (!strcmp(option, "--samples") && !*end) samples = value;
        else if (!strcmp(option, "--interval-ms") && !*end && value <= UINT64_MAX / 1000000)
            s.interval_ns = value * 1000000;
        else goto usage;
    }
    if (!s.root_count || !samples || !s.interval_ns) goto usage;
    if ((setup_option && !stable_mode) || (stable_mode && samples < 2)) goto usage;
    if (mach_timebase_info(&s.timebase) != KERN_SUCCESS ||
        !s.timebase.numer || !s.timebase.denom) {
        error_event(&s, 0, "mach_timebase", 0);
        free(s.roots);
        return 2;
    }
    if (stable_mode) return run_stable(&s, samples, setup_attempts);
    event(&s, "configuration");
    printf(",\"timebase_numer\":%u,\"timebase_denom\":%u,\"interval_ns\":%" PRIu64
           ",\"requested_samples\":%" PRIu64
           ",\"cpu_raw_unit\":\"mach_ticks\",\"lifecycle_accounting_complete\":false"
           ",\"snapshot_atomic\":false,\"interval_mode\":\"delay_after_sample\""
           ",\"root_must_remain_live_through_final_sample\":true,\"roots\":[",
           s.timebase.numer, s.timebase.denom, s.interval_ns, samples);
    for (size_t i = 0; i < s.root_count; ++i) {
        if (i) putchar(',');
        printf("{\"pid\":%d,\"role\":", s.roots[i].pid);
        json_string(s.roots[i].role);
        putchar('}');
    }
    puts("]}");
    for (s.sample = 0; s.sample < samples; ++s.sample) {
        Row *rows = NULL;
        size_t count = 0;
        uint64_t begin = mach_absolute_time();
        bool ok = collect(&s, &rows, &count) && observe(&s, rows, count);
        free(rows);
        event(&s, "sample_end");
        printf(",\"begin_ticks\":%" PRIu64 ",\"end_ticks\":%" PRIu64
               ",\"observations_valid\":%s,\"unresolved_turnover\":%s,"
               "\"ancestry_discovery_complete\":%s}\n",
               begin, mach_absolute_time(), s.invalid ? "false" : "true",
               s.turnover ? "true" : "false", s.discovery_incomplete ? "false" : "true");
        if (fflush(stdout) || ferror(stdout)) { s.invalid = true; break; }
        if (!ok || s.sample == samples - 1) break;
        struct timespec delay = {(time_t)(s.interval_ns / 1000000000),
                                 (long)(s.interval_ns % 1000000000)};
        while (nanosleep(&delay, &delay) < 0) {
            if (errno != EINTR) { error_event(&s, 0, "nanosleep", errno); break; }
        }
    }
    event(&s, "summary");
    printf(",\"observations_valid\":%s,\"unresolved_turnover\":%s,"
           "\"ancestry_discovery_complete\":%s,"
           "\"lifecycle_accounting_valid\":false,\"polling_can_miss_processes\":true}\n",
           s.invalid ? "false" : "true", s.turnover ? "true" : "false",
           s.discovery_incomplete ? "false" : "true");
    free(s.tracked);
    free(s.roots);
    return (s.invalid || fflush(stdout) || ferror(stdout)) ? 2 : 0;
usage:
    fputs("Usage: measure-processes --owned-root PID:ROLE [--owned-root PID:ROLE ...] "
          "--interval-ms N --samples N [--physical-footprint] "
          "[--stable-baseline [--setup-attempts N]]\n", stderr);
    free(s.roots);
    return 2;
}
