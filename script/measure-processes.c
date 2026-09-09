/* Darwin raw process evidence. This program does not calculate tree totals. */
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
    bool invalid, turnover, footprint;
} Sampler;

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

static bool collect(Sampler *s, Row **rows, size_t *count) {
    int estimate = proc_listallpids(NULL, 0);
    if (estimate <= 0) {
        error_event(s, 0, "list_size", errno);
        return false;
    }
    size_t capacity = (size_t)estimate + 128;
    pid_t *pids = NULL;
    int found;
    for (;;) {
        if (capacity > INT_MAX / sizeof(*pids)) {
            error_event(s, 0, "list_size_overflow", EOVERFLOW);
            free(pids);
            return false;
        }
        pids = resize(s, pids, capacity, sizeof(*pids));
        if (!pids) return false;
        found = proc_listallpids(pids, (int)(capacity * sizeof(*pids)));
        if (found <= 0) {
            error_event(s, 0, "list_pids", errno);
            free(pids);
            return false;
        }
        if ((size_t)found < capacity) break;
        capacity *= 2;
    }
    *rows = resize(s, NULL, (size_t)found, sizeof(**rows));
    if (!*rows) { free(pids); return false; }
    *count = 0;
    for (int i = 0; i < found; ++i) {
        if (pids[i] <= 0) continue;
        Row row = {0};
        if (read_row(s, pids[i], &row)) (*rows)[(*count)++] = row;
    }
    free(pids);
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
    s.roots = calloc((size_t)argc, sizeof(*s.roots));
    if (!s.roots) return 2;
    for (int i = 1; i < argc; ++i) {
        if (!strcmp(argv[i], "--physical-footprint")) { s.footprint = true; continue; }
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
        } else if (!strcmp(option, "--samples") && !*end) samples = value;
        else if (!strcmp(option, "--interval-ms") && !*end && value <= UINT64_MAX / 1000000)
            s.interval_ns = value * 1000000;
        else goto usage;
    }
    if (!s.root_count || !samples || !s.interval_ns) goto usage;
    if (mach_timebase_info(&s.timebase) != KERN_SUCCESS ||
        !s.timebase.numer || !s.timebase.denom) {
        error_event(&s, 0, "mach_timebase", 0);
        free(s.roots);
        return 2;
    }
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
               ",\"observations_valid\":%s,\"unresolved_turnover\":%s}\n",
               begin, mach_absolute_time(), s.invalid ? "false" : "true",
               s.turnover ? "true" : "false");
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
           "\"lifecycle_accounting_valid\":false,\"polling_can_miss_processes\":true}\n",
           s.invalid ? "false" : "true", s.turnover ? "true" : "false");
    free(s.tracked);
    free(s.roots);
    return (s.invalid || fflush(stdout) || ferror(stdout)) ? 2 : 0;
usage:
    fputs("Usage: measure-processes --owned-root PID:ROLE [--owned-root PID:ROLE ...] "
          "--interval-ms N --samples N [--physical-footprint]\n", stderr);
    free(s.roots);
    return 2;
}
