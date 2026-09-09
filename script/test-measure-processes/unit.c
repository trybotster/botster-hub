#define main sampler_main
#define proc_pid_rusage test_proc_pid_rusage
#define proc_pidinfo test_proc_pidinfo
#define proc_listchildpids test_proc_listchildpids
#include "../measure-processes.c"
#undef main
#undef proc_pid_rusage
#undef proc_pidinfo
#undef proc_listchildpids
#include <assert.h>

static unsigned read_count;
static bool change_identity, short_metadata;
static Row fake_rows[4];
static size_t fake_count;
static pid_t denied_pid;
static unsigned denied_usage_reads;
static unsigned denied_metadata_reads, child_buffer_calls;
static int child_list_errno;
static bool small_estimate, parent_enumerated;
static pid_t reused_parent;

int test_proc_listchildpids(pid_t parent, void *buffer, int bytes) {
    if (child_list_errno) { errno = child_list_errno; return 0; }
    size_t count = 0;
    for (size_t i = 0; i < fake_count; ++i) {
        if (fake_rows[i].bsd.pbi_ppid != (uint32_t)parent) continue;
        if (buffer && count < (size_t)bytes / sizeof(pid_t))
            ((pid_t *)buffer)[count] = fake_rows[i].pid;
        count++;
    }
    if (!buffer) return small_estimate && count ? 1 : (int)count;
    child_buffer_calls++;
    if (parent == reused_parent) parent_enumerated = true;
    size_t capacity = (size_t)bytes / sizeof(pid_t);
    return (int)(count < capacity ? count : capacity);
}

int test_proc_pid_rusage(int pid, int flavor, rusage_info_t *buffer) {
    assert(flavor == RUSAGE_INFO_V2);
    if (pid == denied_pid) {
        denied_usage_reads++;
        errno = EPERM;
        return -1;
    }
    struct rusage_info_v2 *usage = (struct rusage_info_v2 *)buffer;
    for (size_t i = 0; i < fake_count; ++i) {
        if (fake_rows[i].pid == pid) {
            *usage = fake_rows[i].usage;
            if (pid == reused_parent && parent_enumerated) usage->ri_proc_start_abstime += 1000;
            return 0;
        }
    }
    memset(usage, 0, sizeof(*usage));
    usage->ri_proc_start_abstime = change_identity && read_count ? 200 : 100;
    usage->ri_user_time = 3;
    read_count++;
    return 0;
}

int test_proc_pidinfo(int pid, int flavor, uint64_t arg, void *buffer, int size) {
    (void)arg;
    assert(flavor == PROC_PIDTBSDINFO);
    assert(size == sizeof(struct proc_bsdinfo));
    if (pid == denied_pid) { denied_metadata_reads++; errno = EPERM; return 0; }
    struct proc_bsdinfo *bsd = buffer;
    for (size_t i = 0; i < fake_count; ++i) {
        if (fake_rows[i].pid == pid) {
            *bsd = fake_rows[i].bsd;
            return size;
        }
    }
    memset(bsd, 0, sizeof(*bsd));
    bsd->pbi_pid = (uint32_t)pid;
    return short_metadata ? 1 : size;
}

static Row fixture(pid_t pid, uint32_t ppid, uint64_t start) {
    return (Row){.pid = pid, .bsd = {.pbi_pid = (uint32_t)pid, .pbi_ppid = ppid},
                 .usage = {.ri_proc_start_abstime = start}};
}

int main(void) {
    uint64_t ns;
    assert(ticks_ns(3, (mach_timebase_info_data_t){125, 3}, &ns) && ns == 125);
    assert(ticks_ns(1, (mach_timebase_info_data_t){125, 3}, &ns) && ns == 41);
    assert(ticks_ns(UINT64_MAX, (mach_timebase_info_data_t){1, 1}, &ns) && ns == UINT64_MAX);
    assert(!ticks_ns(UINT64_MAX, (mach_timebase_info_data_t){125, 3}, &ns));
    assert(!ticks_ns(1, (mach_timebase_info_data_t){1, 0}, &ns));
    assert(!ticks_ns(1, (mach_timebase_info_data_t){0, 1}, &ns));

    struct rusage_info_v2 before = {.ri_proc_start_abstime = 10}, after = before;
    struct proc_bsdinfo bsd = {.pbi_pid = 42};
    after.ri_uuid[0] = 1;
    assert(stable_identity(42, &bsd, &before, &after));
    after.ri_proc_start_abstime++;
    assert(!stable_identity(42, &bsd, &before, &after));
    after = before;
    assert(!stable_identity(43, &bsd, &before, &after));
    before.ri_proc_start_abstime = after.ri_proc_start_abstime = 0;
    assert(!stable_identity(42, &bsd, &before, &after));

    Sampler s = {.timebase = {125, 3}};
    Row row = {0};
    assert(read_row(&s, 42, &row) && read_count == 2);
    assert(row.usage.ri_user_time == 3 && !s.invalid);
    read_count = 0;
    change_identity = true;
    assert(!read_row(&s, 42, &row) && s.invalid && s.turnover);
    change_identity = false;
    short_metadata = true;
    assert(!read_row(&s, 42, &row));

    Root root = {.pid = 10, .role = "fixture"};
    s = (Sampler){.roots = &root, .root_count = 1, .timebase = {125, 3}};
    Row initial[] = {fixture(30, 20, 300), fixture(10, 1, 100), fixture(20, 10, 200)};
    assert(observe(&s, initial, 3));
    assert(s.tracked_count == 3 && !s.invalid);
    s.sample++;
    Row reparented[] = {fixture(10, 1, 100), fixture(20, 1, 200), fixture(30, 20, 300)};
    assert(observe(&s, reparented, 3));
    assert(reparented[1].selected && reparented[2].selected && s.turnover && s.invalid);
    s.sample++;
    Row reused[] = {fixture(10, 1, 100), fixture(20, 1, 400)};
    assert(observe(&s, reused, 2));
    assert(!reused[1].selected && s.tracked_count == 3);
    free(s.tracked);

    root.start = 0;
    s = (Sampler){.roots = &root, .root_count = 1, .timebase = {125, 3}};
    row = fixture(10, 1, 100);
    assert(observe(&s, &row, 1));
    s.sample++;
    row = fixture(10, 1, 100);
    row.usage.ri_child_user_time = 1;
    assert(observe(&s, &row, 1) && s.invalid && s.turnover);
    s.sample++;
    row = fixture(10, 1, 500);
    assert(observe(&s, &row, 1) && !row.selected);
    free(s.tracked);

    root.start = 0;
    s = (Sampler){.roots = &root, .root_count = 1, .timebase = {125, 3}};
    Row live[] = {fixture(10, 1, 100), fixture(20, 10, 200)};
    assert(observe(&s, live, 2));
    s.sample++;
    Row exited[] = {fixture(10, 1, 100), fixture(20, 10, 200)};
    exited[1].usage.ri_proc_exit_abstime = 250;
    assert(observe(&s, exited, 2));
    s.sample++;
    row = fixture(10, 1, 100);
    assert(observe(&s, &row, 1) && s.invalid && s.turnover);
    free(s.tracked);

    /* Discovery must not query an inaccessible unrelated process. */
    short_metadata = false;
    denied_pid = 99;
    fake_rows[0] = fixture(10, 1, 100);
    fake_rows[1] = fixture(20, 10, 200);
    fake_rows[2] = fixture(99, 1, 300);
    fake_count = 3;
    root.start = 0;
    s = (Sampler){.roots = &root, .root_count = 1, .timebase = {125, 3}};
    Row *collected = NULL;
    size_t count;
    assert(collect(&s, &collected, &count) && count == 2);
    assert(!s.discovery_incomplete && !s.invalid && denied_usage_reads == 0);
    assert(denied_metadata_reads == 0);
    assert(observe(&s, collected, count) && !s.invalid);
    free(collected);

    /* A reused tracked PID outside the roots must not retain ownership. */
    s.sample++;
    fake_rows[1] = fixture(20, 1, 400);
    assert(collect(&s, &collected, &count) && count == 1);
    assert(collected[0].pid == 10 && denied_usage_reads == 0);
    assert(observe(&s, collected, count) && s.invalid && s.turnover);
    free(collected);
    free(s.tracked);

    /* Empty child lists ignore stale errno; zero with a new errno is an error. */
    s = (Sampler){0};
    pid_t *children = NULL;
    size_t children_count = 1;
    errno = ERANGE;
    assert(list_children(&s, 123, &children, &children_count));
    assert(children_count == 0 && children == NULL && !s.invalid);
    child_list_errno = EPERM;
    assert(!list_children(&s, 123, &children, &children_count));
    assert(s.invalid && s.discovery_incomplete);
    child_list_errno = 0;

    /* A full child buffer must grow and preserve every returned PID. */
    s = (Sampler){0};
    fake_count = 4;
    fake_rows[0] = fixture(10, 1, 100);
    fake_rows[1] = fixture(20, 10, 200);
    fake_rows[2] = fixture(30, 10, 300);
    fake_rows[3] = fixture(40, 10, 400);
    small_estimate = true;
    child_buffer_calls = 0;
    assert(list_children(&s, 10, &children, &children_count));
    assert(children_count == 3 && child_buffer_calls == 2);
    assert(children[0] == 20 && children[1] == 30 && children[2] == 40);
    free(children);
    small_estimate = false;

    /* Parent reuse during enumeration must reject all provisional children. */
    fake_count = 2;
    reused_parent = 10;
    parent_enumerated = false;
    root.start = 0;
    s = (Sampler){.roots = &root, .root_count = 1, .timebase = {125, 3}};
    assert(collect(&s, &collected, &count) && count == 1);
    assert(collected[0].pid == 10 && s.invalid && s.turnover && s.discovery_incomplete);
    free(collected);
    reused_parent = 0;

    /* New observed descendants require later lifecycle reconciliation. */
    fake_count = 1;
    root.start = 0;
    s = (Sampler){.roots = &root, .root_count = 1, .timebase = {125, 3}};
    assert(collect(&s, &collected, &count) && count == 1);
    assert(observe(&s, collected, count) && !s.invalid);
    free(collected);
    s.sample++;
    fake_count = 2;
    fake_rows[1] = fixture(20, 10, 400);
    assert(collect(&s, &collected, &count) && count == 2);
    assert(observe(&s, collected, count) && s.invalid && s.turnover);
    free(collected);
    free(s.tracked);
    fputs("Sampler unit checks passed.\n", stderr);
    return 0;
}
