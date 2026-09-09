#define main sampler_main
#define proc_pid_rusage test_proc_pid_rusage
#define proc_pidinfo test_proc_pidinfo
#include "../measure-processes.c"
#undef main
#undef proc_pid_rusage
#undef proc_pidinfo
#include <assert.h>

static unsigned read_count;
static bool change_identity, short_metadata;

int test_proc_pid_rusage(int pid, int flavor, rusage_info_t *buffer) {
    (void)pid;
    assert(flavor == RUSAGE_INFO_V2);
    struct rusage_info_v2 *usage = (struct rusage_info_v2 *)buffer;
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
    struct proc_bsdinfo *bsd = buffer;
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
    assert(observe(&s, &row, 1) && !s.invalid && !s.turnover);
    free(s.tracked);
    fputs("Sampler unit checks passed.\n", stderr);
    return 0;
}
