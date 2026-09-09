#define main sampler_main
#include "../measure-processes.c"
#undef main
#include <assert.h>

static uint64_t clock_ns(struct timespec value) {
    assert(value.tv_sec >= 0 && value.tv_nsec >= 0);
    __uint128_t ns = (__uint128_t)value.tv_sec * 1000000000 + (uint64_t)value.tv_nsec;
    assert(ns <= UINT64_MAX);
    return (uint64_t)ns;
}

int main(void) {
    struct timespec before, after, resolution;
    struct rusage_info_v2 usage = {0};
    mach_timebase_info_data_t base;
    assert(mach_timebase_info(&base) == KERN_SUCCESS);
    assert(clock_getres(CLOCK_PROCESS_CPUTIME_ID, &resolution) == 0);
    assert(clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &before) == 0);
    assert(proc_pid_rusage(getpid(), RUSAGE_INFO_V2, (rusage_info_t *)&usage) == 0);
    assert(clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &after) == 0);
    assert(usage.ri_user_time <= UINT64_MAX - usage.ri_system_time);
    uint64_t own_ns;
    assert(ticks_ns(usage.ri_user_time + usage.ri_system_time, base, &own_ns));
    uint64_t before_ns = clock_ns(before), after_ns = clock_ns(after);
    uint64_t resolution_ns = clock_ns(resolution);
    printf("{\"version\":1,\"event\":\"native_cpu_units\",\"pid\":%d,"
           "\"timebase_numer\":%u,\"timebase_denom\":%u,"
           "\"own_user_ticks\":%" PRIu64 ",\"own_system_ticks\":%" PRIu64 ","
           "\"own_cpu_ns\":%" PRIu64 ",\"clock_before_ns\":%" PRIu64 ","
           "\"clock_after_ns\":%" PRIu64 ",\"clock_resolution_ns\":%" PRIu64 "}\n",
           getpid(), base.numer, base.denom, usage.ri_user_time, usage.ri_system_time,
           own_ns, before_ns, after_ns, resolution_ns);
    fflush(stdout);
    assert((__uint128_t)own_ns + resolution_ns >= before_ns);
    assert((__uint128_t)own_ns <= (__uint128_t)after_ns + resolution_ns);
    return 0;
}
