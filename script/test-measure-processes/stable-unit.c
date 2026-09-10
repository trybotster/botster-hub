#define kevent(...) test_kevent(__VA_ARGS__)
#define main sampler_main
#include "../measure-processes.c"
#undef main
#undef kevent
#include <assert.h>

static unsigned pending, drain_calls;
static int receipt_count = 1, receipt_error;
static bool drain_error;

int test_kevent(int queue, const struct kevent *changes, int change_count,
               struct kevent *events, int event_count, const struct timespec *timeout) {
    (void)queue;
    assert(timeout && timeout->tv_sec == 0 && timeout->tv_nsec == 0);
    if (change_count) {
        assert(change_count == 1 && event_count == 1);
        assert(changes[0].filter == EVFILT_PROC);
        assert(changes[0].flags & EV_RECEIPT);
        assert(!(changes[0].flags & (EV_DELETE | EV_DISABLE | EV_ONESHOT)));
        assert(changes[0].fflags == (NOTE_FORK | NOTE_EXEC | NOTE_EXIT));
        events[0] = changes[0];
        events[0].flags = EV_ERROR;
        events[0].data = receipt_error;
        return receipt_count;
    }
    drain_calls++;
    if (drain_error) { errno = EIO; return -1; }
    unsigned count = pending < (unsigned)event_count ? pending : (unsigned)event_count;
    for (unsigned i = 0; i < count; ++i) {
        EV_SET(&events[i], (uintptr_t)getpid(), EVFILT_PROC, EV_CLEAR, NOTE_FORK, 0, NULL);
    }
    pending -= count;
    return (int)count;
}

int main(void) {
    Sampler s = {0};
    StableBaseline stable = {.sampler = &s, .queue = 10, .attempt = 1};
    pending = 35;
    assert(stable_drain(&stable));
    assert(pending == 0 && drain_calls == 3 && stable.dirty && !stable.failed);
    stable.started = true;
    pending = 65;
    drain_calls = 0;
    assert(stable_drain(&stable));
    assert(pending == 0 && drain_calls == 4 && stable.failed);
    assert(stable_drain(&stable) && stable.failed);
    drain_error = true;
    assert(!stable_drain(&stable) && stable.failed && s.invalid);
    drain_error = false;

    stable = (StableBaseline){.sampler = &s, .queue = 10, .attempt = 1};
    Row self = {0};
    assert(read_row(&s, getpid(), &self));
    receipt_error = EPERM;
    assert(!stable_watch(&stable, &self) && stable.watch_count == 0);
    receipt_error = 0;
    receipt_count = 0;
    assert(!stable_watch(&stable, &self) && stable.watch_count == 0);
    receipt_count = 1;
    s.invalid = false;
    stable.already_watched = true;
    assert(stable_watch(&stable, &self) && stable.watch_count == 1 && !stable.already_watched);
    stable.already_watched = true;
    assert(stable_watch(&stable, &self) && !stable.already_watched);
    stable.attempt = 2;
    stable.already_watched = true;
    assert(stable_watch(&stable, &self) && stable.already_watched);
    stable.started = true;
    Row replacement = self;
    replacement.usage.ri_proc_start_abstime++;
    assert(!stable_watch(&stable, &replacement) && stable.failed);
    free(stable.watches);

    s.invalid = false;
    stable = (StableBaseline){.sampler = &s, .started = true};
    replacement = self;
    assert(stable_same_set(&stable, &self, 1, &replacement, 1));
    replacement.bsd.pbi_ppid++;
    assert(!stable_same_set(&stable, &self, 1, &replacement, 1));
    replacement = self;
    replacement.usage.ri_child_user_time++;
    assert(!stable_same_set(&stable, &self, 1, &replacement, 1));
    assert(!stable_same_set(&stable, &self, 1, NULL, 0));
    fputs("Stable unit checks passed.\n", stderr);
    return 0;
}
