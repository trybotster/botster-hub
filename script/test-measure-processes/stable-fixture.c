#include <assert.h>
#include <inttypes.h>
#include <mach/mach_time.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/wait.h>
#include <unistd.h>

int main(void) {
    int hold[2], ready[2];
    assert(pipe(hold) == 0 && pipe(ready) == 0);
    pid_t child = fork();
    assert(child >= 0);
    if (child == 0) {
        close(hold[1]);
        close(ready[0]);
        assert(write(ready[1], "r", 1) == 1);
        close(ready[1]);
        char byte;
        while (read(hold[0], &byte, 1) > 0) {}
        close(hold[0]);
        _exit(0);
    }
    close(hold[0]);
    close(ready[1]);
    char byte;
    assert(read(ready[0], &byte, 1) == 1);
    close(ready[0]);
    printf("ready %d %d\n", getpid(), child);
    fflush(stdout);
    while (read(STDIN_FILENO, &byte, 1) == 1) {
        if (byte != 'f') continue;
        uint64_t begin = mach_absolute_time();
        pid_t transient = fork();
        assert(transient >= 0);
        if (transient == 0) _exit(0);
        int status;
        assert(waitpid(transient, &status, 0) == transient && WIFEXITED(status));
        printf("fork_exit %d %" PRIu64 " %" PRIu64 "\n",
               transient, begin, mach_absolute_time());
        fflush(stdout);
    }
    close(hold[1]);
    int status;
    assert(waitpid(child, &status, 0) == child && WIFEXITED(status));
    return 0;
}
