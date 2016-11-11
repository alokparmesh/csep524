#include <stdbool.h>
#include "timing.h"

static double   _ns_per_cycle;
static bool     _init = false;

void    calibrate_ns() {
    if (_init)
        return;

    u_int64_t   start_usec = gettimeofday_usec();
    u_int64_t   start_cycle = rdtsc();
    u_int64_t   finish_usec, finish_cycle;
    while (true) {
        finish_usec = gettimeofday_usec();
        if ((finish_usec - start_usec) > USECS_PER_SEC)
            break;
        }    
    finish_cycle = rdtsc();
    double  ns_in_period = (finish_usec - start_usec) * NSECS_PER_USEC;
    _ns_per_cycle = ns_in_period / ((double) (finish_cycle - start_cycle));
    _init = true;
}

double  ns_per_cycle() {
    calibrate_ns();
    return _ns_per_cycle;
}

double  cycles_to_ns_f(double   cycles_f) {
    return cycles_f * ns_per_cycle();
}

u_int64_t   cycles_to_ns(u_int64_t  cycles) {
    double  cycles_f = cycles;
    return (u_int64_t) cycles_to_ns_f(cycles_f);
}


