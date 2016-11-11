#ifndef _timing_h
#define _timing_h

#include <stdio.h>
#include <sys/types.h>
//#include <sys/time.h>
#include "sizes.h"

#define SECONDS_PER_MINUTE  (60ULL)
#define MINUTES_PER_HOUR    (60ULL)
#define HOURS_PER_DAY       (24ULL)

#define USECS_PER_SEC       (ONE_MILLION)
#define NSECS_PER_USEC      (ONE_THOUSAND)
#define NSECS_PER_SEC       (NSECS_PER_USEC * USECS_PER_SEC)
#define USECS_PER_MINUTE    (SECONDS_PER_MINUTE * USECS_PER_SEC)
#define USECS_PER_HOUR      (MINUTES_PER_HOUR * USECS_PER_MINUTE)
#define USECS_PER_DAY       (HOURS_PER_DAY * USECS_PER_HOUR)

static inline u_int64_t rdtsc(void) {
    u_int32_t hi, lo;
        
    __asm__ __volatile__ ("rdtsc" : "=a" (lo), "=d" (hi));
    return ((u_int64_t)lo) | (((u_int64_t)hi) << 32);
}

static inline u_int64_t usec_to_seconds(u_int64_t usec) {
    return usec / ONE_MILLION;
}

static inline u_int64_t usec_to_minutes(u_int64_t usec) {
    return usec_to_seconds(usec) / SECONDS_PER_MINUTE;
}

static inline u_int64_t usec_to_hours(u_int64_t usec) {
    return usec_to_minutes(usec) / MINUTES_PER_HOUR;
}

static inline u_int64_t usec_to_days(u_int64_t usec) {
    return usec_to_hours(usec) / HOURS_PER_DAY;
}

static inline void usec_to_days_hours_minutes_seconds(u_int64_t usec,
    u_int64_t   *days,
    u_int64_t   *hours,
    u_int64_t   *minutes,
    u_int64_t   *seconds) {
    (*days) = usec_to_days(usec);
    (*hours) = usec_to_hours(usec - (*days) * USECS_PER_DAY);
    (*minutes) = usec_to_minutes(usec - (*days) * USECS_PER_DAY -
        (*hours) * USECS_PER_HOUR);
    (*seconds) = usec_to_seconds(usec - (*days) * USECS_PER_DAY -
        (*hours) * USECS_PER_HOUR - (*minutes) * USECS_PER_MINUTE);
}

static inline u_int64_t gettimeofday_usec() {
    struct timeval  tv;
    
    gettimeofday(&tv, NULL);
    return tv.tv_sec * ONE_MILLION + tv.tv_usec;
}

void    calibrate_ns();
double  ns_per_cycle();
u_int64_t   cycles_to_ns(u_int64_t  cycles);
double  cycles_to_ns_f(double   cycles_f);
#endif


