/* SPDX-License-Identifier: (LGPL-2.1 OR BSD-2-Clause) */
/* Copyright (c) 2026 TraceKV Team / Deng Zhiwen */
#ifndef __SCHED_MONITOR_H__
#define __SCHED_MONITOR_H__

#define TASK_COMM_LEN 16
#define HIST_SLOTS_COUNT 32

/* Metric types corresponding to TraceKV v0.2 / v0.3 semantics */
enum metric_type {
    METRIC_WAKEUP_LATENCY = 0,
    METRIC_RUNNABLE_WAIT  = 1,
    METRIC_ON_CPU         = 2,
    METRIC_DIRECT_RECLAIM = 3,
};

/* Unified ring buffer event record (32 bytes, cacheline aligned) */
struct sched_event {
    unsigned long long timestamp_ns;
    unsigned long long wakeup_ns;
    unsigned long long latency_ns;
    unsigned int pid;
    unsigned int tid;
    unsigned int cpu;
    unsigned char metric_type;
    unsigned char is_target;
    char comm[TASK_COMM_LEN];
};

/* In-kernel aggregated histogram slot representation */
struct hist_bucket {
    unsigned long long slots[HIST_SLOTS_COUNT];
};

/* Global runtime telemetry counters */
enum stat_counter {
    STAT_MATCHED = 0,
    STAT_RUNWAIT_MATCHED = 1,
    STAT_ONCPU_MATCHED = 2,
    STAT_RECLAIM_MATCHED = 3,
    STAT_DUP_WAKEUP = 4,
    STAT_UNMATCHED_SWITCH = 5,
    STAT_EXIT_CLEANUP = 6,
    STAT_MAP_OVERFLOW = 7,
    STAT_MAX = 8,
};

#endif /* __SCHED_MONITOR_H__ */
