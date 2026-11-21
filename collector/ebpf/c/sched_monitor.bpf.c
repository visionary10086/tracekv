// SPDX-License-Identifier: GPL-2.0 OR BSD-3-Clause
/* Copyright (c) 2026 TraceKV Team / Deng Zhiwen */

#include "sched_monitor.h"

/*
 * Minimal vmlinux / BPF helper declarations for portable CO-RE compilation.
 * Avoids rigid dependency on specific kernel header trees.
 */
typedef unsigned char __u8;
typedef unsigned short __u16;
typedef unsigned int __u32;
typedef unsigned long long __u64;
typedef int pid_t;

#define BPF_MAP_TYPE_HASH 1
#define BPF_MAP_TYPE_ARRAY 2
#define BPF_MAP_TYPE_PERCPU_ARRAY 6
#define BPF_MAP_TYPE_RINGBUF 27

#define SEC(name) __attribute__((section(name), used))

/* BPF Helper prototypes */
static void *(*bpf_map_lookup_elem)(void *map, const void *key) = (void *) 1;
static long (*bpf_map_update_elem)(void *map, const void *key, const void *value, __u64 flags) = (void *) 2;
static long (*bpf_map_delete_elem)(void *map, const void *key) = (void *) 3;
static __u64 (*bpf_ktime_get_ns)(void) = (void *) 5;
static __u64 (*bpf_get_current_pid_tgid)(void) = (void *) 14;
static long (*bpf_get_current_comm)(void *buf, __u32 size_of_buf) = (void *) 16;
static __u32 (*bpf_get_smp_processor_id)(void) = (void *) 8;
static void *(*bpf_ringbuf_reserve)(void *ringbuf, __u64 size, __u64 flags) = (void *) 131;
static void (*bpf_ringbuf_submit)(void *data, __u64 flags) = (void *) 132;
static void (*bpf_ringbuf_discard)(void *data, __u64 flags) = (void *) 133;

#define BPF_ANY 0
#define BPF_NOEXIST 1

/* Tracepoint context structs */
struct sched_switch_args {
    __u64 _pad;
    char prev_comm[16];
    pid_t prev_pid;
    int prev_prio;
    long prev_state;
    char next_comm[16];
    pid_t next_pid;
    int next_prio;
};

struct sched_wakeup_args {
    __u64 _pad;
    char comm[16];
    pid_t pid;
    int prio;
    int success;
    int target_cpu;
};

struct sched_process_exit_args {
    __u64 _pad;
    char comm[16];
    pid_t pid;
    int prio;
};

struct reclaim_args {
    __u64 _pad;
    int order;
    int may_writepage;
    unsigned int gfp_flags;
};

/* --- Global Configuration (user-space configurable before load) --- */
const volatile __u32 target_tgid = 0;
const volatile __u64 outlier_threshold_ns = 5000000ULL; /* 5ms */

/* --- BPF Maps --- */

/* 1. Ring buffer for zero-copy userspace streaming of outlier events */
struct {
    __u32 type;
    __u32 max_entries;
} events SEC(".maps") = {
    .type = BPF_MAP_TYPE_RINGBUF,
    .max_entries = 256 * 1024, /* 256 KB ring buffer */
};

/* 2. Enqueue timestamps for wakeup-to-run calculation (E1) */
struct {
    __u32 type;
    __u32 key_size;
    __u32 value_size;
    __u32 max_entries;
} enqueue_ts SEC(".maps") = {
    .type = BPF_MAP_TYPE_HASH,
    .key_size = sizeof(__u32),
    .value_size = sizeof(__u64),
    .max_entries = 65536,
};

/* 3. Preempt timestamps for runnable preemption wait calculation (E1) */
struct {
    __u32 type;
    __u32 key_size;
    __u32 value_size;
    __u32 max_entries;
} preempt_ts SEC(".maps") = {
    .type = BPF_MAP_TYPE_HASH,
    .key_size = sizeof(__u32),
    .value_size = sizeof(__u64),
    .max_entries = 65536,
};

/* 4. Switch-in timestamps for actual On-CPU execution duration (E2) */
struct {
    __u32 type;
    __u32 key_size;
    __u32 value_size;
    __u32 max_entries;
} on_cpu_start SEC(".maps") = {
    .type = BPF_MAP_TYPE_HASH,
    .key_size = sizeof(__u32),
    .value_size = sizeof(__u64),
    .max_entries = 65536,
};

/* 5. Memory reclaim timestamps for direct reclaim stall (E5) */
struct {
    __u32 type;
    __u32 key_size;
    __u32 value_size;
    __u32 max_entries;
} reclaim_start SEC(".maps") = {
    .type = BPF_MAP_TYPE_HASH,
    .key_size = sizeof(__u32),
    .value_size = sizeof(__u64),
    .max_entries = 65536,
};

/* 6. In-kernel Per-CPU latency histogram (zero lock contention) */
struct {
    __u32 type;
    __u32 key_size;
    __u32 value_size;
    __u32 max_entries;
} sched_hist SEC(".maps") = {
    .type = BPF_MAP_TYPE_PERCPU_ARRAY,
    .key_size = sizeof(__u32),
    .value_size = sizeof(struct hist_bucket),
    .max_entries = 1,
};

/* 7. Diagnostic counters */
struct {
    __u32 type;
    __u32 key_size;
    __u32 value_size;
    __u32 max_entries;
} stats_map SEC(".maps") = {
    .type = BPF_MAP_TYPE_ARRAY,
    .key_size = sizeof(__u32),
    .value_size = sizeof(__u64),
    .max_entries = STAT_MAX,
};

static __always_inline void inc_stat(__u32 key)
{
    __u64 *val = bpf_map_lookup_elem(&stats_map, &key);
    if (val)
        __sync_fetch_and_add(val, 1);
}

/* In-kernel logarithmic slot index (0..31) for fast percentile bucketing */
static __always_inline __u32 compute_slot(__u64 val_us)
{
    if (val_us == 0)
        return 0;
    __u32 slot = 0;
    __u64 v = val_us;
    while (v > 1 && slot < HIST_SLOTS_COUNT - 1) {
        v >>= 1;
        slot++;
    }
    return slot;
}

static __always_inline void record_hist(__u64 lat_ns)
{
    __u32 key = 0;
    struct hist_bucket *hist = bpf_map_lookup_elem(&sched_hist, &key);
    if (!hist)
        return;
    __u32 slot = compute_slot(lat_ns / 1000);
    if (slot < HIST_SLOTS_COUNT)
        hist->slots[slot]++;
}

static __always_inline void emit_ringbuf_event(
    __u64 ts, __u64 wakeup_ts, __u64 lat,
    __u32 pid, __u32 tid, __u32 cpu,
    __u8 metric, __u8 is_target, const char *comm
)
{
    struct sched_event *ev = bpf_ringbuf_reserve(&events, sizeof(*ev), 0);
    if (!ev) {
        inc_stat(STAT_MAP_OVERFLOW);
        return;
    }
    ev->timestamp_ns = ts;
    ev->wakeup_ns = wakeup_ts;
    ev->latency_ns = lat;
    ev->pid = pid;
    ev->tid = tid;
    ev->cpu = cpu;
    ev->metric_type = metric;
    ev->is_target = is_target;
    for (int i = 0; i < TASK_COMM_LEN; i++) {
        ev->comm[i] = comm[i];
    }
    bpf_ringbuf_submit(ev, 0);
}

/* --- Tracepoint Handlers --- */

SEC("tp/sched/sched_wakeup")
int handle_sched_wakeup(struct sched_wakeup_args *ctx)
{
    __u32 tid = ctx->pid;
    if (tid == 0)
        return 0;

    __u64 ts = bpf_ktime_get_ns();
    bpf_map_update_elem(&enqueue_ts, &tid, &ts, BPF_ANY);
    return 0;
}

SEC("tp/sched/sched_wakeup_new")
int handle_sched_wakeup_new(struct sched_wakeup_args *ctx)
{
    __u32 tid = ctx->pid;
    if (tid == 0)
        return 0;

    __u64 ts = bpf_ktime_get_ns();
    bpf_map_update_elem(&enqueue_ts, &tid, &ts, BPF_ANY);
    return 0;
}

SEC("tp/sched/sched_switch")
int handle_sched_switch(struct sched_switch_args *ctx)
{
    __u64 ts = bpf_ktime_get_ns();
    __u32 cpu = bpf_get_smp_processor_id();
    __u64 pid_tgid = bpf_get_current_pid_tgid();
    __u32 cur_tgid = pid_tgid >> 32;

    /* Anti-Probe Defense: drop collector self-induced events */
    if (ctx->prev_comm[0] == 'b' && ctx->prev_comm[1] == 'p' && ctx->prev_comm[2] == 'f')
        return 0;
    if (ctx->next_comm[0] == 'b' && ctx->next_comm[1] == 'p' && ctx->next_comm[2] == 'f')
        return 0;

    /* 1. Process Outgoing Task (prev_pid) */
    __u32 prev_tid = ctx->prev_pid;
    if (prev_tid != 0) {
        /* Record Preempt TS if task was preempted while still runnable */
        if (ctx->prev_state == 0) {
            bpf_map_update_elem(&preempt_ts, &prev_tid, &ts, BPF_NOEXIST);
        }

        /* E2: Record ONCPU execution duration */
        __u64 *in_ts = bpf_map_lookup_elem(&on_cpu_start, &prev_tid);
        if (in_ts && *in_ts > 0 && ts >= *in_ts) {
            __u64 dur = ts - *in_ts;
            __u8 is_tgt = (target_tgid != 0 && cur_tgid == target_tgid);
            inc_stat(STAT_ONCPU_MATCHED);

            /* Stream outlier or target slice */
            if (is_tgt || dur >= outlier_threshold_ns) {
                emit_ringbuf_event(ts, *in_ts, dur, cur_tgid, prev_tid, cpu,
                                   METRIC_ON_CPU, is_tgt, ctx->prev_comm);
            }
            bpf_map_delete_elem(&on_cpu_start, &prev_tid);
        }
    }

    /* 2. Process Upcoming Task (next_pid) */
    __u32 next_tid = ctx->next_pid;
    if (next_tid == 0)
        return 0;

    /* Start On-CPU timer for upcoming task */
    bpf_map_update_elem(&on_cpu_start, &next_tid, &ts, BPF_ANY);

    /* E1 Path A: Wakeup-to-Run Latency */
    __u64 *enq = bpf_map_lookup_elem(&enqueue_ts, &next_tid);
    if (enq && *enq > 0 && ts >= *enq) {
        __u64 lat = ts - *enq;
        inc_stat(STAT_MATCHED);
        record_hist(lat);

        __u8 is_tgt = (target_tgid != 0 && cur_tgid == target_tgid);
        if (is_tgt || lat >= outlier_threshold_ns) {
            emit_ringbuf_event(ts, *enq, lat, cur_tgid, next_tid, cpu,
                               METRIC_WAKEUP_LATENCY, is_tgt, ctx->next_comm);
        }
        bpf_map_delete_elem(&enqueue_ts, &next_tid);
    } else {
        inc_stat(STAT_UNMATCHED_SWITCH);
    }

    /* E1 Path B: Runnable Wait (Preemption Queueing Stall) */
    __u64 *pmt = bpf_map_lookup_elem(&preempt_ts, &next_tid);
    if (pmt && *pmt > 0 && ts >= *pmt) {
        __u64 wait = ts - *pmt;
        inc_stat(STAT_RUNWAIT_MATCHED);

        __u8 is_tgt = (target_tgid != 0 && cur_tgid == target_tgid);
        if (is_tgt || wait >= outlier_threshold_ns) {
            emit_ringbuf_event(ts, *pmt, wait, cur_tgid, next_tid, cpu,
                               METRIC_RUNNABLE_WAIT, is_tgt, ctx->next_comm);
        }
        bpf_map_delete_elem(&preempt_ts, &next_tid);
    }

    return 0;
}

SEC("tp/sched/sched_process_exit")
int handle_sched_process_exit(struct sched_process_exit_args *ctx)
{
    __u32 tid = ctx->pid;
    bpf_map_delete_elem(&enqueue_ts, &tid);
    bpf_map_delete_elem(&preempt_ts, &tid);
    bpf_map_delete_elem(&on_cpu_start, &tid);
    inc_stat(STAT_EXIT_CLEANUP);
    return 0;
}

/* E5: Direct memory reclaim stalls */
SEC("tp/vmscan/mm_vmscan_direct_reclaim_begin")
int handle_reclaim_begin(struct reclaim_args *ctx)
{
    __u64 pid_tgid = bpf_get_current_pid_tgid();
    __u32 tid = (__u32)pid_tgid;
    __u64 ts = bpf_ktime_get_ns();
    bpf_map_update_elem(&reclaim_start, &tid, &ts, BPF_ANY);
    return 0;
}

SEC("tp/vmscan/mm_vmscan_direct_reclaim_end")
int handle_reclaim_end(void *ctx)
{
    __u64 ts = bpf_ktime_get_ns();
    __u64 pid_tgid = bpf_get_current_pid_tgid();
    __u32 tid = (__u32)pid_tgid;
    __u32 tgid = pid_tgid >> 32;

    __u64 *start_ts = bpf_map_lookup_elem(&reclaim_start, &tid);
    if (start_ts && *start_ts > 0 && ts >= *start_ts) {
        __u64 dur = ts - *start_ts;
        inc_stat(STAT_RECLAIM_MATCHED);

        char comm[16];
        bpf_get_current_comm(comm, sizeof(comm));
        __u8 is_tgt = (target_tgid != 0 && tgid == target_tgid);

        /* Emit reclaim stall */
        emit_ringbuf_event(ts, *start_ts, dur, tgid, tid, bpf_get_smp_processor_id(),
                           METRIC_DIRECT_RECLAIM, is_tgt, comm);
        bpf_map_delete_elem(&reclaim_start, &tid);
    }
    return 0;
}

char LICENSE[] SEC("license") = "Dual BSD/GPL";
