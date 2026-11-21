# SPDX-License-Identifier: (LGPL-2.1 OR BSD-2-Clause)
# Makefile for compiling TraceKV native eBPF CO-RE bytecode

CLANG ?= clang
LLVM_STRIP ?= llvm-strip
BPFTOOL ?= bpftool

ARCH ?= $(shell uname -m | sed 's/x86_64/x86/' | sed 's/aarch64/arm64/')
CFLAGS ?= -g -O2 -Wall -target bpf -D__TARGET_ARCH_$(ARCH)

OUTPUT ?= .
VMLINUX ?= /sys/kernel/btf/vmlinux

all: $(OUTPUT)/sched_monitor.bpf.o

$(OUTPUT)/sched_monitor.bpf.o: sched_monitor.bpf.c sched_monitor.h
	$(CLANG) $(CFLAGS) -I. -c $< -o $@
	$(LLVM_STRIP) -g $@ 2>/dev/null || true

clean:
	rm -f $(OUTPUT)/sched_monitor.bpf.o

.PHONY: all clean
