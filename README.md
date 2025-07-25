#  🏋️ benchpress

`benchpress` is a lightweight command-line tool written in Go to help you coordinate benchmarks,
run experiments, and collect output from various system tools, scripts, or probesall in a repeatable
and structured way. Ideal for systems research, performance tuning, observability, or chaos testing
workflows.

## Features

- Run benchmarks in multiple phases: prerequisites, loads, and cases
- Define and reuse shell commands across benchmarking runs
- Run benchmarks multiple times (runs: N) to average out noise
- Collect structured output from diverse tools (e.g. funclatency, stress-ng, custom scripts)
- YAML-based config: easy to read, version, and share

## How it works

- Prerequisites: Started before the main workload (e.g. tracing tools, background monitors)
- Loads: The synthetic or real workload to be measured
- Cases: Variations of system setup or instrumentation to benchmark (e.g. enabling probes)
- Runs: Each case/load pair is run multiple times for statistical significance

### Example

``` yaml
runs: 5
prerequisites:
  - name: funclatency
    cmd: funclatency
    args:
      - /usr/lib/x86_64-linux-gnu/libc.so.6:read
loads:
  - name: stressor
    cmd: stress-ng
    args:
      - --hdd
      - 4
      - --hdd-bytes
      - 8G
      - --timeout 5m
      - --temp-path
      - /tmp/stress
cases:
  - name: baseline
  - name: with a uprobe
    cmd: ./uprobe
    env:
      - SOMECONFIG=value
    workdir: /home/ubuntu
```

## Installation

```bash
go install github.com/therealbobo/benchpress@latest
```
