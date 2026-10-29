#!/usr/bin/env python3
"""Where r8sd spends wall time, from its log.

    scripts/r8sd-timings.py pods     [LOG] [--since TS] [--until TS] [--top N]
    scripts/r8sd-timings.py requests [LOG] [--since TS] [--until TS] [--top N]

`pods` follows every pod from its binding through network setup,
container create and start to Running (and Ready when a readiness probe
gates it), then prints each phase's p50/p90/max and the slowest pods with
their breakdown. A pod name that is reused (a StatefulSet replacement)
starts a new record at each binding.

`requests` groups API requests by method and path shape (namespace and
object names folded into placeholders, query dropped) and prints count,
p50, p90 and max of the logged durations. Watches and streamed logs are
left out: their duration is only the time to open the stream.

LOG defaults to /var/lib/r8s/clusters/default/r8sd.log. --since/--until
take an ISO timestamp prefix as the log prints it (2026-10-28T17:20).
"""

import argparse
import re
import sys
from collections import defaultdict
from datetime import datetime

DEFAULT_LOG = "/var/lib/r8s/clusters/default/r8sd.log"
ANSI = re.compile(r"\x1b\[[0-9;]*m")
STAMP = re.compile(r"^(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d+)Z")

# (phase, pattern capturing the pod name); the first phase opens a record.
POD_EVENTS = [
    ("scheduled", re.compile(r"r8s_scheduler::scheduler: scheduled pod '([^']+)'")),
    ("network", re.compile(r'r8s_network::bridge: pod network configured pod_name="([^"]+)"')),
    ("created", re.compile(r'r8s_runtime::containerd: created container name="([^"]+?)_[^"]*"')),
    ("started", re.compile(r"r8s_kubelet: pod '([^']+)': started container")),
    ("running", re.compile(r"r8s_kubelet: pod '([^']+)': status updated to Running")),
    ("ready", re.compile(r"r8s_kubelet: pod '([^']+)': readiness changed to true")),
]
PHASES = [name for name, _ in POD_EVENTS]
UNSCHEDULABLE = re.compile(r"r8s_scheduler::scheduler: pod '([^']+)' unschedulable")
REQUEST = re.compile(r"r8s_api::server: (\w+) (\S+) -> (\d{3})[^(]*\(([\d.]+)ms\)")


def lines(path, since, until):
    with open(path, errors="replace") as f:
        for raw in f:
            line = ANSI.sub("", raw)
            m = STAMP.match(line)
            if not m:
                continue
            ts = m.group(1)
            if since and ts < since:
                continue
            if until and ts >= until:
                break
            yield datetime.fromisoformat(ts), line


def percentile(values, p):
    values = sorted(values)
    if not values:
        return 0.0
    k = min(len(values) - 1, max(0, round(p / 100 * (len(values) - 1))))
    return values[k]


def pods(args):
    records = []  # each: {"name", phase: datetime, "waits": n}
    live = {}
    pending_waits = defaultdict(int)
    for ts, line in lines(args.log, args.since, args.until):
        m = UNSCHEDULABLE.search(line)
        if m:
            pending_waits[m.group(1)] += 1
            continue
        for phase, pattern in POD_EVENTS:
            m = pattern.search(line)
            if not m:
                continue
            name = m.group(1)
            if phase == "scheduled":
                rec = {"name": name, "scheduled": ts, "waits": pending_waits.pop(name, 0)}
                records.append(rec)
                live[name] = rec
            elif name in live and phase not in live[name]:
                live[name][phase] = ts
            break

    steps = list(zip(PHASES, PHASES[1:]))
    durations = defaultdict(list)
    totals = []
    for rec in records:
        for a, b in steps:
            if a in rec and b in rec:
                durations[f"{a}->{b}"].append((rec[b] - rec[a]).total_seconds())
        end = rec.get("ready") or rec.get("running")
        if end:
            totals.append(((end - rec["scheduled"]).total_seconds(), rec))

    print(f"{len(records)} pods bound, {len(totals)} reached Running")
    unschedulable = sum(1 for r in records if r["waits"])
    if unschedulable:
        print(f"{unschedulable} were unschedulable at least once before binding")
    print(f"\n{'phase':<20} {'count':>6} {'p50':>8} {'p90':>8} {'max':>8}")
    for a, b in steps:
        values = durations[f"{a}->{b}"]
        if values:
            print(f"{a + '->' + b:<20} {len(values):>6} "
                  f"{percentile(values, 50):>7.2f}s {percentile(values, 90):>7.2f}s {max(values):>7.2f}s")
    total_values = [t for t, _ in totals]
    if total_values:
        print(f"{'scheduled->up':<20} {len(total_values):>6} "
              f"{percentile(total_values, 50):>7.2f}s {percentile(total_values, 90):>7.2f}s "
              f"{max(total_values):>7.2f}s")

    print(f"\nslowest {args.top} pods (scheduled -> Running, or Ready when gated):")
    for total, rec in sorted(totals, key=lambda t: -t[0])[: args.top]:
        parts = []
        for a, b in steps:
            if a in rec and b in rec:
                parts.append(f"{b} +{(rec[b] - rec[a]).total_seconds():.1f}s")
        when = rec["scheduled"].strftime("%H:%M:%S")
        print(f"{total:7.1f}s  {when}  {rec['name'][:60]}")
        print(f"          {', '.join(parts)}")


def path_shape(path):
    """`/api/v1/namespaces/x/pods/y/log` -> `/api/v1/namespaces/{ns}/pods/{name}/log`."""
    parts = path.split("?", 1)[0].strip("/").split("/")
    if not parts or parts[0] not in ("api", "apis"):
        return "/" + "/".join(parts)
    head = 2 if parts[0] == "api" else 3  # api/v1 | apis/group/version
    out = parts[:head]
    rest = parts[head:]
    if len(rest) >= 2 and rest[0] == "namespaces":
        out += ["namespaces", "{ns}"]
        rest = rest[2:]
    if rest:
        out.append(rest[0])  # resource
        if len(rest) >= 2:
            out.append("{name}")
            out += rest[2:]  # subresource
    return "/" + "/".join(out)


def requests(args):
    groups = defaultdict(list)
    for _, line in lines(args.log, args.since, args.until):
        m = REQUEST.search(line)
        if not m:
            continue
        method, uri, status, ms = m.groups()
        query = uri.split("?", 1)[1] if "?" in uri else ""
        if "watch=true" in query or "watch=1" in query or "follow=true" in query:
            continue
        path = re.sub(r"^https?://[^/]+", "", uri)
        groups[(method, path_shape(path))].append(float(ms))
    if not groups:
        sys.exit("no timed requests in range (the log predates request durations?)")
    rows = sorted(groups.items(), key=lambda kv: -percentile(kv[1], 90))
    print(f"{'count':>6} {'p50':>9} {'p90':>9} {'max':>9}  request")
    for (method, shape), values in rows[: args.top]:
        print(f"{len(values):>6} {percentile(values, 50):>7.1f}ms {percentile(values, 90):>7.1f}ms "
              f"{max(values):>7.1f}ms  {method} {shape}")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("report", choices=["pods", "requests"])
    parser.add_argument("log", nargs="?", default=DEFAULT_LOG)
    parser.add_argument("--since")
    parser.add_argument("--until")
    parser.add_argument("--top", type=int, default=15)
    args = parser.parse_args()
    {"pods": pods, "requests": requests}[args.report](args)


if __name__ == "__main__":
    main()
