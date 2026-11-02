#!/bin/sh
# qits-ci builder gate (qits-1807): a `buildctl` in front of the real one.
#
# Every lease on a target dials the same buildkitd at $BUILDKIT_HOST, and a build that buildkitd has
# not started yet used to sit inside `buildctl build` on the step's clock. This shim queues a build
# for a place at the builder before buildctl ever dials it, and while it waits it leaves a marker in
# $QITS_CI_STEP_CLOCK_DIR that tells the ci-daemon to stop the step's clock. Once it holds a place it
# execs the real buildctl, and the place is held exactly as long as that process lives.
#
# Written by the ci-daemon at startup (BuilderGate); POSIX sh, no bashisms — dash and busybox ash
# run it. Anything that is not a build passes straight through.
#
# Env:
#   QITS_CI_REAL_BUILDCTL              the buildctl this shim stands in front of (set by the daemon)
#   QITS_CI_STEP_CLOCK_DIR             where the wait marker goes; unset, no marker is written
#   QITS_CI_BUILDER_GATE_DIR           /cache/git/.qits-builder-gate — shared by the target's leases
#   QITS_CI_BUILDER_SLOTS              builds the builder runs at once (1)
#   QITS_CI_BUILDER_WAIT_MAX_SECONDS   how long one build may wait for a place (3600); then exit 75

real=${QITS_CI_REAL_BUILDCTL:-}
if [ -z "$real" ]; then
  echo "[builder] QITS_CI_REAL_BUILDCTL is not set; no buildctl to run" >&2
  exit 127
fi

# Gate only a build. Global options (--addr <x>, --tlscacert <x>, ...) may precede the subcommand,
# and telling an option's value from the subcommand needs buildctl's own option table, so the rule
# is the conservative one: any argument that is exactly `build` gates. `du`, `prune` and `debug`
# pass through at once, even while the gate is full.
gated=no
for arg in "$@"; do
  if [ "$arg" = build ]; then
    gated=yes
    break
  fi
done
if [ "$gated" = no ]; then
  exec "$real" "$@"
fi

builder=${BUILDKIT_HOST:-the builder}
gate=${QITS_CI_BUILDER_GATE_DIR:-/cache/git/.qits-builder-gate}
slots=${QITS_CI_BUILDER_SLOTS:-1}
wait_max=${QITS_CI_BUILDER_WAIT_MAX_SECONDS:-3600}
case $slots in '' | *[!0-9]*) slots=1 ;; esac
[ "$slots" -ge 1 ] || slots=1
case $wait_max in '' | *[!0-9]*) wait_max=3600 ;; esac

# Prints only: the exec has to stay at each call site, because "$@" inside a function is the
# function's own arguments, not buildctl's.
ungated() {
  echo "[builder] gate unavailable at $gate, building ungated${1:+ ($1)}" >&2
}

if ! command -v flock >/dev/null 2>&1; then
  ungated "no flock"
  exec "$real" "$@"
fi
parent=$(dirname -- "$gate")
if [ ! -d "$parent" ] || [ ! -w "$parent" ]; then
  ungated
  exec "$real" "$@"
fi
mkdir -p -- "$gate/q" "$gate/new" 2>/dev/null
if [ ! -d "$gate/q" ] || [ ! -w "$gate/q" ] || [ ! -w "$gate/new" ]; then
  ungated
  exec "$real" "$@"
fi

# The ticket's name orders the queue: epoch nanoseconds, zero-padded so name order is time order,
# then who (a container's hostname is its id) and which process.
ns=$(date +%s%N 2>/dev/null)
case $ns in '' | *[!0-9]*) ns="$(date +%s)000000000" ;; esac
host=$(uname -n 2>/dev/null | tr -c 'A-Za-z0-9.\n-' '_')
name=$(printf '%020d' "$ns")-${host:-host}-$$
staged=$gate/new/$name
ticket=$gate/q/$name
marker=
if [ -n "${QITS_CI_STEP_CLOCK_DIR:-}" ]; then
  marker=$QITS_CI_STEP_CLOCK_DIR/wait-$$
fi

cleanup() {
  [ -n "$marker" ] && rm -f -- "$marker" "$marker.tmp" 2>/dev/null
  rm -f -- "$ticket" "$staged" 2>/dev/null
}
trap 'cleanup' EXIT
trap 'cleanup; trap - EXIT; exit 129' HUP
trap 'cleanup; trap - EXIT; exit 130' INT
trap 'cleanup; trap - EXIT; exit 143' TERM

# The ticket is locked before it is queued: created and flocked under new/, then renamed into q/
# (a rename keeps the inode, and with it the lock). So no waiter can ever see an unlocked live
# ticket and take it for a dead one. fd 9 stays open across the final exec — the lock lives exactly
# as long as buildctl does, and a ticket whose lock is free belongs to a build that has ended.
# Probed in a subshell first: a failed redirection on `exec` ends a POSIX shell outright.
if ! (: >"$staged") 2>/dev/null; then
  ungated "cannot write a ticket"
  exec "$real" "$@"
fi
exec 9>"$staged"
if ! flock -x 9 || ! mv -f -- "$staged" "$ticket" 2>/dev/null; then
  rm -f -- "$staged" 2>/dev/null
  exec 9>&-
  ungated "cannot queue a ticket"
  exec "$real" "$@"
fi

started=$(date +%s)
waiting=no
while :; do
  ahead=0
  for other in "$gate"/q/*; do
    [ -e "$other" ] || continue
    [ "$other" = "$ticket" ] && break
    # A lock that is free is a holder that is gone — a finished build, or a waiter that died. Racing
    # another sweeper over the same file is harmless; a failure here means nothing.
    if flock -n "$other" true 2>/dev/null; then
      rm -f -- "$other" 2>/dev/null
      continue
    fi
    ahead=$((ahead + 1))
  done
  if [ "$ahead" -lt "$slots" ]; then
    break
  fi
  now=$(date +%s)
  if [ "$waiting" = no ]; then
    waiting=yes
    if [ -n "$marker" ]; then
      # builder address, since (epoch seconds), builds ahead — renamed into place whole.
      printf '%s\n%s\n%s\n' "$builder" "$started" "$ahead" >"$marker.tmp" 2>/dev/null &&
        mv -f -- "$marker.tmp" "$marker" 2>/dev/null
    fi
    echo "[builder] waiting for $builder ($ahead builds ahead)" >&2
  fi
  if [ $((now - started)) -ge "$wait_max" ]; then
    echo "[builder] $builder did not start this build within ${wait_max}s ($ahead builds ahead)" >&2
    cleanup
    trap - EXIT
    exit 75
  fi
  sleep 1
done

[ -n "$marker" ] && rm -f -- "$marker" "$marker.tmp" 2>/dev/null
trap - EXIT HUP INT TERM
exec "$real" "$@"
