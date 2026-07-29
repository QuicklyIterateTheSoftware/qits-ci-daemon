# qits-ci-daemon

Everything qits-ci runs **inside** a step container. One binary, one step, one container lifetime.

A ci run is a host-side sequence of steps, and each step is its own throwaway container started from
the image that step's pipeline config declares. This binary is what runs in it: it clones the pushed
commit, executes that one step's script as its own child process, streams the output back over the
socket it dialled, reports the exit, and exits. Then the container is reaped and the next step gets a
fresh one. Everything on the host's side of the boundary — launching containers, parsing the
pipeline config, persisting runs and steps — belongs to
[qits-ci](https://github.com/QuicklyIterateTheSoftware/qits-ci).

    ./mvnw verify     # a clone of this repo alone builds and tests green — no monorepo, no docker

## Layout

| Module | What |
|---|---|
| `ci-daemon-protocol/` | The control-socket wire contract: message records + a codec over a plain `Map`. Depends on nothing. qits-ci vendors a byte-identical copy. |
| `ci-daemon/` | The binary. A Quarkus command-mode app — no web stack, it dials out and never listens — compiled to a fully static musl native image. |

Inside `ci-daemon/`, `Main` is the only CDI bean: it resolves configuration and news up plain
classes with plain constructors — `DaemonMain` (the flow), `ControlSocket` (the dial),
`Workspace` (clone + checkout), `StepProcess` (the child). One reader per setting, which is what
stops two components from resolving the same key differently, and a suite that can drive the whole
flow against a real socket without a container.

`ci-daemon-protocol` is **framework-free**: no Quarkus, no CDI, no JAX-RS, no Jackson — a plain jar
of records with plain constructors. That is not stylistic. The shipping form of this daemon is a
fully static musl native image, so every dependency is a decision about image size and about what the
GraalVM builder has to be told; and the module is copied into qits-ci, where a framework dependency
would arrive as a second opinion about how a ci service is wired.

## The boundary

**qits-ci never executes anything.** It starts containers; that is all. A step's script reaches a
container only as the reply on the socket that container's daemon dialled, and executes only as that
daemon's child. This binary is the other half of that rule: it is the process that *does* run
repo-controlled code, and it runs it inside the sandbox the host built — `--cap-drop=ALL`,
`no-new-privileges`, resource caps, no docker socket.

The consequence runs the other way too. From the moment a step's script starts, everything this
daemon sends is attacker-influenceable data about the run. The host records it and never trusts it —
timestamps are host-stamped at message receipt rather than daemon-reported, and the per-container
secret authorizes exactly "deliver data about this run" and nothing else.

## The lifecycle

Told, never derived. The daemon parses nothing out of its environment and announces no address of
its own; it is handed everything before the socket exists:

| Env | What |
|---|---|
| `QITS_CI_DAEMON_URL` | The control socket, dialled **verbatim**. |
| `QITS_CI_DAEMON_ID` | The host-minted registration identity. |
| `QITS_CI_DAEMON_SECRET` | The per-container secret, minted at launch and dead when the container is reaped. |
| `QITS_CI_REPOSITORY_URL`, `QITS_CI_BRANCH`, `QITS_CI_SHA`, `QITS_CI_REPO_ID` | What to clone and what to check out. |
| `CI`, `QITS_CI` | Both `true`, set for the step script's benefit rather than the daemon's. |

Then, over one WebSocket dialled outbound with `X-Qits-Ci-Daemon-Id` and `X-Qits-Ci-Daemon-Secret` as
handshake headers — **there is no inbound listener in the container at all, at any stage**:

    dial → Hello → Ack → clone+checkout → Initialized → RunStep → StepChunk* → StepFinished → exit

`Heartbeat` runs underneath from dial to close. `Cancel` may arrive after `RunStep` and is answered
with a `StepFinished` like any other ending. Failure to clone or check out replaces `Initialized`
with `InitFailed`, whose `reason` is the structured signal that retired qits-ci's old
prelude-sentinel inference.

**The workspace daemon never exits; this daemon always exits.** That is the one deliberate inversion
of the precedent it otherwise mirrors. Any terminal condition — result delivered, `InitFailed` sent,
an `Ack` carrying a capability version this binary does not know, or dial failure after a short
capped retry — ends the process. Exit code 0 only on the clean paths, so `docker logs` of a reaped
container reads honestly:

| Exit | Ending |
|---|---|
| 0 | A step ran and its `StepFinished` reached the host. The only clean one. |
| 2 | The env contract was not satisfied; nothing was dialled. |
| 3 | No connection within the dial budget (~30s total, not infinite). |
| 4 | The `Ack` carried a capability version this binary does not know. |
| 5 | `InitFailed` delivered — the daemon did its job, the container did not run its step. |
| 6 | The socket closed before a `RunStep`; the host has reaped us. |
| 7 | `Cancel` with no step running. |

The socket already carried whether the daemon behaved; the exit code is about whether the
*container* did what it was started for, which is why 5 is not a zero.

## The image contract

The daemon is **not** baked into an image. qits-ci runs arbitrary repo-chosen images — that is the
feature — so the entrypoint is overridden with a fixed, host-authored bootstrap that downloads
`$QITS_CI_DAEMON_BINARY_URL`, `chmod +x`, and `exec`s it. Nothing is mounted and nothing is injected
into the image; the container fetches over the same network path it already uses for its clone.

What that asks of a step's image: **`git`, `bash`, and a downloader** (`wget` or `curl` — the
bootstrap probes for either). What it asks of this repo: the binary must be a **fully static musl
build**, because a glibc-linked one dies on every alpine-family image, and it must take no arguments
— the environment above is its whole input.

`git` and `bash` are probed with `--version` before the clone, so an image that does not satisfy the
contract reports `InitFailed{TOOLING_MISSING}` rather than failing halfway through a step.

## Building the binary

    docker build -t qits/graalvmce-musl-builder:jdk-25 -f docker/Dockerfile.musl-builder docker/
    ./mvnw -B -ntp -pl ci-daemon -am package -Dnative -DskipTests

The result is `ci-daemon/target/qits-ci-daemon` (~43 MB, stripped), and it is the direct output that
`$QITS_CI_DAEMON_BINARY_URL` serves — no image, no wrapper.

The builder image is **built locally and lives in no registry**, so whatever publishes a release has
to build it first; `quarkus.native.builder-image.pull=missing` is what stops every build from trying
to `docker pull` it. Its base is GraalVM CE and **not** the Mandrel image qits-workspace-daemon
builds on, because Mandrel ships static JDK libraries for glibc only and a `--libc=musl` build dies
in its first second. `docker/Dockerfile.musl-builder` carries the error message and the rest of the
reasoning; `ci-daemon/src/main/resources/application.properties` carries the four properties.

Checking a build:

    $ file ci-daemon/target/qits-ci-daemon
    ...: ELF 64-bit LSB pie executable, x86-64, version 1 (SYSV), static-pie linked, stripped
    $ ldd ci-daemon/target/qits-ci-daemon
            statically linked

**`file` says `static-pie linked`, not `statically linked`** — GraalVM emits a position-independent
static executable. It is genuinely fully static; assert on `ldd`, or on `static-pie`, never on the
literal string `statically linked`.

Running the binary with no environment at all is the cheap check the test suite structurally cannot
do: it must print the name of the first missing variable and exit 2, not die resolving config.

    $ docker run --rm -v "$PWD/ci-daemon/target/qits-ci-daemon":/qits-ci-daemon:ro alpine:3 /qits-ci-daemon
    ... ci-daemon cannot start: QITS_CI_DAEMON_URL is not set. Exiting.

## Relationship to qits-workspace-daemon

The same recipe, at one-fifth the size:
[qits-workspace-daemon](https://github.com/QuicklyIterateTheSoftware/qits-workspace-daemon) earned
the outbound-only shape, the protocol-module pattern, and the clone-alone rule, and this repo spends
them rather than re-deriving them. It is **not** a reuse of that protocol: the messages are disjoint,
and coupling the two wire contracts would make every workspace capability bump a ci event.
