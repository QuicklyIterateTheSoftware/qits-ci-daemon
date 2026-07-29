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

The Quarkus application module lands next; today this repo is the wire contract and the scaffolding
around it. The reactor lists only modules that exist, because `./mvnw verify` on a fresh clone is the
gate and a placeholder module would fail it.

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
container reads honestly.

## The image contract

The daemon is **not** baked into an image. qits-ci runs arbitrary repo-chosen images — that is the
feature — so the entrypoint is overridden with a fixed, host-authored bootstrap that downloads
`$QITS_CI_DAEMON_BINARY_URL`, `chmod +x`, and `exec`s it. Nothing is mounted and nothing is injected
into the image; the container fetches over the same network path it already uses for its clone.

What that asks of a step's image: **`git`, `bash`, and a downloader** (`wget` or `curl` — the
bootstrap probes for either). What it asks of this repo: the binary must be a **fully static musl
build**, because a glibc-linked one dies on every alpine-family image, and it must take no arguments
— the environment above is its whole input.

## Relationship to qits-workspace-daemon

The same recipe, at one-fifth the size:
[qits-workspace-daemon](https://github.com/QuicklyIterateTheSoftware/qits-workspace-daemon) earned
the outbound-only shape, the protocol-module pattern, and the clone-alone rule, and this repo spends
them rather than re-deriving them. It is **not** a reuse of that protocol: the messages are disjoint,
and coupling the two wire contracts would make every workspace capability bump a ci event.
