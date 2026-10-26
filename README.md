# qits-ci-daemon

Everything qits-ci runs **inside** a CI lease. One binary, one run, one container lifetime.

A ci run is one lease on an execution target: one container, from the shared
`qits/build-images/ci-slot` image, with this binary as its entrypoint (qits-1541, epic qits-1481).
The daemon makes the run's worktree from a warm bare mirror the target keeps in `/cache/git`, then
runs every step of the run in it, one after another, each as its own child process, streaming the
output back over the socket it dialled and reporting each exit. When the host says `Close` it
removes the worktree and exits, and qits-ci deletes the lease. Everything on the host's side of the
boundary — leases, parsing the pipeline config, persisting runs and steps — belongs to
[qits-ci-service](https://github.com/QuicklyIterateTheSoftware/qits-ci-service).

    ./mvnw verify     # a clone of this repo alone builds and tests green — no monorepo, no docker

## Layout

| Module | What |
|---|---|
| `ci-daemon-protocol/` | The control-socket wire contract: message records + a codec over a plain `Map`, plus `CiDaemonBinary` naming the binary version released beside it. Depends on nothing. Published as `eu.wohlben.qits:qits-ci-daemon-protocol`; qits-ci-service depends on it. |
| `ci-daemon/` | The binary. A Quarkus command-mode app — no web stack, it dials out and never listens — compiled to a fully static musl native image. |

Inside `ci-daemon/`, `Main` is the only CDI bean: it resolves configuration and news up plain
classes with plain constructors — `DaemonMain` (the flow), `ControlSocket` (the dial),
`MirrorWorktree` (mirror + worktree), `StepProcess` (each step's child), `Telemetry` (the export to
qits-observability, below). One reader per setting, which is what
stops two components from resolving the same key differently, and a suite that can drive the whole
flow against a real socket without a container.

`ci-daemon-protocol` is **framework-free**: no Quarkus, no CDI, no JAX-RS, no Jackson — a plain jar
of records with plain constructors. That is not stylistic. The shipping form of this daemon is a
fully static musl native image, so every dependency is a decision about image size and about what the
GraalVM builder has to be told; and the module is a released jar on qits-ci-service's classpath, where
a framework dependency would arrive as a second opinion about how a ci service is wired — and, since
that service pins this jar to pin the daemon, would arrive on every host that ever bumps the daemon.

## The protocol module is a published artifact, and its version is the binary's version

`ci-daemon-protocol` is deployed by this repository's release pipeline as
`eu.wohlben.qits:qits-ci-daemon-protocol`, at the same version as the binary the same release PUTs
to `…/artifacts/daemons/qits-ci-daemon/<version>`. It carries `CiDaemonBinary.VERSION`, which the
build filters out of `${project.version}` — so the jar's version and the binary's version are one
string by construction, and there is no second place for them to disagree.

That exists to answer one question: **who decides which daemon binary a CI lease downloads.**
It used to be the platform underneath qits-ci — `QITS_CI_DAEMON_VERSION`, a configuration entry
rewritten the instant a version was published, with an adoption probe layered on top that launched
whatever had just landed and kept it if the container dialled. Both of those move a version into a
running service without any build ever having put the two sides together: one is a string swapped
under a deployment, the other is a probe the released service performs on itself in production. The
consequence was that a protocol break was discovered by a real run, in a throwaway container, after
the release.

Now qits-ci pins the jar in its pom and reads the daemon version off the constant. Moving the daemon
version is an ordinary dependency bump on a branch — the same act as moving any internal library —
which means a change to the wire contract reaches qits-ci only through qits-ci's own release
request: gated by its fold, proven before the merge by an integration test that downloads and runs
*this* binary at *that* version against its own host, and approved by a person against the diff that
ships it. A daemon that broke the wire fails a gate instead of a production step.

Two consequences worth stating, because they are the ones that bite:

- **The pipeline publishes in one order and it is not arbitrary.** The binary goes to the `daemons`
  store first and the jar is deployed second, because the jar is what tells qits-ci which binary to
  fetch — a jar that resolves before its binary exists is a pin pointing at nothing.
- **Nothing here reaches qits-ci by releasing.** Publishing a version makes it *available*; a bump
  in qits-ci's pom is what makes it *used*. A release of this repository that nobody bumps changes
  no running step, which is the property the old configuration entry did not have.
- **A release publishes what `main`'s recipe says, not what the released tree says.** qits-ci reads
  `.config/qits/ci-event-*.yml` at this repository's `main` head and takes only the branch and sha
  from the release, so a version tagged before the recipe reaching `main` is built by the *old*
  pipeline. Measured here on 2026-09-17: `2026.917.31410` carried the jar step and published no jar,
  which was expected; `2026.917.33716` carried the jar itself and *also* published none, because its
  build was decided while `main` still ended at the release before it. The rule is not "budget one
  extra release" but "do not start the next release until the recipe is on `main`" — check with
  `git log origin/main` rather than assuming the previous release finished landing.

## The boundary

**qits-ci never executes anything.** It asks for leases; that is all. A step's script reaches a
container only as a frame on the socket that container's daemon dialled, and executes only as that
daemon's child. This binary is the other half of that rule: it is the process that *does* run
repo-controlled code, and it runs it inside the sandbox the target built — `--cap-drop=ALL`,
`no-new-privileges`, resource caps, no docker socket.

The consequence runs the other way too. From the moment a step's script starts, everything this
daemon sends is attacker-influenceable data about the run. The host records it and never trusts it —
timestamps are host-stamped at message receipt rather than daemon-reported.

## The lifecycle

Told, never derived. The daemon parses nothing out of its environment and announces no address of
its own; it is handed everything before the socket exists:

| Env | What |
|---|---|
| `QITS_CI_DAEMON_URL` | The control socket, dialled **verbatim**. |
| `QITS_CI_DAEMON_ID` | The host-minted registration identity — carried in the `Hello` that opens every conversation. |
| `QITS_CI_REPOSITORY_URL`, `QITS_CI_BRANCH`, `QITS_CI_SHA` | What the mirror is cloned and fetched from, the run's branch (a label: a mirror carries every ref), and the commit the worktree is at. |
| `QITS_CI_RUN_ID` | The run. Its worktree is `/work/<runId>`; also a telemetry label and the trace id's seed. Absent or blank, the daemon exits 2 naming it. |
| `QITS_CI_PROJECT_ID`, `QITS_CI_REPO_NAME`, `QITS_CI_REPO_ID` | Which repository, in both coordinate systems: the public `(projectId, repoName)` pair names the mirror, `/cache/git/<projectId>/<repoName>.git`; on an id-addressed run the pair is empty — never absent — and the mirror is `/cache/git/_by-id/<repoId>.git`. Telemetry labels too. |
| `QITS_CI_STEP_INDEX`, `QITS_DOMAIN` | *Optional.* Telemetry labels only (below). A lease runs every step, so the step index is normally absent; each step's index travels in its `RunStep`. |
| `QITS_CI_GIT_CACHE_DIR`, `QITS_CI_WORK_DIR` | *Optional.* `/cache/git` and `/work` unless set — keys for the suite, constants in a lease. |
| `QITS_CI_TELEMETRY_URL` | *Optional.* The OTLP endpoint (below). Unset derives it; set but empty switches the export off. |
| `CI`, `QITS_CI` | Both `true`, set for the step script's benefit rather than the daemon's. |
| `QITS_TOKEN` | The run's own `ci-run` token, presented as the handshake's bearer (below). Absent or blank, the daemon exits 2 naming it. |
| `QITS_TOKEN_SUBJECT` | *Optional.* Names the token in one log line so a container can be matched to its run. Never sent. |

Then, over one WebSocket dialled outbound — **there is no inbound listener in the container at all,
at any stage** — with its credential in one handshake header and its launch id in the first frame:

| Where | What |
|---|---|
| `Authorization: Bearer $QITS_TOKEN` | sent, with the launch id (`QITS_CI_DAEMON_ID`) in the `Hello` that is the first frame |

The token is the run's own and opens exactly what the run may do — it is commissioned when the run
is claimed and deleted when it closes, so it is not an identity the daemon asserts but a credential
the edge validates. The token is worth exactly one run; what remains to establish is only *which
launch* of that run this container is, and a launch id is not a secret. So the `Hello` this daemon
sends first names the launch in its `daemonId` field, and qits-ci matches it against the token's
subject (`CiDaemonRegistry.admitByToken`) rather than against a header.

The header-asserting handshake and the per-container secret were removed in the release carrying
qits-514 (2026-09-30).

A `wss://` url is dialled over TLS against the binary's **own** trust store, never the step image's:
the image is arbitrary and its `/etc/ssl` may be anything. In the native image that store is the
build JDK's `cacerts`, which GraalVM embeds at build time, and `quarkus.ssl.native=true` in
`application.properties` is what stops Quarkus substituting a disabled `SSLContext` — that file
carries the reasoning. The edge's certificate therefore has to chain to a public root.


Capability version 2 (qits-1541). One lease, one daemon, every step:

    dial → Hello{capabilityVersion: 2} → Ack → AckReceived
         → mirror + worktree → Initialized
         → ( RunStep{stepIndex, script, timeoutSeconds, env} → StepChunk* → StepFinished{stepIndex, exitCode, timedOut} )*
         → Close → git worktree unlock + remove --force → flush telemetry → socket closed → exit 0

`Heartbeat` runs underneath from dial to close. Failure to make the worktree replaces `Initialized`
with `InitFailed`, whose `reason` is the structured signal that retired qits-ci's old
prelude-sentinel inference. Capability 1 — one container, one step, one shallow clone — is gone, not
kept beside it: qits-ci pins this daemon by the protocol jar's version, so an old service keeps
fetching an old binary and there is no dual mode.

**The worktree.** Under an exclusive lock on `/cache/git/<projectId>/<repoName>.lock` (a `fcntl`
lock taken by the daemon itself, `GitLock`): `git clone --mirror` on the target's first run of the
repository and `git fetch --prune origin` on every later one, then `git worktree add --lock --reason
"qits-ci run <runId> <ISO instant>" --detach /work/<runId> <sha>`. Submodules are `git submodule
update --init --depth 50`, each with `--reference /cache/git/<projectId>/<submodule>.git` — a
submodule's mirror that is missing is created first, under its own lock, and one that cannot be is
simply not referenced — and each from the repository of the same name beside `QITS_CI_REPOSITORY_URL`,
never from the GitHub url the estate's `.gitmodules` commit. Credentials
are the bootstrap's: it writes a git credential helper answering with `$QITS_TOKEN`, and every git
call inherits it. A mirror clone or fetch the git host answers with a transient failure (a 5xx, a
dropped connection) is retried after 2s and 5s (qits-1253). A sha the fresh mirror does not hold is
`SHA_GONE`; anything else that fails is `CLONE_FAILED`.

**Why the worktree is locked (qits-1541).** Every ci-slot container on a target has its own `/work`
and shares `/cache/git`, so from inside one container every other run's worktree directory is
missing, and a missing directory is exactly what `git worktree prune` deletes an entry for. A bare
prune before the add took every other live run's `worktrees/<id>` — submodule gitdirs included,
since they live under its `modules/` — and six runs started together on one target all failed at
the submodule clone. The lock is taken by the add itself, prune skips a locked entry, and nothing
here ever runs a bare prune again: Close unlocks and removes this run's entry alone. A container
killed before its Close leaves a locked entry behind, so the next run on the mirror unlocks and
removes any whose qits-ci lock reason is older than `qits.ci.worktree-stale-after-seconds` (a day:
far past the longest lease, because taking a live entry breaks that run) and runs `git worktree
prune --expire` with the same bound for the unlocked entries an older daemon left.

**The steps.** `RunStep`s run one at a time, in the order they arrive, on one lane that also runs the
initialization and the teardown — so a step never shares the worktree with another and the worktree
is never removed under one. Each runs with `/work/<runId>` as its cwd, under `setsid` (when the image
has it) as the leader of a process group of its own, with the step's `env` laid over the lease's
environment for that child only. `StepChunk.seq` is one counter for the whole lease. A `RunStep`
whose `stepIndex` is not above the last one accepted is a replay and is ignored. `Cancel` kills the
running step's whole process group — SIGTERM, `qits.ci.step-kill-grace-ms`, SIGKILL — and the step
still finishes with a `StepFinished`; whatever a finished step left running in its group is killed
with it, so the next step inherits nothing.

**The ending.** `Close` kills a step still running (its `StepFinished` is still sent), removes the
worktree — `git worktree unlock`, `git worktree remove --force`, and a delete of the directory and of
this run's own `worktrees/<id>` for what git refuses (under the mirror's lock again) — flushes the telemetry,
closes the socket and exits 0. There is no answer frame; qits-ci deletes the lease after the socket
closes. Every other ending after initialization removes the worktree too.

A socket that drops after connecting — every lease's does when qits-edge is redeployed — is
re-dialled with the same token within `qits.ci.reconnect-budget-ms`. The steps keep running; the
daemon says `Hello` again on the new socket, and the host's `Ack` releases what the outage held, in
the order it was produced: an `Initialized` that failed to send, every step's chunks (about 1 MiB,
oldest dropped first, `seq` unchanged) and every `StepFinished` (never dropped). A second `Ack` does
not make the worktree again.

**The workspace daemon never exits; this daemon always exits.** That is the one deliberate inversion
of the precedent it otherwise mirrors. Any terminal condition — `Close`, `InitFailed` sent, an `Ack`
carrying a capability version this binary does not know, or dial failure after a short capped retry
— ends the process. Exit code 0 only on the clean path:

| Exit | Ending |
|---|---|
| 0 | The host sent `Close` and the lease was torn down. The only clean one; the steps' own exit codes travel in their `StepFinished`s. |
| 2 | The env contract was not satisfied; nothing was dialled. |
| 3 | No connection within the dial budget (~30s total, not infinite). |
| 4 | The `Ack` carried a capability version this binary does not know. |
| 5 | `InitFailed` delivered — the daemon did its job, no step ran. |
| 6 | The socket dropped and the reconnect budget (~90s) ran out before the host closed the lease. |
| 7 | `Cancel` before any step was run. |

The socket already carried whether the daemon behaved; the exit code is about whether the
*container* did what it was started for, which is why 5 is not a zero.

## Telemetry

The daemon ships its own log, its step's output and the step's spans to qits-observability, so a
step's work can be found in `qits observe --filter 'service=qits-ci-daemon'` rather than only in the
output tail qits-ci keeps. OTLP/HTTP protobuf, hand-encoded (`Otlp`, copied from
qits-ci-runner-daemon's `OtlpLogs` and extended to traces): the SDK and a protobuf runtime are
megabytes and reflection configuration in a static musl binary, and qits-observability takes nothing
but protobuf. No new dependency — the POSTs ride the vertx-core client the socket already brings,
and `quarkus.ssl.native=true` already covers `https`.

**Where.** A step runs on a runner's default docker bridge, outside the swarm, so the platform's
internal aliases are unreachable; the route is the public edge. The endpoint is resolved once:

| `QITS_CI_TELEMETRY_URL` | Endpoint |
|---|---|
| unset | derived from `QITS_CI_DAEMON_URL`: `wss://ci.<rest>/ci/daemon` → `https://observability.<rest>/observability/api/otel` (`ws://` → `http://`); a daemon url whose host is not `ci.…` derives none, and the export is off |
| set | that url |
| set but empty | off |
| not an http(s) url | off, with one line saying so — telemetry never fails a step |

`/v1/logs` and `/v1/traces` are appended. The variable is read raw, not through config, because
SmallRye reads an empty value as no value and "empty means off" would be lost; the config key
`qits.ci.telemetry-url` is the fallback when the variable is absent. The bearer is `$QITS_TOKEN`:
the edge admits it like every platform token, the ingest route asks for no role, and it lives as long
as the run, so there is nothing to rotate.

**What.**

- *The daemon's own log*, through a JUL handler on the root logger: `publish` is a bounded `offer`
  and nothing else, a full queue drops its oldest line and ships the count as a line, a body is cut
  at 16 KiB keeping its head, and a timer sends one batch at a time, dropping (not retrying) a batch
  that fails.
- *The steps' output*, one record per line — INFO for stdout, WARN for stderr, attribute
  `qits.ci.stream=out|err` — split out of the chunks (which are not lines) with partial lines carried
  over. Only the **latest** `qits.ci.telemetry-output-max-lines` (200) across the lease's steps are
  kept, because the failure is at the end, and `step output: N lines, M not exported` is shipped when the cap bit. This is
  load-bearing: every step on the estate shares one `qits-ci-daemon` bucket of a few thousand
  records, and one chatty build must not evict everyone else's. Output has a buffer of its own and is
  sent only at the flush, so it can never displace one of the daemon's own lines. qits-ci still gets
  all of it as chunks.
- *Spans*: a root `ci-daemon lease` from the daemon's start to the flush, and children `dial` (first
  connect → `Ack`), `checkout` (mirror + worktree) and one `script` per step (spawn → exit, with
  `qits.ci.step_index`, `qits.ci.exit_code`, `qits.ci.timed_out` and, when it was,
  `qits.ci.cancelled`). ERROR, with the reason as the status message, on a non-zero exit, a timeout,
  `InitFailed`, a failed dial, a capability mismatch and a lost socket; the root carries the first
  failing step's. The trace id is the first 16 bytes of SHA-256 over `QITS_CI_RUN_ID`. Log records
  carry the ids of the span they were written in.
- *Resource*: `service.name=qits-ci-daemon`, `service.version` (the binary's version),
  `qits.ci.run_id`, `qits.ci.step_index`, `qits.ci.repo_id`, `qits.ci.repo_name`,
  `qits.ci.project_id`, `vcs.ref.head.name`, `vcs.ref.head.revision`, `qits.domain` — each left out
  when its variable is absent or blank.

**No metrics.** One process runs one run, so a series would have a handful of points; the `script` span
already carries the duration and the exit code; and a series per run would churn
qits-observability's 500-series bucket for nothing.

**When: once, at the ending, bounded.** qits-ci deletes the lease — a SIGKILL, with no grace — once
the socket closes after `Close`, and ends it the moment an `InitFailed` arrives, so anything sent
after those races the kill. So every ending flushes **first**: the spans are ended, both buffers
drained, the final requests sent and their responses awaited — and only then is the socket closed
(after `Close`) or `InitFailed` handed to it. A `StepFinished` is not an ending any more and does
not wait for a flush. The flush is bounded by `qits.ci.telemetry-flush-ms` (3000): an unreachable,
silent or failing collector delays the exit by at most that, and never changes a step's result or
the exit code. It is a future composed in front of the close, bounded by a Vert.x timer, not a wait
— the flush's own requests and the socket need the event loop, and nothing parks it. It runs once.
The endings with nothing left to say to the host (exits 3, 4, 6, 7) flush the same way before they
exit; nothing waits for those, so best effort is the contract. A misconfigured daemon (exit 2) exports nothing, because the
address may be the thing that is missing.

The daemon's last line before `ci-daemon exiting N` says how it went — it is what a `docker logs`
tail of the lease's container shows:

    telemetry: submitted 37 logs, 4 spans (0 dropped)
    telemetry: NOT submitted (timeout after 3000 ms)
    telemetry: off

A JVM shutdown hook runs the same bounded flush if none has run, which covers a SIGTERM or SIGINT (a
manual `docker stop`, a Ctrl-C). It is not the mechanism and cannot be: Quarkus closes Vert.x from
its own hook concurrently, so it is best effort, and **a lease deleted is a SIGKILL that no hook
sees** — anything the daemon has not flushed by then is gone. That is why the flush sits in front of
the close rather than behind the exit.

## The image contract

The daemon is **not** baked into an image. A CI lease runs `qits/build-images/ci-slot` with a fixed,
host-authored bootstrap as its entrypoint, the same `BOOTSTRAP` shape as before: it downloads
`$QITS_CI_DAEMON_BINARY_URL` with `$QITS_TOKEN`, writes the git credential helper, `chmod +x`, and
`exec`s the binary. Nothing of the daemon is baked in, so the image and the daemon version move
independently.

What that asks of the image: **`git` and a downloader** (`wget` or `curl` — the bootstrap probes for
either), and, preferably, `bash` and `setsid` (util-linux). No `kill` binary: a step's process group
is signalled through the step shell's builtin `kill`, because Debian's binary is procps' and the
ci-slot image has no procps (qits-1541). What it asks of this repo: the binary must be a **fully static musl build**, because a glibc-linked one dies on every
alpine-family image, and it takes no arguments but `--version` — the environment above is its whole
input.

`git` is probed with `--version` before the mirror, so an image that does not satisfy the contract
reports `InitFailed{TOOLING_MISSING}` rather than failing halfway through a step.

**`bash` is preferred but not required.** It is probed at the same moment — before the mirror rather
than at step time, so a broken image is still reported as a broken image — and an image that has it
runs every step's script under it. An image without it runs the scripts under `sh`. **`setsid` is
probed too**: with it each step is the leader of its own process group, which is what lets `Cancel`,
a timeout and a finished step's cleanup reach its background children; without it only the shell is
signalled.

That distinction is not a nicety, and it was bought. Demanding bash refused an entire class of image
for no capability: `docker:28-dind` carries docker, git and wget but no bash, and it is the only
upstream image that can build the platform's own `qits/build-images/*` step images. So those images
were buildable only from an image that was itself one of them — and on 2026-08-20 a `docker system
prune` on a full disk deleted all five, leaving no run anywhere on the platform able to rebuild them.
Requiring a shell every container already has, rather than a specific one, is what closes that.

## Building the binary

    docker build -t qits/graalvmce-musl-builder:jdk-25 -f docker/Dockerfile.musl-builder docker/
    ./mvnw -B -ntp -pl ci-daemon -am package -Dnative -DskipTests

The result is `ci-daemon/target/qits-ci-daemon` (~43 MB, stripped), and it is the direct output that
`$QITS_CI_DAEMON_BINARY_URL` serves — no image, no wrapper.

**CI does not build it that way.** `docker/Dockerfile` is the shipping form: it compiles inside the
toolchain image as a build stage and ends on a `FROM scratch` holding the one file, so both pipelines
are `buildctl build --opt target=binary --output type=local,dest=out` on the platform's own
buildkitd, and the binary comes back as `out/qits-ci-daemon`. That file replaced a hand-rolled
builder container — build the image, `docker create`, `docker cp` the source in, `docker start -a`,
`docker cp` the binary out — five verbs that existed only because a step container holding the host
daemon's socket cannot bind-mount its own workspace. The wrapper's `qits-buildkit-plan.md` is the
migration. The same file exports the CycloneDX document as a second stage, so a release's SBOM is a
cache hit rather than a second compile:

    docker build --target binary --output type=local,dest=out -f docker/Dockerfile .

The builder image is **not released by anything**, so whatever needs it builds it first;
`quarkus.native.builder-image.pull=missing` is what stops every build from trying to `docker pull`
it. CI pushes it to the platform registry under a fixed tag, because a `FROM` is resolved from a
registry and from nowhere else — it is a cache coordinate for one builder and not an artifact, and
nothing declares or deploys it. Its base is GraalVM CE and **not** the Mandrel image
qits-workspace-daemon builds on, because Mandrel ships static JDK libraries for glibc only and a
`--libc=musl` build dies in its first second. `docker/Dockerfile.musl-builder` carries the error
message and the rest of the reasoning, and stays a file of its own because its last stage must
remain the toolchain image: qits-bootstrap-cli builds it by default target, on a machine where no
platform exists yet. `ci-daemon/src/main/resources/application.properties` carries the four
properties.

Checking a build:

    $ file ci-daemon/target/qits-ci-daemon
    ...: ELF 64-bit LSB pie executable, x86-64, version 1 (SYSV), static-pie linked, stripped
    $ ldd ci-daemon/target/qits-ci-daemon
            statically linked

**`file` says `static-pie linked`, not `statically linked`** — GraalVM emits a position-independent
static executable. It is genuinely fully static; assert on `ldd`, or on `static-pie`, never on the
literal string `statically linked`.

Running the binary is the cheap check the test suite structurally cannot do, and the QA step does
both halves of it directly — the binary is static, so no container is needed (and a CI lease has no
docker to give one): `--version` must print `qits-ci-daemon <version>`, and a run with no
environment at all must print the name of the first missing variable and exit 2, not die resolving
config.

    $ ci-daemon/target/qits-ci-daemon --version
    qits-ci-daemon 2026.1023.120000
    $ env -i ci-daemon/target/qits-ci-daemon; echo $?
    ... ci-daemon cannot start: QITS_CI_DAEMON_URL is not set. Exiting.
    2

## Other repositories build on this image

CI pushes the builder image to
`registry.qits.$QITS_DOMAIN/$QITS_IMAGE_REPOSITORY/graalvmce-musl-builder:jdk-25` (`wohlben.eu` when
a qits-ci too old to inject `$QITS_DOMAIN` runs the recipe), and other repositories build `FROM` that
tag:

- **qits-artifacts-cli** passed it as `BUILDER_IMAGE` in its recipes. That repository is retired —
  its publish client now ships inside the `qits` CLI — so nothing of it names the tag any more.
- **qits-platform-access-cli** did the same in its first release. A fix builds its toolchain as
  stages of its own instead.

The tag exists in a platform's registry only after this repository's gate (a release-request fold)
or release has run. The bootstrap builds the image into the host's image store and does not push it.
So on a freshly booted platform, a release that builds `FROM` the tag fails with
`graalvmce-musl-builder:jdk-25: not found` until a qits-ci-daemon fold has run. That happened on
2026-09-12: qits-ci-daemon had not run since the 2026-09-10 clean boot.

What follows:

- On a cold platform, run a qits-ci-daemon fold (open a release request for it) before you release
  anything that builds `FROM` the tag.
- Keep `push=true` on the builder build in both recipes. Without it the tag never reaches the
  registry and nothing else can build on it.

## Relationship to qits-workspace-daemon

The same recipe, at one-fifth the size:
[qits-workspace-daemon](https://github.com/QuicklyIterateTheSoftware/qits-workspace-daemon) earned
the outbound-only shape, the protocol-module pattern, and the clone-alone rule, and this repo spends
them rather than re-deriving them. It is **not** a reuse of that protocol: the messages are disjoint,
and coupling the two wire contracts would make every workspace capability bump a ci event.
