# qits-ci-daemon — working notes

Read `README.md` first: it defines the boundary, the module layout, and the image contract. This file
is the working conventions on top of it.

## The two rules that shape everything

**A clone of this repo alone builds and tests green.** No monorepo, no docker, no prior `mvn install`
elsewhere, no credentials, no network. `./mvnw verify` is the gate. That is why the protocol module
is self-contained, why the reactor lists only modules that exist, and why nothing in the suite shells
`docker`.

**It compiles to a fully static musl GraalVM native image.** Every dependency is a decision about
image size and about what the builder has to be told — and here also about whether it links anything
glibc-only, because a glibc-linked binary dies on every alpine-family image and alpine-family images
are half of what a repo declares. Before adding one, check whether something already in the image
does the job — `io.vertx.core.json` instead of Jackson, `ProcessBuilder` instead of a process
library, `java.lang.foreign` instead of JNA (and see qits-workspace-daemon's `AGENTS.md` on how an
FFM downcall has to be registered by hand — there should be no reason for one here: a step's script
gets pipes, not a terminal).

**An empty `defaultValue` is not a default.** SmallRye reads `@ConfigProperty(name = "…",
defaultValue = "")` as *no value* and then fails to resolve a plain `String`, so the binary dies at
startup with `Failed to load config value of type class java.lang.String for: <key>`. Optional
settings are `Optional<String>`. The suite cannot see this — `docker run` on the image with no
environment is what catches it.

## Module conventions

`eu.wohlben.qits.cidaemon.*`, one sub-package per module, no split packages.

The capability modules — today `ci-daemon-protocol`, and anything extracted beside it — are
**framework-free**: plain classes with plain constructors and no annotations. The application module
news them up.

**`Main` is the only CDI bean**, and the application module holds itself to the same standard the
capability modules are held to: `DaemonMain`, `ControlSocket`, `Workspace` and `StepProcess` are
plain classes taking everything they need as constructor arguments. `Main` resolves configuration,
constructs them, and hands its exit code back to the runtime. (The workspace daemon keeps its socket
and its API as beans; it has more of them and a longer life. Here there is one flow and it ends, so
there is nothing a bean would buy — and the whole flow being constructible by hand is what lets
`DaemonMainTest` drive it against a real socket with no container.)

That has a consequence worth stating plainly: **a capability module cannot read configuration.** Every
setting it needs arrives as a constructor argument from the one class that resolves it. Do not reach
for `ConfigProvider` to get around this.

## Testing

Plain JUnit 5. **No Mockito, no `@QuarkusTest`** — neither is used anywhere in this repo and neither
should start being. Fakes are anonymous classes or nested records implementing a seam interface.

Real processes and real sockets are preferred over seams where the thing under test *is* the
integration: a step-process test drives a real `bash`, a dial test binds a real Vert.x server on an
ephemeral port. Anything OS-dependent is `@EnabledOnOs(OS.LINUX)`.

Test names are sentences describing the behaviour, not the method
(`aStepKilledAtItsDeadlineReportsTimedOutRatherThanItsExitCode`).

## Adding a control-socket message

1. A record in `ci-daemon-protocol`, added to the `CiDaemonMessage` permits list.
2. `Type` and `Field` constants — never a bare string at a call site.
3. Encode and decode arms in `CiDaemonCodec`.
4. A round-trip case in `CiDaemonCodecTest`.
5. Bump `CiDaemonProtocol.CAPABILITY_VERSION`.
6. **Mirror the whole module into qits-ci**, byte-identical, and handle the new case in its
   `CiDaemonRegistry`. `CiDaemonCodecTest` living in both copies is the drift detector; `diff -r` the
   two `src/` trees before you push.

**This repo is the protocol's only author.** qits-ci *copies* — it never edits its vendored copy, not
even for a one-line fix, not even to unbreak its own build. A correction discovered while working in
qits-ci comes back here as a commit and goes over as a re-vendor; that round trip is slower than the
edit and it is the whole point. The workspace pair drifted exactly once, by exactly that shortcut,
and the two sides then disagreed about a field for as long as nobody diffed them.

    diff -r ci-daemon-protocol/src <qits-ci>/services/qits-ci/ci-daemon-protocol/src

Prefer extending an existing message to minting a new one, and prefer an enum constant to a free-text
field: `InitFailed.reason` is the shape to copy — three values the host branches on, with the human
detail in a separate bounded string that nothing parses.

Neither direction of a version skew degrades gracefully here, and that is deliberate. A container
lives for one step; there is nothing to fall back to and no session to preserve. A daemon that reads
an `Ack` carrying a version it does not know exits nonzero, the run records that, and the operator
reads one honest log line instead of debugging a half-understood conversation. qits-ci pins the
daemon version per run, so the only way to see a skew at all is a deploy racing a run.

## Untrusted input

The checkout is untrusted and the step's script *is* the untrusted thing — it arrives from a repo,
over the socket, and this daemon runs it on purpose. That is the design, not a hole. What follows
from it:

- The script is passed to `bash -c` as **one argv element**, never assembled into a shell line with
  anything else interpolated around it.
- Nothing the script writes is read back as instructions. Output is bytes to be chunked and
  forwarded, bounded at the source; a marker string in stdout must never mean anything to this
  daemon or to the host. (qits-ci's old runner inferred setup failure from a sentinel in the output
  tail, so a step that merely echoed the sentinel looked like a broken clone. `InitFailed` exists so
  that inference is gone.)
- The daemon reports facts about a process it owns and nothing about the world — no clocks, no
  addresses, no identity it was not handed. The host stamps its own timestamps for exactly this
  reason, and re-sending a value the host already knows is not a favour.
- Values that cross from the checkout into a path or an argv are validated at the boundary, with a
  comment saying why.

## Formatting

`google-java-format`, 100 columns, two-space indent. Javadoc explains *why* — the tradeoff, the
alternative rejected, the failure it prevents. What the code does is the code's job.
