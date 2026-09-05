# HOW THE BINARY IS BUILT, now that the build runs on the platform's own BuildKit.
#
# This repository ships no image — the artifact is `ci-daemon/target/qits-ci-daemon`, a fully static
# musl binary that qits-artifacts serves as bytes and every step container downloads and execs. So
# the last stage here is a `FROM scratch` holding that one file, and what a build "produces" is an
# `--output type=local` of it. There is nothing to push and nothing to run.
#
# --- WHY THIS FILE EXISTS AT ALL, WHEN docker/Dockerfile.musl-builder ALREADY DID THE HARD PART ---
# Until the buildkit migration (qits-buildkit-plan.md in the wrapper) both pipelines hand-rolled the
# build: `docker build` the toolchain image beside this file, `docker create` a container from it,
# `docker cp` the checkout in, `docker start -a`, `docker cp` the binary and the SBOM back out. That
# shape existed because a step container holding the HOST daemon's socket cannot bind-mount its own
# /workspace — the path is resolved on the host, where it does not exist — so streaming the source
# in over the API was the only way to hand the builder a tree. The whole detour is a workaround for
# the socket, and the socket is what the migration removes: BuildKit takes a context, so a `COPY . .`
# in a build stage does what four `docker` verbs did, in one call, with a content-addressed cache
# underneath and no root-equivalence anywhere.
#
# THE TOOLCHAIN IMAGE STAYS ITS OWN FILE AND IS NOT INLINED HERE, and that is a constraint rather
# than a preference. `docker/Dockerfile.musl-builder`'s LAST stage must remain the toolchain image:
# qits-bootstrap-cli reads that file and builds it by default target, on a machine where no platform
# exists yet. Appending build stages there would move the default target to the binary and break the
# bootstrap. So the pipelines build that file first, push the result under a fixed tag, and this file
# names it — which is also why the builder image now lives in a registry after all, contrary to what
# README.md said about it: BuildKit resolves a `FROM` from a registry and from nowhere else, and the
# platform's own store is the right place for the one image that is not an artifact.
#
# THE DEFAULT NAMES THE LOCAL HAND-BUILT TAG, so a developer's build is one line and needs no
# platform. It is deliberately NOT a qits-maintenance pin: `qits/graalvmce-musl-builder` is released
# by nothing, follows nothing, and moves only when the file beside it changes.
#
#     docker build -t qits/graalvmce-musl-builder:jdk-25 -f docker/Dockerfile.musl-builder docker/
#     docker build --target binary --output type=local,dest=out -f docker/Dockerfile .
#
ARG BUILDER_IMAGE=qits/graalvmce-musl-builder:jdk-25

# --- stage `build`: the musl native compile -------------------------------------------------------
# /qits-build is where the hand-rolled container put the tree, kept verbatim so a log line from
# either shape reads the same and so qits-bootstrap-cli's own copy of this build stays comparable.
# USER root because the base image ends on 1001 and the checkout arrives owned by root.
FROM ${BUILDER_IMAGE} AS build
USER root
WORKDIR /qits-build

# The whole reactor, not a module list. `-am` builds ci-daemon's dependency from source —
# `ci-daemon-protocol` is a 1.0.0-SNAPSHOT this repo publishes nowhere — so a builder with an empty
# ~/.m2 has nowhere else to get it. `.dockerignore` is what keeps this cheap: without it the line
# drags in every target/ dir, and a stale one can shadow what this stage compiles.
COPY . .

# Where Maven Central is mirrored, when it is. CI passes the platform mirror's IN-NETWORK route
# ($QITS_MAVEN_PROXY_URL): a RUN executes in the builder's own network namespace on qits-net, where
# the edge vhost the old `--network host` reached does not resolve. EMPTY — the default, and every
# hand build — deactivates `.qits-maven-settings.xml`'s central-proxy profile and the resolve goes
# to Maven Central directly, exactly as it always did.
#
# NO CREDENTIAL RIDES ALONG, which is unchanged from the hand-rolled shape and still deliberate:
# platform reads are anonymous. If that policy ever tightens this build fails loudly and earns a
# secret mount then — in file form, like every sibling's.
ARG QITS_MAVEN_CENTRAL_URL=
ENV QITS_MAVEN_CENTRAL_URL=$QITS_MAVEN_CENTRAL_URL

# `quarkus.native.container-build=false`: we are already inside the container it would have launched,
# and it has no docker to launch another with.
#
# THE `ldd` ASSERTION RIDES ON THE SAME RUN because this is where the binary is, and it is `ldd`
# rather than `file` on purpose: GraalVM emits a position-independent static executable, so `file`
# says "static-pie linked" and never the literal "statically linked". A glibc-linked binary is
# useless here — it does not execute at all on an alpine-family image, and half of what a pipeline
# config declares is alpine-family — so the check belongs in the build rather than after it.
#
# `makeBom` RIDES ON THE SAME INVOCATION rather than getting a second one: a second `./mvnw` would
# be a second layer on top of a native compile and a re-resolve of the same reactor to produce a
# document the first run already had the model for. It runs on EVERY build, QA fold included, so the
# QA build and the release build are byte-identical LLB and the release is a stack of cache hits —
# and so a document that fails to generate fails the cheap gate rather than a release.
#
# cyclonedx.skipNotDeployed=false is LOAD-BEARING, measured in qits-artifacts-service on its first
# release: `ci-daemon` is <packaging>quarkus</packaging> and ships as this binary rather than as a
# jar, so it skips maven-deploy — and the plugin SKIPS deploy-skipping modules by default, with a
# green build and one INFO line. The `sbom` stage's COPY then fails on a file nothing wrote. The
# spelling matters too: a bare -DskipNotDeployed=false is silently ignored, and only the
# cyclonedx.-prefixed property reaches the plugin.
RUN ./mvnw -B -ntp -s .qits-maven-settings.xml -pl ci-daemon -am package -Dnative -DskipTests \
      -Dquarkus.native.container-build=false \
      org.cyclonedx:cyclonedx-maven-plugin:2.9.1:makeBom \
      -DoutputFormat=json -DoutputName=sbom -DschemaVersion=1.6 \
      -Dcyclonedx.skipNotDeployed=false \
 && ldd ci-daemon/target/qits-ci-daemon 2>&1 | grep -qE "statically linked|not a dynamic executable"

# --- sbom -----------------------------------------------------------------------------------------
# The ci-daemon module's CycloneDX document, exported so the release step can PUT it to the
# platform's SBOM store. A stage rather than a file the release step digs out of a container, and
# BEFORE the `binary` stage on purpose: the last stage is what a plain build produces, and that must
# stay the artifact. `.config/qits/ci-event-release.yml` reaches this one with `--opt target=sbom
# --output type=local`, using build-args byte-identical to the binary export, so every layer above is
# a cache hit and the export costs seconds rather than a second native compile.
FROM scratch AS sbom
COPY --from=build /qits-build/ci-daemon/target/sbom.json /sbom.json

# --- binary ---------------------------------------------------------------------------------------
# THE ARTIFACT, AND THE DEFAULT TARGET. A `FROM scratch` holding one file is how a build that
# produces bytes rather than an image says so: `--output type=local,dest=out` writes
# `out/qits-ci-daemon`, which is what the `docker cp` it replaces used to fetch out of a container.
# The path inside the stage has no directory component, so the export lands flat.
#
# THE EXPORT DIRECTORY IS NOT THE CHECKOUT, AND THAT IS NOT TIDINESS. The release pipeline runs a
# second build (`--opt target=sbom`) against the SAME context, and a build's own output landing in
# that context would change it — so the `COPY . .` above would hash differently and the second
# export would pay for a whole second native compile. `.dockerignore` excludes `out/` as the belt
# to that braces.
FROM scratch AS binary
COPY --from=build /qits-build/ci-daemon/target/qits-ci-daemon /qits-ci-daemon
