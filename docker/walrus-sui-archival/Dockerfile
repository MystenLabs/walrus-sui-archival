# Build application
#
# Copy in all crates, Cargo.toml and Cargo.lock unmodified,
# and build the application.
FROM rust:1.89-bookworm AS builder
ARG PROFILE=release
ARG GIT_REVISION
ENV GIT_REVISION=$GIT_REVISION
WORKDIR "/tmp/walrus-sui-archival"
RUN apt-get update && apt-get install -y cmake clang

COPY Cargo.toml Cargo.lock ./
COPY crates crates

RUN cargo build --profile $PROFILE --bin walrus-sui-archival

# Production Image for walrus-sui-archival
FROM rust:1.89-bookworm AS walrus-sui-archival
RUN apt-get update && apt-get install -y ca-certificates curl
ARG PROFILE=release
WORKDIR "/tmp/walrus-sui-archival"
COPY --from=builder /tmp/walrus-sui-archival/target/release/walrus-sui-archival /opt/walrus/bin/walrus-sui-archival

ARG BUILD_DATE
ARG GIT_REVISION
LABEL build-date=$BUILD_DATE
LABEL git-revision=$GIT_REVISION
ENV PATH="/opt/walrus/bin:$PATH"
ENTRYPOINT ["/opt/walrus/bin/walrus-sui-archival"]