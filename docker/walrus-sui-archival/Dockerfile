# Build application
#
# Copy in all crates, Cargo.toml and Cargo.lock unmodified,
# and build the application.
FROM rust:1.91-bookworm AS builder
ARG PROFILE=release
ARG GIT_REVISION
ENV GIT_REVISION=$GIT_REVISION
WORKDIR "/tmp/walrus-sui-archival"
RUN apt-get update && apt-get install -y cmake clang

COPY Cargo.toml Cargo.lock ./
COPY crates crates
COPY resources resources

RUN cargo build --profile $PROFILE --bin walrus-sui-archival --bin caching-server

# Production Image for walrus-sui-archival
FROM rust:1.91-bookworm AS walrus-sui-archival
RUN apt-get update && apt-get install -y ca-certificates curl
# Use jemalloc for better memory management
RUN apt-get install -y libjemalloc2
ARG PROFILE=release
WORKDIR "/tmp/walrus-sui-archival"
COPY --from=builder /tmp/walrus-sui-archival/target/release/walrus-sui-archival /opt/walrus/bin/walrus-sui-archival
COPY --from=builder /tmp/walrus-sui-archival/target/release/caching-server /opt/walrus/bin/caching-server
COPY --from=builder /tmp/walrus-sui-archival/resources/walrus_image.png /opt/walrus/resources/walrus_image.png

ARG BUILD_DATE
ARG GIT_REVISION
LABEL build-date=$BUILD_DATE
LABEL git-revision=$GIT_REVISION
ENV PATH="/opt/walrus/bin:$PATH"
ENTRYPOINT ["/opt/walrus/bin/walrus-sui-archival"]