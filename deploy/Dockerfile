FROM docker.io/library/rust:1-bookworm AS build
WORKDIR /src
COPY . .
RUN cargo build --release --example wordcount --example sum

FROM docker.io/library/debian:bookworm-slim
COPY --from=build /src/target/release/examples/wordcount /usr/local/bin/wordcount
COPY --from=build /src/target/release/examples/sum /usr/local/bin/sum
ENTRYPOINT ["/usr/local/bin/wordcount"]
