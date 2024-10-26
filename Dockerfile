ARG ELIXIR_VERSION=1.17.3
ARG ERLANG_VERSION=27

FROM elixir:${ELIXIR_VERSION}-otp-${ERLANG_VERSION}

RUN apt-get update && \
    apt-get install -y protobuf-compiler && \
    rm -rf /var/lib/apt/lists/*
