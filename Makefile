.PHONY: all deps setup compile shell
.PHONY: up down
.PHONY: clean

SHELL := BASH_ENV=.rc /bin/bash --noprofile

ELIXIR_VERSION ?= $(shell source .elixir-version && echo $$ELIXIR_VERSION)
ERLANG_VERSION ?= $(shell source .erlang-version && echo $$ERLANG_VERSION)

PBP_VERSION ?= v3.3.2
PBP_URL     := https://raw.githubusercontent.com/apache/pulsar/refs/tags/$(PBP_VERSION)/pulsar-common/src/main/proto/PulsarApi.proto
PBP_FILE    := pulsar.proto
PBP_ELIXIR  := lib/pulsar/protocol/binary.ex

PBC_ELIXIR_PLUGIN := escripts/protoc-gen-elixir


## Targets
##=========================≈===============================================

all: deps compile

setup: $(PBP_FILE) $(PBP_ELIXIR)

$(PBP_FILE):
	curl -o $(PBP_FILE) $(PBP_URL)

protoc:
	docker build --build-arg ELIXIR_VERSION=$(ELIXIR_VERSION) --build-arg ERLANG_VERSION=$(ERLANG_VERSION) . -t protoc

$(PBC_ELIXIR_PLUGIN):
	mix escript.install hex protobuf --force

$(PBP_ELIXIR): $(PBC_ELIXIR_PLUGIN) protoc
	protoc --plugin=$(PBC_ELIXIR_PLUGIN) pulsar.proto --elixir_out=./lib --elixir_opt=package_prefix=pulsar.protocol.binary
	mv lib/pulsar.pb.ex $(PBP_ELIXIR)

deps:
	mix deps.get

compile: deps setup
	mix compile

shell:
	iex -S mix

up:
	docker compose up -d

down:
	docker compose down

clean:
	mix clean
