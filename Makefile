.PHONY: all deps setup compile shell test
.PHONY: up down
.PHONY: clean publish

## Targets
##=========================≈===============================================

all: setup deps compile

setup:
	mise install
	mise setup

deps:
	mix deps.get

compile: deps setup
	mix compile

test:
	mix test

shell:
	iex -S mix

up:
	docker compose up -d

down:
	docker compose down

clean:
	mix clean

publish: deps
	mix local.hex --force
	mix hex.publish --yes
