.PHONY: all deps compile shell
.PHONY: up down
.PHONY: clean

SHELL := BASH_ENV=.rc /bin/bash --noprofile


## Targets
##=========================≈===============================================

all: deps compile

deps:
	mix deps.get

compile:
	mix compile

shell:
	iex -S mix

up:
	docker compose up -d

down:
	docker compose down

clean:
	mix clean
