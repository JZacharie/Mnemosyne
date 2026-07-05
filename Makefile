.PHONY: build run test lint clean docker-build seed fmt ci

IMAGE_NAME ?= mnemosyne
TAG ?= latest

build:
	cargo build --release

run:
	cargo run

seed:
	cargo run --bin seed

test:
	cargo test

fmt:
	cargo fmt

lint:
	cargo fmt --all --check && cargo clippy -- -D warnings

ci:
	./local-ci.sh

docker-build:
	docker build -t $(IMAGE_NAME):$(TAG) .

clean:
	cargo clean
