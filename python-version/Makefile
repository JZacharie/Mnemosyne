.PHONY: setup build run test clean

IMAGE_NAME ?= mnemosyne-indexer
TAG ?= latest

setup:
	pip install -r requirements.txt

build:
	docker build -t $(IMAGE_NAME):$(TAG) .

run:
	python indexer.py

db-setup:
	python setup_db.py

lint:
	black .
	flake8 .

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
