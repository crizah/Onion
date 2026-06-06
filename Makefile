DB_URL         ?= postgres://postgres:password@localhost:5432/onion_dev?sslmode=disable
MIGRATIONS_DIR  = migrations
BINARY          = onion.exe

build:
	go build -o $(BINARY) main/main.go

run:
	./$(BINARY)

# Diffs models/ against the current DB and generates a migration file.
# Atlas spins up a temporary Docker container as scratch — no second DB needed.
# usage: make migrate-create name=add_column_foo
migrate-create:
	atlas migrate diff $(name) \
		--dir "file://$(MIGRATIONS_DIR)" \
		--to "file://models" \
		--dev-url "docker://postgres/15/dev"

migrate-up:
	atlas migrate apply \
		--dir "file://$(MIGRATIONS_DIR)" \
		--url "$(DB_URL)" \
		--revisions-schema public

migrate-down:
	atlas migrate down 1 \
		--dir "file://$(MIGRATIONS_DIR)" \
		--url "$(DB_URL)" \
		--dev-url "docker://postgres/15/dev" \
		--revisions-schema public

migrate-status:
	atlas migrate status \
		--dir "file://$(MIGRATIONS_DIR)" \
		--url "$(DB_URL)" \
		--revisions-schema public

.PHONY: build run migrate-create migrate-up migrate-down migrate-status
