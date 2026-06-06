DB_URL     ?= postgres://postgres:password@localhost:5432/onion_dev?sslmode=disable
DEV_DB_URL ?= postgres://postgres:password@localhost:5432/onion_dev?sslmode=disable
MIGRATIONS_DIR = migrations
BINARY = onion.exe

build:
	go build -o $(BINARY) main/main.go

run:
	./$(BINARY)

# Diffs models/ against the current DB and generates a migration file.
# usage: make migrate-create name=add_column_foo
migrate-create:
	atlas migrate diff $(name) \
		--dir "file://$(MIGRATIONS_DIR)" \
		--to "file://models" \
		--dev-url "$(DEV_DB_URL)"

migrate-up:
	atlas migrate apply \
		--dir "file://$(MIGRATIONS_DIR)" \
		--url "$(DB_URL)"

migrate-down:
	atlas migrate down 1 \
		--dir "file://$(MIGRATIONS_DIR)" \
		--url "$(DB_URL)" \
		--dev-url "$(DEV_DB_URL)"

migrate-status:
	atlas migrate status \
		--dir "file://$(MIGRATIONS_DIR)" \
		--url "$(DB_URL)"

.PHONY: build run migrate-create migrate-up migrate-down migrate-status
