export GOTOOLCHAIN := auto

.PHONY: up stop restart down logs test smoke simulator loadtest

up:
	docker compose up -d --build

stop:
	docker compose stop

restart:
	docker compose restart

down:
	docker compose down -v

logs:
	docker compose logs -f

test:
	go test -race ./...

smoke:
	@if command -v go >/dev/null 2>&1; then \
		go run ./scripts/smoketest; \
	else \
		docker compose --profile tools run --rm smoketest; \
	fi

simulator:
	@if command -v go >/dev/null 2>&1; then \
		go run ./scripts/simulator -devices=$(or $(DEVICES),20) -backfill-days=$(or $(DAYS),3) -year-devices=$(or $(YEAR_DEVICES),1); \
	else \
		docker compose run --rm --no-deps -e SIM_DEVICES=$(or $(DEVICES),20) -e SIM_BACKFILL_DAYS=$(or $(DAYS),3) -e SIM_YEAR_DEVICES=$(or $(YEAR_DEVICES),1) simulator; \
	fi

loadtest:
	go run ./scripts/loadtest
