# Convenience targets. Requires: uv, (optional) dotnet, cmake + apache-arrow for C++.
.PHONY: help setup data data-small data-medium data-large serve flight \
        demo-arrow demo-json demo-proto bench \
        client csharp-build csharp-run cpp-build cpp-run clean clean-data

ARROW_PREFIX := $(shell brew --prefix apache-arrow 2>/dev/null)

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

setup:           ## Install the minimal core server (4 deps)
	uv sync

data-small:      ## Generate the small sample dataset (needs [gen])
	uv run --extra gen kob-gen --scale small
data-medium:     ## Generate the medium sample dataset (needs [gen])
	uv run --extra gen kob-gen --scale medium
data-large:      ## Generate the large sample dataset (needs [gen])
	uv run --extra gen kob-gen --scale large
data: data-small ## Alias for data-small

serve:           ## Run kob: Arrow Flight (:8815) + Swagger UI (http://localhost:8000/docs)
	uv run kob
flight:          ## Run Flight only, standalone, on :8815 (no Swagger)
	uv run kob-flight
demo-arrow:      ## Run the Arrow-IPC-over-HTTP demo server on :8001
	uv run kob-demo-arrow
demo-json:       ## Run the REST/JSON demo server on :8002
	uv run kob-demo-json
demo-proto:      ## Run the gRPC/Protobuf demo server on :8816 (needs [bench])
	uv run --extra bench kob-demo-proto

bench:           ## Benchmark Flight vs HTTP-Arrow vs Proto vs REST/JSON (needs [bench])
	uv run --extra bench kob-bench --out docs/BENCHMARKS.md

client:          ## Example query against a discovered dataset (needs [client])
	uv run --extra client kob-client --dataset optionmetrics --filter 'underlying:=:AAPL' --filter 'year:=:2023' --limit 10

csharp-build:    ## Build the C# client
	dotnet build clients/csharp -c Release
csharp-run:      ## Run the C# client over Flight (server must be up)
	dotnet run --project clients/csharp -c Release -- --transport flight --dataset optionmetrics --underlying AAPL --year 2023

cpp-build:       ## Build the C++ client
	cmake -S clients/cpp -B clients/cpp/build -DCMAKE_PREFIX_PATH=$(ARROW_PREFIX) -DCMAKE_BUILD_TYPE=Release
	cmake --build clients/cpp/build -j
cpp-run:         ## Run the C++ client over Flight (server must be up)
	clients/cpp/build/arrow_client --transport flight --dataset optionmetrics --underlying AAPL --year 2023

clean:           ## Remove build artifacts
	rm -rf clients/cpp/build clients/csharp/bin clients/csharp/obj
clean-data:      ## Remove generated data
	rm -rf data/georev data/optionmetrics data/MANIFEST.json
