.PHONY: help build run test cluster-start cluster-status cluster-stop cluster-clean open-ui clean

# Default binary name
BIN := nexus-server

# Colors for terminal output
CYAN  := \033[36m
GREEN := \033[32m
YELLOW:= \033[33m
RESET := \033[0m

help: ## Show this help message
	@echo ""
	@echo "$(CYAN)Nexus KV & Raft Cluster Management$(RESET)"
	@echo ""
	@echo "Usage: make $(GREEN)<target>$(RESET)"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(GREEN)%-18s$(RESET) %s\n", $$1, $$2}'
	@echo ""

build: ## Build the standalone Nexus binary with embedded web UI
	@echo "$(CYAN)Building $(BIN)...$(RESET)"
	@go build -o $(BIN) main.go
	@echo "$(GREEN)Build complete: ./$(BIN)$(RESET)"

run: build ## Run a standalone single node on :8080
	@echo "$(GREEN)Starting Nexus standalone on http://localhost:8080$(RESET)"
	@./$(BIN) wal.log

test: ## Run unit and race tests across all packages
	@echo "$(CYAN)Running tests with race detector...$(RESET)"
	@go test -v -race ./...

cluster-start: build ## Start a 3-node Raft consensus cluster in the background (:8001, :8002, :8003)
	@echo "$(CYAN)Stopping any previously running cluster nodes...$(RESET)"
	@-pkill -f $(BIN) 2>/dev/null || true
	@mkdir -p /tmp/nexus_cluster
	@echo "$(GREEN)Launching Node 1 on :8001 (Leader candidate)...$(RESET)"
	@PORT=8001 NODE_ID=localhost:8001 PEERS=localhost:8002,localhost:8003 ./$(BIN) /tmp/nexus_cluster/n1.wal > /tmp/nexus_cluster/n1.log 2>&1 &
	@echo "$(GREEN)Launching Node 2 on :8002 (Follower replica)...$(RESET)"
	@PORT=8002 NODE_ID=localhost:8002 PEERS=localhost:8001,localhost:8003 ./$(BIN) /tmp/nexus_cluster/n2.wal > /tmp/nexus_cluster/n2.log 2>&1 &
	@echo "$(GREEN)Launching Node 3 on :8003 (Follower replica)...$(RESET)"
	@PORT=8003 NODE_ID=localhost:8003 PEERS=localhost:8001,localhost:8002 ./$(BIN) /tmp/nexus_cluster/n3.wal > /tmp/nexus_cluster/n3.log 2>&1 &
	@sleep 1.5
	@$(MAKE) cluster-status
	@echo ""
	@echo "$(YELLOW)Open http://localhost:8001 in your browser to view the live dashboard!$(RESET)"

cluster-status: ## Check the Raft role, term, and leader across all 3 nodes
	@echo ""
	@echo "$(CYAN)=== Raft Cluster Health & Roles ===$(RESET)"
	@for p in 8001 8002 8003; do \
		printf "Node on :%s -> " "$$p"; \
		curl -s --connect-timeout 1 localhost:$$p/raft/status 2>/dev/null || echo '{"status":"offline"}'; \
		echo ""; \
	done

cluster-stop: ## Gracefully terminate all running cluster nodes
	@echo "$(YELLOW)Stopping running Nexus nodes...$(RESET)"
	@-pkill -f "nexus-" 2>/dev/null && echo "$(GREEN)All cluster nodes stopped.$(RESET)" || echo "No nodes were running."

cluster-clean: cluster-stop ## Stop cluster and remove all temporary cluster WAL files
	@rm -rf /tmp/nexus_cluster
	@echo "$(GREEN)Cleaned cluster data in /tmp/nexus_cluster.$(RESET)"

open-ui: ## Open the Web Dashboard in your default browser
	@echo "$(GREEN)Opening http://localhost:8001 in browser...$(RESET)"
	@open http://localhost:8001 || xdg-open http://localhost:8001 || echo "Open http://localhost:8001 in your browser"

clean: cluster-clean ## Clean built binaries and local WAL files
	@rm -f $(BIN) nexus-cluster nexus-server wal.log *.log.snap *.snap.tmp
	@echo "$(GREEN)Clean complete.$(RESET)"

docker-up: ## Start the 3-node cluster using Docker Compose
	@echo "$(CYAN)Building and starting Nexus cluster with Docker Compose...$(RESET)"
	@docker compose up -d --build
	@echo "$(GREEN)Cluster launched via Docker Compose!$(RESET)"
	@echo "$(YELLOW)Nodes exposed on :8001, :8002, :8003. Open http://localhost:8001$(RESET)"

docker-down: ## Stop Docker Compose cluster and remove containers
	@echo "$(YELLOW)Stopping Docker Compose cluster...$(RESET)"
	@docker compose down
	@echo "$(GREEN)Docker containers stopped.$(RESET)"

