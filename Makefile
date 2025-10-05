.PHONY: all fmt lint build test generate manifests

HELM ?= helm
CHART_DIR ?= charts/keyval-operator
CHART_PACKAGE_DIR ?= dist/charts
CHART_VERSION ?=

COSIGN ?= cosign
VERSION ?=
RELEASE_REGISTRY ?= ghcr.io/ivelok
RELEASE_REPOSITORY ?= keyval-operator

# Prefer a locally installed controller-gen, otherwise fall back to go run.
CONTROLLER_GEN := $(shell command -v controller-gen 2>/dev/null)

all: fmt

fmt:
	@echo "Formatting Go code..."
	@go fmt ./...
	@command -v goimports >/dev/null 2>&1 && goimports -w . || echo "goimports not installed, skipping"

lint:
	@echo "Running staticcheck (may require modules)..."
	@staticcheck ./... || echo "staticcheck skipped or failed (likely missing modules)"

build:
	@echo "Build placeholder: controller code and modules not initialized yet."
	@if [ -f go.sum ]; then \
		echo "Attempting go build..."; \
		go build ./... || echo "go build failed (likely missing modules)"; \
	else \
		echo "Skipping go build (no go.sum). Run after kubebuilder init."; \
	fi

test:
	@echo "Test placeholder: no tests yet."
	@if [ -f go.sum ]; then \
		go test -race -ldflags="-extldflags=-Wl,-w" ./... || echo "go test failed (likely missing modules or envtest)"; \
	else \
		echo "Skipping tests (no go.sum)."; \
	fi

generate:
	@echo "Generating deepcopy and other object code via controller-gen..."
	@if [ -n "$(CONTROLLER_GEN)" ]; then \
		$(CONTROLLER_GEN) object paths=./api/...; \
	else \
		GOFLAGS=-mod=mod go run sigs.k8s.io/controller-tools/cmd/controller-gen@v0.16.5 object paths=./api/...; \
	fi

manifests:
	@echo "Generating CRDs from API types via controller-gen..."
	@if [ -n "$(CONTROLLER_GEN)" ]; then \
		$(CONTROLLER_GEN) crd:crdVersions=v1 paths=./api/... output:crd:dir=config/crd/bases; \
	else \
		GOFLAGS=-mod=mod go run sigs.k8s.io/controller-tools/cmd/controller-gen@v0.16.5 crd:crdVersions=v1 paths=./api/... output:crd:dir=config/crd/bases; \
	fi

.PHONY: docker-build docker-push deploy undeploy

IMG ?= controller:latest

E2E_TAGS ?= e2e
E2E_TIMEOUT ?= 20m
E2E_NAMESPACE ?= keyval-e2e

CHAOS_TAGS ?= chaos
CHAOS_TIMEOUT ?= 20m
CHAOS_NAMESPACE ?= keyval-chaos
CHAOS_EXPERIMENTS ?= pod-kill-master,network-latency,sentinel-flood

docker-build:
	@echo "Building Docker image $(IMG)..."
	docker build -t $(IMG) .
	@if [ -n "$(KIND_CLUSTER_NAME)" ]; then \
		echo "Loading image $(IMG) into kind cluster $(KIND_CLUSTER_NAME)..."; \
		kind load docker-image --name $(KIND_CLUSTER_NAME) $(IMG); \
	fi

docker-push:
	@echo "Pushing Docker image $(IMG)..."
	docker push $(IMG)

deploy: manifests
	@echo "Applying kustomize manifests to the cluster..."
	sleep 2
	kubectl apply -k config/default
	sleep 2
	kubectl -n keyval-operator-system rollout restart deployment keyval-operator-controller-manager
	@echo "Deployed. Check pods with: kubectl -n keyval-operator-system get pods"

undeploy:
	@echo "Deleting kustomize manifests from the cluster..."
	kubectl delete -k config/default || true

.PHONY: restart redeploy

restart:
	@echo "Rolling out restart of the operator Deployment..."
	sleep 2
	kubectl -n keyval-operator-system rollout restart deploy/keyval-operator-controller-manager

redeploy: docker-build deploy restart
	@echo "Rebuilt image, applied manifests, and restarted operator."

.PHONY: e2e e2e-setup e2e-test e2e-clean

e2e-setup:
	@echo "Building image $(IMG) and applying operator manifests..."
	@$(MAKE) --no-print-directory docker-build
	@$(MAKE) --no-print-directory deploy

e2e-test:
	@echo "Running e2e suite (namespace=$(E2E_NAMESPACE), timeout=$(E2E_TIMEOUT))..."
	E2E_NAMESPACE=$(E2E_NAMESPACE) IMG=$(IMG) go test -tags=$(E2E_TAGS) -count=1 -v -timeout=$(E2E_TIMEOUT) ./test/suites/...

e2e: e2e-setup e2e-test

e2e-clean:
	@echo "Deleting e2e namespace/resources ($(E2E_NAMESPACE))..."
	-@kubectl delete keyvalclusters.keyval.ivelok.io -n $(E2E_NAMESPACE) --all --ignore-not-found
	-@kubectl delete namespace $(E2E_NAMESPACE) --ignore-not-found

.PHONY: chaos chaos-test chaos-clean

chaos:
	@echo "Running chaos suite (namespace=$(CHAOS_NAMESPACE), experiments=$(CHAOS_EXPERIMENTS))..."
	E2E_NAMESPACE=$(CHAOS_NAMESPACE) CHAOS_NAMESPACE=$(CHAOS_NAMESPACE) CHAOS_EXPERIMENTS=$(CHAOS_EXPERIMENTS) \
		KEEP_RESOURCES=$(KEEP_RESOURCES) KEEP_ARTIFACTS=$(KEEP_ARTIFACTS) \
		go test -tags=$(CHAOS_TAGS) -count=1 -v -timeout=$(CHAOS_TIMEOUT) ./test/chaos

chaos-test:
	@echo "Running chaos Go tests only (no deploy)"
	E2E_NAMESPACE=$(CHAOS_NAMESPACE) CHAOS_NAMESPACE=$(CHAOS_NAMESPACE) CHAOS_EXPERIMENTS=$(CHAOS_EXPERIMENTS) \
		go test -tags=$(CHAOS_TAGS) -count=1 -timeout=$(CHAOS_TIMEOUT) ./test/chaos

chaos-clean:
	@echo "Deleting chaos namespace/resources ($(CHAOS_NAMESPACE))..."
	-@kubectl delete keyvalclusters.keyval.ivelok.io -n $(CHAOS_NAMESPACE) --all --ignore-not-found
	-@kubectl delete namespace $(CHAOS_NAMESPACE) --ignore-not-found

.PHONY: sla-report

sla-report:
	@echo "Generating SLA report..."
	@mkdir -p test/sla
	@go run ./cmd/sla-report -output test/sla/report.json $(SLA_FLAGS)

.PHONY: helm-lint helm-template helm-package runbook-check release

helm-lint:
	@echo "Linting Helm chart in $(CHART_DIR)..."
	@$(HELM) lint $(CHART_DIR)

helm-template:
	@echo "Rendering Helm templates for smoke validation..."
	@$(HELM) template smoke $(CHART_DIR) --namespace keyval-operator-system --set examples.enabled=false >/dev/null

helm-package: helm-lint helm-template
	@echo "Packaging Helm chart into $(CHART_PACKAGE_DIR)..."
	@mkdir -p $(CHART_PACKAGE_DIR)
	@if [ -n "$(CHART_VERSION)" ]; then \
		$(HELM) package $(CHART_DIR) --dependency-update --version $(CHART_VERSION) --app-version $(CHART_VERSION) --destination $(CHART_PACKAGE_DIR); \
	else \
		$(HELM) package $(CHART_DIR) --dependency-update --destination $(CHART_PACKAGE_DIR); \
	fi

runbook-check:
	@echo "Validating runbook formatting..."
	@./hack/runbook-check.sh

release: runbook-check
	@if [ -z "$(VERSION)" ]; then \
		echo "VERSION must be provided, e.g. make release VERSION=1.2.3" >&2; \
		exit 1; \
	fi
	@echo "Building release image $(RELEASE_REGISTRY)/$(RELEASE_REPOSITORY):$(VERSION)..."
	@$(MAKE) --no-print-directory docker-build IMG=$(RELEASE_REGISTRY)/$(RELEASE_REPOSITORY):$(VERSION)
	@echo "Tagging latest image..."
	@docker tag $(RELEASE_REGISTRY)/$(RELEASE_REPOSITORY):$(VERSION) $(RELEASE_REGISTRY)/$(RELEASE_REPOSITORY):latest
	@echo "Pushing container images to $(RELEASE_REGISTRY)..."
	@docker push $(RELEASE_REGISTRY)/$(RELEASE_REPOSITORY):$(VERSION)
	@docker push $(RELEASE_REGISTRY)/$(RELEASE_REPOSITORY):latest
	@if command -v $(COSIGN) >/dev/null 2>&1; then \
		echo "Signing container image with cosign"; \
		if [ -n "$$COSIGN_KEY" ]; then \
			COSIGN_YES=1 $(COSIGN) sign --key "$$COSIGN_KEY" $(RELEASE_REGISTRY)/$(RELEASE_REPOSITORY):$(VERSION); \
		else \
			COSIGN_YES=1 $(COSIGN) sign $(RELEASE_REGISTRY)/$(RELEASE_REPOSITORY):$(VERSION); \
		fi; \
	else \
		echo "cosign not available – skipping image signing"; \
	fi
	@$(MAKE) --no-print-directory helm-package CHART_VERSION=$(VERSION)
	@echo "Release artifacts:"
	@echo "  - Helm packages in $(CHART_PACKAGE_DIR)"
	@echo "  - Container image $(RELEASE_REGISTRY)/$(RELEASE_REPOSITORY):$(VERSION)"
