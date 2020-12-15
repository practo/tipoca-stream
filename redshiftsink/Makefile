BINS := redshiftbatcher redshiftloader redshiftsink

# Where to push the docker image.
REGISTRY ?= 746161288457.dkr.ecr.ap-south-1.amazonaws.com

# This version-strategy uses git tags to set the version string
VERSION ?= $(shell git describe --tags --always --dirty)
#
# This version-strategy uses a manual value to set the version string
#VERSION ?= 1.2.3

###
### These variables should not need tweaking.
###

SRC_DIRS := cmd pkg # directories which hold app source (not vendored)

ALL_PLATFORMS := linux/amd64 linux/arm linux/arm64 linux/ppc64le linux/s390x

# Used internally.  Users should pass GOOS and/or GOARCH.
OS := $(if $(GOOS),$(GOOS),$(shell go env GOOS))
ARCH := $(if $(GOARCH),$(GOARCH),$(shell go env GOARCH))

BASEIMAGE ?= gcr.io/distroless/static

TAG := $(VERSION)

BUILD_IMAGE ?= golang:1.15.6-alpine

# If you want to build all binaries, see the 'all-build' rule.
# If you want to build all containers, see the 'all-container' rule.
# If you want to build AND push all containers, see the 'all-push' rule.
all: # @HELP builds binaries for one platform ($OS/$ARCH)
all: build

# For the following OS/ARCH expansions, we transform OS/ARCH into OS_ARCH
# because make pattern rules don't match with embedded '/' characters.

build-%:
	@$(MAKE) build                        \
	    --no-print-directory              \
	    GOOS=$(firstword $(subst _, ,$*)) \
	    GOARCH=$(lastword $(subst _, ,$*))

container-%:
	@$(MAKE) container                    \
	    --no-print-directory              \
	    GOOS=$(firstword $(subst _, ,$*)) \
	    GOARCH=$(lastword $(subst _, ,$*))

push-%:
	@$(MAKE) push                         \
	    --no-print-directory              \
	    GOOS=$(firstword $(subst _, ,$*)) \
	    GOARCH=$(lastword $(subst _, ,$*))

all-build: # @HELP builds binaries for all platforms
all-build: $(addprefix build-, $(subst /,_, $(ALL_PLATFORMS)))

all-container: # @HELP builds containers for all platforms
all-container: $(addprefix container-, $(subst /,_, $(ALL_PLATFORMS)))

all-push: # @HELP pushes containers for all platforms to the defined registry
all-push: $(addprefix push-, $(subst /,_, $(ALL_PLATFORMS)))

build: $(foreach bin,$(BINS),bin/$(OS)_$(ARCH)/$(bin))

fmt: # @HELP formats the code
	@echo "formatting code"
	/bin/sh -c "./build/format.sh"

# Directories that we need created to build/test.
BUILD_DIRS := bin/$(OS)_$(ARCH)     \
              .go/bin/$(OS)_$(ARCH) \
              .go/cache

# The following structure defeats Go's (intentional) behavior to always touch
# result files, even if they have not changed.  This will still run `go` but
# will not trigger further work if nothing has actually changed.
OUTBINS = $(foreach bin,$(BINS),bin/$(OS)_$(ARCH)/$(bin))

# Each outbin target is just a facade for the respective stampfile target.
# This `eval` establishes the dependencies for each.
$(foreach outbin,$(OUTBINS),$(eval  \
    $(outbin): .go/$(outbin).stamp  \
))
# This is the target definition for all outbins.
$(OUTBINS):
	@true

# Each stampfile target can reference an $(OUTBIN) variable.
$(foreach outbin,$(OUTBINS),$(eval $(strip   \
    .go/$(outbin).stamp: OUTBIN = $(outbin)  \
)))
# This is the target definition for all stampfiles.
# This will build the binary under ./.go and update the real binary iff needed.
STAMPS = $(foreach outbin,$(OUTBINS),.go/$(outbin).stamp)
.PHONY: $(STAMPS)
$(STAMPS): go-build
	@echo "binary: $(OUTBIN)"
	@if ! cmp -s .go/$(OUTBIN) $(OUTBIN); then  \
	    mv .go/$(OUTBIN) $(OUTBIN);             \
	    date >$@;                               \
	fi

# This runs the actual `go build` which updates all binaries.
go-build: $(BUILD_DIRS)
	@docker run                                                 \
	    -i                                                      \
	    --rm                                                    \
	    -u $$(id -u):$$(id -g)                                  \
	    -v $$(pwd):/src                                         \
	    -w /src                                                 \
	    -v $$(pwd)/.go/bin/$(OS)_$(ARCH):/go/bin                \
	    -v $$(pwd)/.go/bin/$(OS)_$(ARCH):/go/bin/$(OS)_$(ARCH)  \
	    -v $$(pwd)/.go/cache:/.cache                            \
	    --env HTTP_PROXY=$(HTTP_PROXY)                          \
	    --env HTTPS_PROXY=$(HTTPS_PROXY)                        \
	    $(BUILD_IMAGE)                                          \
	    /bin/sh -c "                                            \
	        ARCH=$(ARCH)                                        \
	        OS=$(OS)                                            \
	        VERSION=$(VERSION)                                  \
	        ./build/build.sh                                    \
	    "

# Example: make shell CMD="-c 'date > datefile'"
shell: # @HELP launches a shell in the containerized build environment
shell: $(BUILD_DIRS)
	@echo "launching a shell in the containerized build environment"
	@docker run                                                 \
	    -ti                                                     \
	    --rm                                                    \
	    -u $$(id -u):$$(id -g)                                  \
	    -v $$(pwd):/src                                         \
	    -w /src                                                 \
	    -v $$(pwd)/.go/bin/$(OS)_$(ARCH):/go/bin                \
	    -v $$(pwd)/.go/bin/$(OS)_$(ARCH):/go/bin/$(OS)_$(ARCH)  \
	    -v $$(pwd)/.go/cache:/.cache                            \
	    --env HTTP_PROXY=$(HTTP_PROXY)                          \
	    --env HTTPS_PROXY=$(HTTPS_PROXY)                        \
	    $(BUILD_IMAGE)                                          \
	    /bin/sh $(CMD)

CONTAINER_DOTFILES = $(foreach bin,$(BINS),.container-$(subst /,_,$(REGISTRY)/$(bin))-$(TAG))

container containers: # @HELP builds containers for one platform ($OS/$ARCH)
container containers: $(CONTAINER_DOTFILES)
	@for bin in $(BINS); do              \
	    echo "container: $(REGISTRY)/$$bin:$(TAG)"; \
	done

# Each container-dotfile target can reference a $(BIN) variable.
# This is done in 2 steps to enable target-specific variables.
$(foreach bin,$(BINS),$(eval $(strip                                 \
    .container-$(subst /,_,$(REGISTRY)/$(bin))-$(TAG): BIN = $(bin)  \
)))
$(foreach bin,$(BINS),$(eval                                                                   \
    .container-$(subst /,_,$(REGISTRY)/$(bin))-$(TAG): bin/$(OS)_$(ARCH)/$(bin) Dockerfile.in  \
))
# This is the target definition for all container-dotfiles.
# These are used to track build state in hidden files.
$(CONTAINER_DOTFILES):
	@sed                                 \
	    -e 's|{ARG_BIN}|$(BIN)|g'        \
	    -e 's|{ARG_ARCH}|$(ARCH)|g'      \
	    -e 's|{ARG_OS}|$(OS)|g'          \
	    -e 's|{REGISTRY}|$(BASEIMAGE)|g' \
	    Dockerfile.in > .dockerfile-$(BIN)-$(OS)_$(ARCH)
	@docker build -t $(REGISTRY)/$(BIN):$(TAG) -f .dockerfile-$(BIN)-$(OS)_$(ARCH) .
	@docker images -q $(REGISTRY)/$(BIN):$(TAG) > $@
	@echo

push: # @HELP pushes the container for one platform ($OS/$ARCH) to the defined registry
push: $(CONTAINER_DOTFILES)
	@for bin in $(BINS); do                    \
	    docker push $(REGISTRY)/$$bin:$(TAG);  \
	done

manifest-list: # @HELP builds a manifest list of containers for all platforms
manifest-list: all-push
	@for bin in $(BINS); do                                   \
	    platforms=$$(echo $(ALL_PLATFORMS) | sed 's/ /,/g');  \
	    manifest-tool                                         \
	        --username=oauth2accesstoken                      \
	        --password=$$(gcloud auth print-access-token)     \
	        push from-args                                    \
	        --platforms "$$platforms"                         \
	        --template $(REGISTRY)/$$bin:$(VERSION)__OS_ARCH  \
	        --target $(REGISTRY)/$$bin:$(VERSION)

version: # @HELP outputs the version string
version:
	@echo $(VERSION)

test: # @HELP runs tests, as defined in ./build/test.sh
test: $(BUILD_DIRS)
	@docker run                                                 \
	    -i                                                      \
	    --rm                                                    \
	    -u $$(id -u):$$(id -g)                                  \
	    -v $$(pwd):/src                                         \
	    -w /src                                                 \
	    -v $$(pwd)/.go/bin/$(OS)_$(ARCH):/go/bin                \
	    -v $$(pwd)/.go/bin/$(OS)_$(ARCH):/go/bin/$(OS)_$(ARCH)  \
	    -v $$(pwd)/.go/cache:/.cache                            \
	    --env HTTP_PROXY=$(HTTP_PROXY)                          \
	    --env HTTPS_PROXY=$(HTTPS_PROXY)                        \
	    $(BUILD_IMAGE)                                          \
	    /bin/sh -c "                                            \
	        ARCH=$(ARCH)                                        \
	        OS=$(OS)                                            \
	        VERSION=$(VERSION)                                  \
	        ./build/test.sh $(SRC_DIRS)                         \
	    "

$(BUILD_DIRS):
	@mkdir -p $@

clean: # @HELP removes built binaries and temporary files
clean: container-clean bin-clean

container-clean:
	rm -rf .container-* .dockerfile-*

bin-clean:
	rm -rf .go bin

help: # @HELP prints this message
help:
	@echo "VARIABLES:"
	@echo "  BINS = $(BINS)"
	@echo "  OS = $(OS)"
	@echo "  ARCH = $(ARCH)"
	@echo "  REGISTRY = $(REGISTRY)"
	@echo
	@echo "TARGETS:"
	@grep -E '^.*: *# *@HELP' $(MAKEFILE_LIST)    \
	    | awk '                                   \
	        BEGIN {FS = ": *# *@HELP"};           \
	        { printf "  %-30s %s\n", $$1, $$2 };  \
	    '

###
# kubebuilder targets
###

# TODO: to make it work with kubectl kustomize edit
# Image URL to use all building/pushing image targets
# CONTROLLER_MANAGER_IMAGE ?= practodev/redshiftsink:latest

# Produce CRDs that work back to Kubernetes 1.11 (no version conversion)
CRD_OPTIONS ?= "crd:trivialVersions=true"

# Get the currently used golang install path (in GOPATH/bin, unless GOBIN is set)
ifeq (,$(shell go env GOBIN))
GOBIN=$(shell go env GOPATH)/bin
else
GOBIN=$(shell go env GOBIN)
endif

# Generate manifests e.g. CRD, RBAC etc.
# find or download controller-gen
# download controller-gen if necessary
controller-gen:
ifeq (, $(shell which controller-gen))
	@{ \
	set -e ;\
	CONTROLLER_GEN_TMP_DIR=$$(mktemp -d) ;\
	cd $$CONTROLLER_GEN_TMP_DIR ;\
	go mod init tmp ;\
	go get sigs.k8s.io/controller-tools/cmd/controller-gen@v0.2.5 ;\
	rm -rf $$CONTROLLER_GEN_TMP_DIR ;\
	}
CONTROLLER_GEN=$(GOBIN)/controller-gen
else
CONTROLLER_GEN=$(shell which controller-gen)
endif

manifests: controller-gen
	$(CONTROLLER_GEN) $(CRD_OPTIONS) rbac:roleName=manager-role webhook paths="./..." output:crd:artifacts:config=config/crd/bases

# Install CRDs into a cluster
install: manifests
	kubectl kustomize config/crd | kubectl apply -f -

# Deploy controller in the configured Kubernetes cluster in ~/.kube/config
deploy: manifests
	cd config/manager #&& kustomize edit set image controller=${CONTROLLER_MANAGER_IMAGE}
	kustomize build config/default | kubectl apply -f -

# Generate code
generate: controller-gen
	$(CONTROLLER_GEN) object:headerFile="build/boilerplate.go.txt" paths="./..."

# Run go vet against code
vet:
	go vet ./...

# Run against the configured Kubernetes cluster in ~/.kube/config
run: generate fmt vet manifests
	go run ./cmd/redshiftsink/main.go
