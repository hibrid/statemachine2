TIMESTAMP ?= $(shell date +%s)

ROOT_DIRECTORY:=$(realpath $(dir $(realpath $(lastword $(MAKEFILE_LIST)))))

REPOSITORY_GOPATH:=$(word 1, $(subst :, ,$(GOPATH)))
REPOSITORY_PACKAGE:=github.com/tidepool-org/platform
REPOSITORY_NAME:=$(notdir $(REPOSITORY_PACKAGE))

BIN_DIRECTORY := ${ROOT_DIRECTORY}/_bin
PATH := ${PATH}:${BIN_DIRECTORY}

ifdef TRAVIS_TAG
	VERSION_BASE:=$(TRAVIS_TAG)
else
	VERSION_BASE:=$(shell git describe --abbrev=0 --tags 2> /dev/null || echo 'v0.0.0')
endif
VERSION_BASE:=$(VERSION_BASE:v%=%)
VERSION_SHORT_COMMIT:=$(shell git rev-parse --short HEAD)
VERSION_FULL_COMMIT:=$(shell git rev-parse HEAD)
VERSION_PACKAGE:=$(REPOSITORY_PACKAGE)/application

GO_BUILD_FLAGS:=-buildvcs=false
GO_LD_FLAGS:=-ldflags '-X $(VERSION_PACKAGE).VersionBase=$(VERSION_BASE) -X $(VERSION_PACKAGE).VersionShortCommit=$(VERSION_SHORT_COMMIT) -X $(VERSION_PACKAGE).VersionFullCommit=$(VERSION_FULL_COMMIT)'

FIND_MAIN_CMD:=find . -path './$(BUILD)*' -not -path './vendor/*' -name '*.go' -not -name '*_test.go' -type f -exec egrep -l '^\s*func\s+main\s*(\s*)' {} \;
TRANSFORM_GO_BUILD_CMD:=sed 's|\.\(.*\)\(/[^/]*\)/[^/]*|_bin\1\2\2 .\1\2/.|'

GO_BUILD_CMD:=CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build $(GO_BUILD_FLAGS) $(GO_LD_FLAGS) -o

CompileDaemon:
ifeq ($(shell which CompileDaemon),)
	go install github.com/githubnemo/CompileDaemon
endif
build:
	@echo "CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build $(BUILD)"
	@cd $(ROOT_DIRECTORY) && $(FIND_MAIN_CMD) | $(TRANSFORM_GO_BUILD_CMD) | while read LINE; do $(GO_BUILD_CMD) $${LINE}; done

service-build:
ifdef SERVICE
	@$(MAKE) build BUILD=$${SERVICE}
endif

service-start: CompileDaemon tmp
ifdef SERVICE
	@cd $(ROOT_DIRECTORY) && BUILD=$(SERVICE) CompileDaemon -build-dir='.' -build='make build' -command='_bin/$(SERVICE)/$(notdir $(SERVICE))' -directory='_tmp' -pattern='^$$' -include='$(subst /,.,$(SERVICE)).restart' -recursive=false -log-prefix=false -graceful-kill=true -graceful-timeout=60
endif

service-debug: CompileDaemon tmp
ifdef SERVICE
ifdef DEBUG_PORT
	@cd $(ROOT_DIRECTORY) && BUILD=$(SERVICE) CompileDaemon -build-dir='.' -build='make build' -command='dlv exec --headless --log --listen=:$(DEBUG_PORT) --api-version=2 _bin/$(SERVICE)/$(notdir $(SERVICE))' -directory='_tmp' -pattern='^$$' -include='$(subst /,.,$(SERVICE)).restart' -recursive=false -log-prefix=false -graceful-kill=true -graceful-timeout=60
endif
endif

service-restart: tmp
ifdef SERVICE
	@cd $(ROOT_DIRECTORY) && date +'%Y-%m-%dT%H:%M:%S%z' > _tmp/$(subst /,.,$(SERVICE)).restart
endif

tmp:
	@mkdir -p $(ROOT_DIRECTORY)/_tmp



.PHONY: default build service-build CompileDaemon tmp service-start service-debug service-restart