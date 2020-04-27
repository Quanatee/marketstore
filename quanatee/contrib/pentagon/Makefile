#GOFLAGS="-mod=vendor"
CUR_DIR := $(CURDIR)
GOPATH0 := $(firstword $(subst :, ,$(GOPATH)))
all:
	GOFLAGS=$(GOFLAGS) go build -o $(GOPATH0)/bin/$(CUR_DIR).so -buildmode=plugin .

debug:
	GOFLAGS=$(GOFLAGS) go build -gcflags="all=-N -l" -o $(GOPATH0)/bin/pentagon.so -buildmode=plugin .
