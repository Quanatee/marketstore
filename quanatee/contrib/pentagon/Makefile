#GOFLAGS="-mod=vendor"
GOPATH0 := $(firstword $(subst :, ,$(GOPATH)))
all:
	GOFLAGS=$(GOFLAGS) go build -o $(GOPATH0)/bin/pentagon.so -buildmode=pie .
	GOFLAGS=$(GOFLAGS) go build -o $(GOPATH0)/bin/pentagon4crypto.so -buildmode=pie .
	GOFLAGS=$(GOFLAGS) go build -o $(GOPATH0)/bin/pentagon4forex.so -buildmode=pie .
	GOFLAGS=$(GOFLAGS) go build -o $(GOPATH0)/bin/pentagon4etfs.so -buildmode=pie .
	GOFLAGS=$(GOFLAGS) go build -o $(GOPATH0)/bin/pentagon4stocks.so -buildmode=pie .
	
debug:
	GOFLAGS=$(GOFLAGS) go build -gcflags="all=-N -l" -o $(GOPATH0)/bin/pentagon.so -buildmode=pie .
	GOFLAGS=$(GOFLAGS) go build -gcflags="all=-N -l" -o $(GOPATH0)/bin/pentagon4crypto.so -buildmode=pie .
	GOFLAGS=$(GOFLAGS) go build -gcflags="all=-N -l" -o $(GOPATH0)/bin/pentagon4forex.so -buildmode=pie .
	GOFLAGS=$(GOFLAGS) go build -gcflags="all=-N -l" -o $(GOPATH0)/bin/pentagon4etfs.so -buildmode=pie .
	GOFLAGS=$(GOFLAGS) go build -gcflags="all=-N -l" -o $(GOPATH0)/bin/pentagon4stocks.so -buildmode=pie .
