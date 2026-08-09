CXX ?= g++
CXXFLAGS ?= -O2 -Wall -Wextra -Werror -std=c++17
PROTOC ?= protoc
PROTOC_GEN_GO ?= $(or $(shell command -v protoc-gen-go),$(shell go env GOPATH)/bin/protoc-gen-go)
PROTOBUF_CFLAGS ?= $(shell pkg-config --cflags protobuf)
PROTOBUF_LIBS ?= $(shell pkg-config --libs protobuf)

.PHONY: generate generate-go generate-cpp verify-go pagebroker-daemon pagebroker-daemon-test wire-fixture test clean
generate: generate-go generate-cpp

generate-go: pagebroker.proto
	$(PROTOC) -I. --plugin=protoc-gen-go=$(PROTOC_GEN_GO) --go_out=.. --go_opt=module=github.com/ai-dynamo/dynamo/deploy/snapshot $<

verify-go: generate-go
	git -C ../../.. diff --exit-code -- deploy/snapshot/internal/pagebroker/pagebroker.pb.go

generate-cpp: pagebroker.proto
	$(PROTOC) -I. --cpp_out=. $<

pagebroker-daemon: generate-cpp broker.cpp broker.hpp daemon.cpp
	$(CXX) $(CXXFLAGS) $(PROTOBUF_CFLAGS) -o $@ pagebroker.pb.cc broker.cpp daemon.cpp $(PROTOBUF_LIBS)

pagebroker-daemon-test: generate-cpp broker.cpp broker.hpp daemon_test.cpp
	$(CXX) $(CXXFLAGS) $(PROTOBUF_CFLAGS) -o $@ daemon_test.cpp pagebroker.pb.cc broker.cpp $(PROTOBUF_LIBS)

wire-fixture: generate-cpp testdata/wire_fixture.cpp
	$(CXX) $(CXXFLAGS) $(PROTOBUF_CFLAGS) -I. -o $@ pagebroker.pb.cc testdata/wire_fixture.cpp $(PROTOBUF_LIBS)
test: wire-fixture
	./wire-fixture >/dev/null
	$(MAKE) pagebroker-daemon-test && ./pagebroker-daemon-test
clean:
	rm -f wire-fixture pagebroker-daemon pagebroker-daemon-test pagebroker.pb.cc pagebroker.pb.h
