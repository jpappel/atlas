BINS := atlas
BUILD_TAGS := icu fts5
SRC := main.go $(wildcard cmd/*.go) $(wildcard pkg/*/*.go)
INSTALL_PATH := ~/.local/bin

.PHONY: all install uninstall test info clean

all: $(BINS)

atlas: $(SRC)
	go build -tags "$(BUILD_TAGS)" -o $@ $<

test:
	go test -tags "$(BUILD_TAGS)" ./...

########
#
########

install: $(INSTALL_PATH)/atlas

$(INSTALL_PATH)/atlas: atlas
	cp atlas $(INSTALL_PATH)

uninstall: $(INSTALL_PATH)/atlas
	rm $<

########
#
########

info:
	@echo "SRC: $(SRC)"
	@echo "BINS: $(BINS)"
	@echo "INSTALL_PATH: $(INSTALL_PATH)"
	@echo "BUILD_TAGS: $(BUILD_TAGS)"

clean:
	rm -f $(BINS) *.db *.db-shm *.db-wal
	go mod tidy
