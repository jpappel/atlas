BINS := atlas
BUILD_TAGS := icu fts5
SRC := main.go $(wildcard cmd/*.go) $(wildcard pkg/*/*.go)
EMBEDDED_FILES := $(wildcard cmd/completions/*)
INSTALL_PATH := ~/.local/bin
COMPLETION_PATHS := ~/.config/zsh/completions/_atlas

.PHONY: all install uninstall test info clean

all: $(BINS)

atlas: $(SRC) $(EMBEDDED_FILES)
	go build -tags "$(BUILD_TAGS)" -o $@ $<

test:
	go test $(if $(subst undefined,,$(origin VIMRUNTIME)), -fullpath) -tags "$(BUILD_TAGS)" ./...

########
#
########

install: $(INSTALL_PATH)/atlas $(COMPLETION_PATHS)

$(INSTALL_PATH)/atlas: atlas
	cp atlas $(INSTALL_PATH)

~/.config/zsh/completions/_atlas: atlas
	mkdir -p $(dir $@)
	$< completions zsh > $@

uninstall:
	rm -f $(INSTALL_PATH)/atlas
	rm -f $(COMPLETION_PATHS)

########
#
########

info:
	@echo "SRC: $(SRC)"
	@echo "BINS: $(BINS)"
	@echo "TEST_BINS: $(TEST_BINS)"
	@echo "INSTALL_PATH: $(INSTALL_PATH)"
	@echo "BUILD_TAGS: $(BUILD_TAGS)"

clean:
	rm -f $(BINS) *.db *.db-shm *.db-wal
	go mod tidy
