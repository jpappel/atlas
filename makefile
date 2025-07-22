BINS := atlas
SRC := main.go $(wildcard cmd/*.go) $(wildcard pkg/*/*.go)
INSTALL_PATH := ~/.local/bin

.PHONY: all install uninstall test info clean

all: $(BINS)

atlas: $(SRC)
	go build -o $@ $<

test:
	go test ./...

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

clean:
	rm -f $(BINS) *.db *.db-shm *.db-wal
	go mod tidy
