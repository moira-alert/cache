VERSION := $(shell git describe --always --tags --abbrev=0 | tail -c +2)
RELEASE := $(shell git describe --always --tags | awk -F- '{ if ($$2) dot="."} END { printf "1%s%s%s%s\n",dot,$$2,dot,$$3}')
VENDOR := "SKB Kontur"
URL := "https://github.com/moira-alert"
LICENSE := "GPLv3"

default: test build

build:
	go build -ldflags "-X main.version=$(VERSION)-$(RELEASE)" -o build/moira-cache

test: prepare
	$(GOPATH)/bin/ginkgo -r --randomizeAllSpecs --randomizeSuites -cover -coverpkg=../filter --failOnPending --failOnPending --trace --race tests

.PHONY: test

prepare:
	go get github.com/sparrc/gdm
	$(GOPATH)/bin/gdm restore
	go get github.com/onsi/ginkgo/ginkgo

clean:
	rm -rf build

tar:
	mkdir -p build/root/usr/local/bin
	mkdir -p build/root/usr/lib/systemd/system
	mkdir -p build/root/etc/logrotate.d

	mv build/moira-cache build/root/usr/local/bin/
	cp pkg/moira-cache.service build/root/usr/lib/systemd/system/moira-cache.service
	cp pkg/logrotate build/root/etc/logrotate.d/moira-cache

	tar -czvPf build/moira-cache-$(VERSION)-$(RELEASE).tar.gz -C build/root  .

rpm:
	fpm -t rpm \
		-s "tar" \
		--description "Moira Cache" \
		--vendor $(VENDOR) \
		--url $(URL) \
		--license $(LICENSE) \
		--name "moira-cache" \
		--version "$(VERSION)" \
		--iteration "$(RELEASE)" \
		--after-install "./pkg/postinst" \
		--depends logrotate \
		-p build \
		build/moira-cache-$(VERSION)-$(RELEASE).tar.gz

deb:
	fpm -t deb \
		-s "tar" \
		--description "Moira Cache" \
		--vendor $(VENDOR) \
		--url $(URL) \
		--license $(LICENSE) \
		--name "moira-cache" \
		--version "$(VERSION)" \
		--iteration "$(RELEASE)" \
		--after-install "./pkg/postinst" \
		--depends logrotate \
		-p build \
		build/moira-cache-$(VERSION)-$(RELEASE).tar.gz

packages: clean build tar rpm deb
