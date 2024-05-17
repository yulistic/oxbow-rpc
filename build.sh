#!/bin/bash -e
PKG_CONFIG_DIR="lib/rdma-core/build/lib/pkgconfig"

if [ "$1" = "re" ]; then
	rm -rf build
fi

if [ ! -d "build" ]; then
	# meson setup build -Dpkg_config_path="$PKG_CONFIG_DIR" -Dbuildtype="debug"
	meson setup build -Dpkg_config_path="$PKG_CONFIG_DIR"
fi
# meson compile -C build
meson compile -vC build
# meson test -C build
# meson test -C build --suite rdma
