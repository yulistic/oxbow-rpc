#!/bin/bash -e
# Install rdma-core
sudo apt install build-essential cmake gcc libudev-dev libnl-3-dev libnl-route-3-dev ninja-build pkg-config valgrind python3-dev cython3 python3-docutils pandoc
(
	cd rdma-core || exit
	./build.sh
)

# BitArray library.
(
	cd BitArray || exit
	make
)
