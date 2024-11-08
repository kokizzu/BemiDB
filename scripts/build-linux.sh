#!/bin/bash

platforms=("linux/amd64" "linux/arm64")

for platform in "${platforms[@]}"
do
  os="${platform%/*}"
  arch="${platform#*/}"
  echo "Building bemidb for $os/$arch"

  docker buildx build \
    --build-arg PLATFORM=$platform \
    --build-arg GOOS=$os \
    --build-arg GOARCH="$arch" \
    -t bemidb-build:$os-$arch .

  docker create --name temp-container bemidb-build:$os-$arch
  docker cp temp-container:/app/bemidb ./build/bemidb-$os-$arch
  docker rm temp-container
done
