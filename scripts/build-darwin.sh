go build -C src -o ../build/bemidb-darwin-arm64

LIBCPP_PATH=$(otool -L ./build/bemidb-darwin-arm64 | grep -o '/.*/libc++\.1\.0\.dylib')
sudo cp $LIBCPP_PATH ./build/libc++.1.0.dylib

sudo cp $LIBCPP_PATH /usr/local/lib/libc++.1.0.dylib
install_name_tool -change $LIBCPP_PATH /usr/local/lib/libc++.1.0.dylib ./build/bemidb-darwin-arm64
otool -L ./build/bemidb-darwin-arm64
