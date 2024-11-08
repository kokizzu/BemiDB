cd src
go build -o ../build/bemidb-darwin-arm64

cd ../build
LIBCPP_PATH=$(otool -L ./bemidb-darwin-arm64 | grep -o '/.*/libc++\.1\.0\.dylib')
sudo cp $LIBCPP_PATH ./libc++.1.0.dylib
sudo cp $LIBCPP_PATH /usr/local/lib/libc++.1.0.dylib
install_name_tool -change $LIBCPP_PATH /usr/local/lib/libc++.1.0.dylib ./bemidb-darwin-arm64
otool -L ./bemidb-darwin-arm64
