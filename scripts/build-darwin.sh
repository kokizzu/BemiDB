cd src
go build -o ../build/bemidb-darwin-arm64

cd ../build
LIBCPP_OLD_PATH=$(otool -L ./bemidb-darwin-arm64 | grep -o '/.*/libc++\.1\.0\.dylib')
LIBCPP_NEW_PATH=/usr/local/lib/libc++.1.0.dylib
sudo sudo cp $LIBCPP_OLD_PATH $LIBCPP_NEW_PATH

LIBCPPABI_OLD_PATH=$(otool -L $LIBCPP_NEW_PATH | grep -o '/.*/libc++abi\.1\.dylib')
LIBCPPABI_NEW_PATH=/usr/local/lib/libc++abi.1.dylib
sudo cp $LIBCPPABI_OLD_PATH $LIBCPPABI_NEW_PATH

sudo install_name_tool -change $LIBCPPABI_OLD_PATH $LIBCPPABI_NEW_PATH $LIBCPP_NEW_PATH
sudo install_name_tool -change $LIBCPP_OLD_PATH $LIBCPP_NEW_PATH ./bemidb-darwin-arm64

sudo cp $LIBCPP_NEW_PATH ./libc++.1.0.dylib
sudo cp $LIBCPPABI_NEW_PATH ./libc++abi.1.dylib
otool -L ./bemidb-darwin-arm64
