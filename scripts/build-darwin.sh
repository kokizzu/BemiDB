cd src
go build -o ../build/bemidb-darwin-arm64

create_dir_if_needed() {
    local dir=$1
    if [ ! -d "$dir" ]; then
        echo "Creating directory: $dir"
        sudo mkdir -p "$dir"
        sudo chmod 755 "$dir"
    fi
}

create_dir_if_needed "/usr/local/lib"
cd ../build
LIBCPP_OLD_PATH=$(otool -L ./bemidb-darwin-arm64 | grep -o '/.*/libc++\.1\.0\.dylib')
if [ -z "$LIBCPP_OLD_PATH" ]; then
    echo "Error: Could not find libc++ dependency in binary"
    exit 1
fi
LIBCPP_NEW_PATH=/usr/local/lib/libc++.1.0.dylib
sudo cp $LIBCPP_OLD_PATH $LIBCPP_NEW_PATH

LIBCPPABI_OLD_PATH=$(otool -L $LIBCPP_NEW_PATH | grep -o '/.*/libc++abi\.1\.dylib')
if [ -z "$LIBCPPABI_OLD_PATH" ]; then
    echo "Error: Could not find libc++abi dependency"
    exit 1
fi
LIBCPPABI_NEW_PATH=/usr/local/lib/libc++abi.1.dylib
sudo cp $LIBCPPABI_OLD_PATH $LIBCPPABI_NEW_PATH

sudo install_name_tool -change $LIBCPPABI_OLD_PATH $LIBCPPABI_NEW_PATH $LIBCPP_NEW_PATH
sudo install_name_tool -change $LIBCPP_OLD_PATH $LIBCPP_NEW_PATH ./bemidb-darwin-arm64

sudo cp $LIBCPP_NEW_PATH ./libc++.1.0.dylib
sudo cp $LIBCPPABI_NEW_PATH ./libc++abi.1.dylib
otool -L ./bemidb-darwin-arm64
