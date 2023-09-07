set -ex

sudo apt-get update
sudo apt-get install gcc-arm-linux-gnueabihf build-essential rsync
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
~/.cargo/bin/rustup target add armv7-unknown-linux-musleabihf

cat << EOF >> ~/.cargo/config

[target.armv7-unknown-linux-musleabihf]
linker = "arm-linux-gnueabihf-gcc"
EOF
