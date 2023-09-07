#!/usr/bin/env bash
# To setup cross build on a linux machine, do the following
# 1. Install package arm-linux-gnu-gcc (yum) or gcc-arm-linux-gnueabihf (apt).
# 5. Install build-essential (apt only).
# 2. Run `rustup target add armv7-unknown-linux-musleabihf`
# 1. Add the following to ~/.cargo/config
# [target.armv7-unknown-linux-musleabihf]
# linker = "arm-linux-gnu-gcc" (yum)
# linker = "arm-linux-gnueabihf-gcc" (apt)
# 4. Install rsync.

set -ex

MACHINE=gcloud
rsync -av /Users/ditsing/Code/ruaft $MACHINE:~/compile/ --exclude 'ruaft/target' --exclude 'ruaft/durio/target' --exclude 'ruaft/.git' --exclude '.idea'
ssh $MACHINE 'cd ~/compile/ruaft/durio && $HOME/.cargo/bin/cargo build --target=armv7-unknown-linux-musleabihf --release'
mkdir -p /tmp/ruaft
rsync -av $MACHINE:'~/compile/ruaft/durio/target/armv7-unknown-linux-musleabihf/release/durio' '/tmp/ruaft/durio'

ssh alice 'pkill -9 durio || echo nothing'
rsync -av '/tmp/ruaft/durio' alice:/tmp/durio
ssh alice '
  RUST_LOG=warp,tarpc=error,ruaft=debug,kvraft=debug,durio nohup /tmp/durio 1 1>>/tmp/durio.out 2>>/tmp/durio.err &
'

ssh bob 'pkill -9 durio || echo nothing'
rsync -av '/tmp/ruaft/durio' bob:/tmp/durio
ssh bob '
  RUST_LOG=warp,tarpc=error,ruaft=debug,kvraft=debug,durio nohup /tmp/durio 2 1>>/tmp/durio.out 2>>/tmp/durio.err &
'

RUST_LOG=warp,tarpc=error,ruaft=debug,kvraft=debug,durio cargo run 0 || echo "Done"

ssh alice 'pkill -9 durio || echo nothing'
ssh bob 'pkill -9 durio || echo nothing'
