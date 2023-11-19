#!/bin/bash
RUST_LOG=raft=debug,kvraft=debug cargo test -p durio -- --include-ignored --nocapture
