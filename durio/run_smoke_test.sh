#!/bin/bash
RUST_LOG=ruaft=debug,kvraft=debug cargo test -p durio -- --include-ignored --nocapture
