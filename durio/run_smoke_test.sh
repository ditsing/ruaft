#!/bin/bash
RUST_LOG=ruaft=debug,kvraft=debug cargo test -- --nocapture
