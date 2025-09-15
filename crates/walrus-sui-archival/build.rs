// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::io::Result;

fn main() -> Result<()> {
    // Use vendored protoc.
    unsafe {
        std::env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path().unwrap());
    }

    prost_build::compile_protos(&["proto/archival.proto"], &["proto/"])?;
    Ok(())
}
