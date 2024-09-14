#![allow(unused)] // Remove this line to enable warnings.

use anyhow::{Context, Result};
use monoio::fs::{File, OpenOptions};
use std::{
    io::{Read, Write},
    time::{Duration, Instant},
};

#[monoio::main]
async fn main() -> Result<()> {
    let cmd = Cmd::from_env().context("failed to parse args")?;
    cmd.run().await?;

    Ok(())
}

#[derive(Debug)]
struct Cmd {
    sub: SubCmd,
    verbose: bool,
}

#[derive(Debug)]
enum SubCmd {
    Write {
        file: String,
        block_size: u64,
        count: u64,
    },
    Read {
        file: String,
        block_size: u64,
        count: u64,
    },
}

impl Cmd {
    fn from_env() -> Result<Self> {
        let mut args = pico_args::Arguments::from_env();
        let sub = match args.subcommand()?.as_deref() {
            Some("write") => SubCmd::Write {
                file: args.value_from_str(["-f", "--file"])?,
                block_size: args
                    .opt_value_from_str(["-s", "--block-size"])?
                    .unwrap_or(32),
                count: args.opt_value_from_str(["-c", "--count"])?.unwrap_or(1),
            },
            Some("read") => SubCmd::Read {
                file: args.value_from_str(["-f", "--file"])?,
                block_size: args
                    .opt_value_from_str(["-s", "--block-size"])?
                    .unwrap_or(32),
                count: args.opt_value_from_str(["-c", "--count"])?.unwrap_or(1),
            },
            _ => return Err(anyhow::anyhow!("Invalid subcommand")),
        };
        let verbose = args.contains(["-v", "--verbose"]);

        Ok(Self { sub, verbose })
    }

    async fn run(self) -> Result<()> {
        match self.sub {
            SubCmd::Write {
                file,
                block_size,
                count,
            } => write_file(&file, block_size, count, self.verbose).await?,
            SubCmd::Read {
                file,
                block_size,
                count,
            } => read_file(&file, block_size, count, self.verbose).await?,
        }

        Ok(())
    }
}

async fn write_file(file: &str, block_size: u64, count: u64, verbose: bool) -> Result<()> {
    let file = OpenOptions::new()
        .write(true)
        .create(true)
        .open(file)
        .await?;

    let block = vec![0u8; block_size as usize];
    let start = Instant::now();
    for _ in 0..count {
        file.write_all_at(block.clone(), 0).await.0?;
    }
    let elapsed = start.elapsed().as_secs_f64();

    let speed = (block_size * count) as f64 / elapsed;
    println!(
        "write {} bytes in {:.6} seconds, speed: {:.3} bytes/s",
        block_size * count,
        elapsed,
        speed
    );

    Ok(())
}

async fn read_file(file: &str, block_size: u64, count: u64, verbose: bool) -> Result<()> {
    Ok(())
}
