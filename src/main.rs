#![allow(unused)] // Remove this line to enable warnings.

use anyhow::{Context, Ok, Result};
use humansize::{ISizeFormatter, SizeFormatter, BINARY};
use monoio::fs::{File, OpenOptions};
use std::{
    default,
    io::{Read, Write},
    rc::Rc,
    str::FromStr,
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
        strategy: Strategy,
    },
    Read {
        file: String,
        block_size: u64,
        count: u64,
        strategy: Strategy,
    },
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
enum Strategy {
    #[default]
    Sequential,
    Async,
    Async2,
}

impl FromStr for Strategy {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "seq" => Ok(Self::Sequential),
            "async" => Ok(Self::Async),
            "async2" => Ok(Self::Async2),
            _ => Err(anyhow::anyhow!("Invalid strategy")),
        }
    }
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
                strategy: args.opt_value_from_str("--strategy")?.unwrap_or_default(),
            },
            Some("read") => SubCmd::Read {
                file: args.value_from_str(["-f", "--file"])?,
                block_size: args
                    .opt_value_from_str(["-s", "--block-size"])?
                    .unwrap_or(32),
                count: args.opt_value_from_str(["-c", "--count"])?.unwrap_or(1),
                strategy: args.opt_value_from_str("--strategy")?.unwrap_or_default(),
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
                strategy,
            } => write_file(&file, block_size, count, strategy, self.verbose).await?,
            SubCmd::Read {
                file,
                block_size,
                count,
                strategy,
            } => read_file(&file, block_size, count, strategy, self.verbose).await?,
        }

        Ok(())
    }
}

async fn write_file(
    file: &str,
    block_size: u64,
    count: u64,
    strategy: Strategy,
    verbose: bool,
) -> Result<()> {
    let file = OpenOptions::new()
        .write(true)
        .create(true)
        .open(file)
        .await?;
    let file = Rc::new(file);

    // let block = &*Vec::leak(vec![0u8; block_size as usize]);
    let mut written = 0;
    let start = Instant::now();
    match strategy {
        Strategy::Sequential => {
            for i in 0..count {
                let pos = i * block_size;
                let block = make_block(block_size, i * block_size / 64);
                written += file.write_at(block, /*pos*/ 0).await.0?;
            }
        }
        Strategy::Async => {
            let mut handles = Vec::with_capacity(count as usize);
            for i in 0..count {
                let file = Rc::clone(&file);
                handles.push(monoio::spawn(async move {
                    let pos = i * block_size;
                    let block = make_block(block_size, i * block_size / 64);
                    file.write_at(block, /*pos*/ 0).await.0
                }));
            }
            for handle in handles {
                written += handle.await?;
            }
        }
        Strategy::Async2 => {
            if count > 0 {
                let mut current = monoio::spawn({
                    let file = Rc::clone(&file);
                    async move {
                        let block = make_block(block_size, 0);
                        file.write_at(block, 0).await.0
                    }
                });
                for i in 1..count {
                    let file = Rc::clone(&file);
                    let next = monoio::spawn(async move {
                        let pos = i * block_size;
                        let block = make_block(block_size, i * block_size / 64);
                        file.write_at(block, /*pos*/ 0).await.0
                    });
                    written += current.await?;
                    current = next;
                }
                written += current.await?;
            }
        }
    }

    let elapsed = start.elapsed().as_secs_f64();

    let speed = (block_size * count) as f64 / elapsed;
    println!(
        "writen {}/{} bytes in {:.6} seconds @ {}/s",
        written,
        block_size * count,
        elapsed,
        ISizeFormatter::new(speed, BINARY),
    );

    Ok(())
}

async fn read_file(
    file: &str,
    block_size: u64,
    count: u64,
    strategy: Strategy,
    verbose: bool,
) -> Result<()> {
    Ok(())
}

fn make_block(block_size: u64, idx: u64) -> Vec<u8> {
    let mut data = vec![0u8; block_size as usize];

    for i in 0..block_size as usize / 64 {
        data[i * 64..i * 64 + 8].copy_from_slice(&u64::to_le_bytes(idx + i as u64));
    }

    data
}
