#![allow(unused)] // Remove this line to enable warnings.

use anyhow::{Context, Ok, Result};
use humansize::{ISizeFormatter, SizeFormatter, BINARY};
use io_uring::{opcode, squeue::Flags, types, IoUring};
use monoio::fs::{File, OpenOptions};
use std::{
    collections::VecDeque,
    default, fs,
    io::{Read, Write},
    os::unix::io::AsRawFd,
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
    IOUring,
    IOUring2,
    IOUring8,
}

impl FromStr for Strategy {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "seq" => Ok(Self::Sequential),
            "async" => Ok(Self::Async),
            "async2" => Ok(Self::Async2),
            "io_uring" => Ok(Self::IOUring),
            "io_uring2" => Ok(Self::IOUring2),
            "io_uring8" => Ok(Self::IOUring8),
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
    path: &str,
    block_size: u64,
    count: u64,
    strategy: Strategy,
    verbose: bool,
) -> Result<()> {
    let file = OpenOptions::new()
        .write(true)
        .create(true)
        .open(path)
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
                file.write_all_at(block, /*pos*/ 0).await.0?;
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
        Strategy::IOUring => {
            drop(file);

            let mut ring = IoUring::new(8)?;

            let file = fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(path)?;
            let fd = types::Fd(file.as_raw_fd());

            for i in 0..count {
                // let mut buf = make_block(block_size, i * block_size / 64);
                let buf = make_block_mem_aligned(block_size, i * block_size / 64)?;
                let write_e = opcode::Write::new(fd, buf, block_size as _)
                    .build()
                    .user_data(0x42);

                // Note that the developer needs to ensure
                // that the entry pushed into submission queue is valid (e.g. fd, buffer).
                unsafe {
                    ring.submission()
                        .push(&write_e)
                        .expect("submission queue is full");
                }

                ring.submit_and_wait(1)?;

                let cqe = ring.completion().next().expect("completion queue is empty");

                assert_eq!(cqe.user_data(), 0x42);
                assert!(cqe.result() >= 0, "write error: {}", cqe.result());

                mem_aligned_free(buf, block_size as usize, 4096);
            }
        }
        Strategy::IOUring2 => {
            drop(file);

            if count > 0 {
                let mut ring = IoUring::new(8)?;

                let file = fs::OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(path)?;
                let fd = types::Fd(file.as_raw_fd());

                let mut write = |ring: &mut IoUring, buf: *mut u8| {
                    let write_e = opcode::Write::new(fd, buf, block_size as _)
                        .build()
                        .user_data(0x42);

                    // Note that the developer needs to ensure
                    // that the entry pushed into submission queue is valid (e.g. fd, buffer).
                    unsafe {
                        ring.submission()
                            .push(&write_e)
                            .expect("submission queue is full");
                    }

                    Ok(())
                };
                let wait = |ring: &mut IoUring| {
                    ring.submit_and_wait(1)?;

                    let cqe = ring.completion().next().expect("completion queue is empty");

                    assert_eq!(cqe.user_data(), 0x42);
                    assert!(cqe.result() >= 0, "write error: {}", cqe.result());

                    Ok(())
                };

                let mut current = make_block_mem_aligned(block_size, 0)?;
                write(&mut ring, current)?;

                for i in 1..count {
                    let next = make_block_mem_aligned(block_size, i * block_size / 64)?;
                    write(&mut ring, next)?;
                    wait(&mut ring)?;
                    mem_aligned_free(current, block_size as usize, 4096);
                    current = next;
                }
                wait(&mut ring)?;
                mem_aligned_free(current, block_size as usize, 4096);
            }
        }
        Strategy::IOUring8 => {
            drop(file);

            let mut ring = IoUring::new(8)?;

            let file = fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(path)?;
            let fd = types::Fd(file.as_raw_fd());

            let mut write = |ring: &mut IoUring, i: u64, buf: *mut u8| {
                let write_e = opcode::Write::new(fd, buf, block_size as _)
                    .build()
                    .flags(Flags::IO_LINK)
                    .user_data(i);

                // Note that the developer needs to ensure
                // that the entry pushed into submission queue is valid (e.g. fd, buffer).
                unsafe {
                    ring.submission()
                        .push(&write_e)
                        .expect("submission queue is full");
                }

                Ok(())
            };
            let wait = |ring: &mut IoUring, want: usize| {
                ring.submit_and_wait(want)?;

                for _ in 0..want {
                    let cqe = ring.completion().next().expect("completion queue is empty");
                    println!("write result: {} @ {}", cqe.result(), cqe.user_data());
                    // if cqe.result() < 0 {
                    //     println!("write error: {} @ {}", cqe.result(), cqe.user_data());
                    // }
                    // assert_eq!(cqe.user_data(), 0x42);
                    // assert!(cqe.result() >= 0, "write error: {}", cqe.result());
                }

                Ok(())
            };

            let mut queue = VecDeque::with_capacity(8);
            for i in 0..u64::min(7, count) {
                let buf = make_block_mem_aligned(block_size, i * block_size / 64)?;
                write(&mut ring, i, buf)?;
                queue.push_back(buf);
            }
            for i in 7..count {
                let buf = make_block_mem_aligned(block_size, i * block_size / 64)?;
                write(&mut ring, i, buf)?;
                queue.push_back(buf);

                wait(&mut ring, 1)?;
                mem_aligned_free(queue.pop_front().unwrap(), block_size as usize, 4096);
            }
            while let Some(buf) = queue.pop_front() {
                wait(&mut ring, 1)?;
                mem_aligned_free(buf, block_size as usize, 4096);
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

fn make_block_mem_aligned(block_size: u64, idx: u64) -> Result<*mut u8> {
    let mut ptr = mem_aligned(block_size as usize, 4096)?;

    let slice = unsafe { std::slice::from_raw_parts_mut(ptr, block_size as usize) };
    for i in 0..block_size as usize / 64 {
        slice[i * 64..i * 64 + 8].copy_from_slice(&u64::to_le_bytes(idx + i as u64));
    }

    Ok(ptr)
}

fn mem_aligned(size: usize, align: usize) -> Result<*mut u8> {
    let layout = std::alloc::Layout::from_size_align(size, align).context("invalid layout")?;
    let ptr = unsafe { std::alloc::alloc(layout) };
    if ptr.is_null() {
        Err(anyhow::anyhow!("failed to allocate memory"))
    } else {
        Ok(ptr)
    }
}

fn mem_aligned_free(ptr: *mut u8, size: usize, align: usize) {
    let layout = std::alloc::Layout::from_size_align(size, align).unwrap();
    unsafe { std::alloc::dealloc(ptr, layout) }
}
