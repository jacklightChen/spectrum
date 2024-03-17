# TODO

- [x] Zipf
- [x] Aria w/o Reordering
- [ ] Calvin
- [ ] TPC-C
- [x] Serial

# Instructions

We use cmake as the building system.

To configure the building plan, we use the following instruction. 

```sh
cmake -S . -B build
```

Optionally, we can use debug option to generate debug logs. 
Note that performance will deterriorate significantly if debug mode is enabled. 

```sh
cmake -S . -B build -DDEBUG=1
```

To build this project, we use the following command. 

```sh
cmake --build build -j
```

This build command will compile and link the main library along with several executables. 

The executable the we use is called bench. The basic usage is: 

```sh
./build/bench [PROTOCOL] [WORKLOAD] [BENCH TIME]
```

# Caution

This project heavily used CXX 20 features. 

Therefore, to compile this project, you either need clang >= 17 or gcc/g++ >= 12 . 

If you have apt (Advanced Packaging Tool), you can use the following command to install clang 17. 

```
wget -qO- https://apt.llvm.org/llvm.sh | sudo bash -s 17
```

If you clang is not 17 by default, use the following command for building with clang. 

```sh
CXX=clang++-17 CC=clang-17 cmake -S . -B build
```

# Experiments

Currently Aria, Sparkle and Spectrum all works properly. 

```sh
./build/bench Aria:8:32:32:TRUE   Smallbank:1000000:2.0 5000ms
./build/bench Aria:8:32:32:FALSE  Smallbank:1000000:1.5 5000ms
./build/bench Sparkle:8:32        Smallbank:1000000:0.5 5000ms
./build/bench Spectrum:8:32:32:STRAWMAN       Smallbank:1000000:0.5 5000ms
./build/bench Spectrum:8:32:32:COPYONWRITE    Smallbank:1000000:0.5 5000ms
```

# Dev Log

## Sparkle

1. 

## Pre-schedule

1. Only schedule hot keys -- select highest. (YCSB / Smallbank)

## Re-schedule

1. Detect: transaction cannot commit / don't re-execute immediately. 
2. Wait until (pessimistic)

