# Spectrum
Spectrum is the first concurrent deterministic execution scheme that preserves consensus-established ordering fairness (by ensuring strict determinism) with high performance for blockchain ledgers.

This repo is for the reproducibility of Spectrum.

Zhihao Chen, Tianji Yang, Yixiao Zheng, Zhao Zhang, Cheqing Jin, and Aoying Zhou. Spectrum: Speedy and Strictly-Deterministic Smart Contract Transactions for Blockchain Ledgers. (under revision)

# Building Instructions

We use `cmake` as the building system.

To configure the building plan, we use the following instruction. 

```sh
cmake -S . -B build
```

Optionally, we can use debug option to generate debug logs. 
Note that performance will deteriorate significantly if debug mode is enabled. 

```sh
cmake -S . -B build -DASAN=1
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

This project heavily used CXX_20 features. 

Therefore, to compile this project, you either need clang >= 17 or gcc/g++ >= 12 . 

If you have apt (Advanced Packaging Tool), you can use the following command to install clang 17. 

```
wget -qO- https://apt.llvm.org/llvm.sh | sudo bash -s 17
```

If you clang version is not 17 by default, use the following command for building with clang. 

```sh
CXX=clang++-17 CC=clang-17 cmake -S . -B build
```

# Experiments

We conduct experiments to show that Spectrum protocol is better than Sparkle protocol and Aria protocol in high-contention scenarios, and only have minor performance gap in comparison to Sparkle protocol in low-contention scenarios. 

# Credit

+ hananbeer/sqlidity

# Implementation Details

## Sparkle

There is a minor bug that we fixed previously. 

When we send shutdown signal to sparkle executors, some of them may stop immediately and destruct its holding transaction, while its read dependencies (implemented as raw pointers) are still held inside the multi-version table (SparkleTable) and accessed by other transactions in their execution phase. 

The solution is to add the transaction back to idling queue. 

