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

# Test

The scripts folder contains scripts to test all protocols, including testing fixed zipf with varying threads and fixed threads with varying zipf.

The parameters in bench-xxx-tps.py can be modified to test different benchmarks.

| Parameter    | Meaning                           |
| ------------ | --------------------------------- |
| keys         | Number of keys                     |
| workload     | Which workload to be tested        |
| repeat       | Number of repetitions per bench, averaged at the end |
| threads      | Number of threads                  |
| zipf         | Zipf distribution parameter        |
| times_to_run | Duration of each bench test        |

The protocols list in bench-xxx-tps.py include different protocols in this bench.

All Spectrum protocols and Sparkle protocols(except original Sparkle) has three parameters. The original Sparkle protocol does not need to add EVMType. Please pass parameters in the following way.

```
Spectrum:threads:table_partition:EVMType
```

The EVMType can be one of the following options: **EVMCOW, STRAWMAN, BASIC**


For the Aria/AriaFB protocol, please pass parameters in the following way.

```
Aria:threads:table_partition:batchsize/threads:ReOrderingFlag(True or False)
```

The Calvin protocol is similar to Aria, but does not need to add ReOrderingFlag

For the Serial protocol, please pass parameters in the following way.

```
Serial:EVMType:1
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