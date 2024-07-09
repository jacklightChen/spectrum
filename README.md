# Spectrum

* Zhihao Chen, Tianji Yang, Yixiao Zheng, Zhao Zhang, Cheqing Jin, and Aoying Zhou. "Spectrum: Speedy and Strictly-Deterministic Smart Contract Transactions for Blockchain Ledgers." In Proceedings of the 50th International Conference on Very Large Data Bases (VLDB 2024), Pages 2541--2554, Guangzhou, China, August, 2024.

Spectrum is the first deterministic concurrency control (DCC) scheme that preserves consensus-established ordering fairness (by ensuring strict determinism) with high performance for blockchain ledgers.

This repo is for the reproducibility of Spectrum.

# Citation
If you find this repo useful, please cite our paper.
```
@article{chen2024spectrum,
  author       = {Zhihao Chen and
                  Tianji Yang and
                  Yixiao Zheng and
                  Zhao Zhang and
                  Cheqing Jin and
                  Aoying Zhou},
  title        = {Spectrum: Speedy and Strictly-Deterministic Smart Contract Transactions for Blockchain Ledgers},
  journal      = {Proc. {VLDB} Endow.},
  volume       = {17},
  number       = {10},
  pages        = {2541--2554},
  year         = {2024}
}
```

# Preparation
#### Clone Project
For quick clone, use the shallow clone flag `--depth 1`.

```
git clone --depth 1 https://github.com/jacklightChen/spectrum.git
```
#### Compile Env
This project heavily used CXX_20 features. 

Therefore, to compile this project, you either need `clang >= 17 or gcc/g++ >= 12`. 

If you have apt (Advanced Packaging Tool), you can use the following command to install clang 17. 

```
wget -qO- https://apt.llvm.org/llvm.sh | sudo bash -s 17
```

If your clang version is not 17 by default, use the following command for building with clang. 

```sh
CXX=clang++-17 CC=clang-17 cmake -S . -B build
```

# Building Instructions

We use `cmake` as the building system.

To configure the building plan, we use the following instruction. 

```sh
cmake -S . -B build
```

Optionally, one can use debug option to generate debug logs. 
Note that performance will deteriorate significantly if debug mode is enabled. 

```sh
cmake -S . -B build -DNDEBUG=1
```

To build this project, we use the following command. 

```sh
cmake --build build -j
```

This build command will compile and link the main library along with several executables. 

The generated executable is called `bench`. Its basic usage is as follows:

```sh
./build/bench [PROTOCOL] [WORKLOAD] [BENCH TIME]
```

For example:

```sh
./build/bench Spectrum:36:9973:COPYONWRITE Smallbank:1000000:0 2s
```

# Evaluation

The scripts folder contains scripts to test all execution schemes, including testing fixed zipf with varying threads and fixed threads with varying zipf.

The parameters in **bench-threads-tps.py/bench-skew-tps.py** can be modified to test different benchmarks.

| Parameter    | Meaning                           |
| ------------ | --------------------------------- |
| keys         | Number of keys                     |
| workload     | Which workload to be tested        |
| repeat       | Number of repetitions per bench, averaged at the end |
| threads      | Number of threads                  |
| zipf         | Zipf distribution parameter        |
| times_to_run | Duration of each bench test        |

All Spectrum schemes and Sparkle schemes(except original Sparkle) have three parameters. The original Sparkle scheme does not need to include EVMType. Please pass parameters in the following way.

```
Spectrum:threads:table_partition:EVMType
```

The EVMType can be one of the following options: **EVMCOW, STRAWMAN, BASIC**


For the Aria/AriaFB scheme, please pass parameters in the following way.

```
Aria:threads:table_partition:batchsize/threads:ReOrderingFlag(True or False)
```

The Calvin scheme is similar to Aria, but does not need to include ReOrderingFlag.

For the Serial scheme, please pass parameters in the following way.

```
Serial:EVMType:1
```
