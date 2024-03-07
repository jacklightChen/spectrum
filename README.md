# TODO

- [ ] Zipf
- [ ] Calvin
- [ ] TPC-C
- [ ] Test New Transaction Protocol on Serial

# Instructions

We use cmake as the building system.

To configure the building plan, we use the following instruction. 

```sh
cmake -S . -B build
```

Optionally, we can use debug option to generate debug logs. 
Note that performance will deterriorate significantly if debug mode is enabled. 

```sh
cmake -S . -B build -DDEBUG=ON
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

# Experiments

