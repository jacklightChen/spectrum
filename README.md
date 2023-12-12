# Spectrum

Spectrum is the first concurrent deterministic execution scheme that preserves consensus-established ordering fairness (by ensuring strict determinism) with high performance for blockchain ledgers.

This repo is for the reproducibility of Spectrum.

**Zhihao Chen**, Tianji Yang, Yixiao Zheng, Zhao Zhang, Cheqing Jin, and Aoying Zhou. Spectrum: Speedy and Strictly-Deterministic Smart Contract Transactions for Blockchain Ledgers. (under review)

## Branches

```shell
* main
  no-partial
  with-partial
  with-partial-predsched
```

Different branches correspond to different implementations.

- `no-partial` corresponds to the baselines of Serial, AriaFB, Sparkle.
- `with-partial` corresponds to the Spectrum with partial rollback.
- `with-partial-predsched` corresponds to the Spectrum with partial rollback and predictive scheduling.

## Dependencies
```shell
sudo apt-get update
sudo apt-get install -y zip make cmake g++-11 libjemalloc-dev libboost-dev libgoogle-glog-dev
```
## Build
```shell
cd spectrum
git checkout -f with-partial  # (choose the branch you need)
mkdir build
cmake -S . -B build
cmake --build build -- -j16
```

This will generate a binary file named `dcc_bench` in the `build` directory, which is used for testing.

## Testing

After generating the `dcc_bench` binary file, you can perform tests using the following command:

```shell
./build/dcc_bench \
	--threads=$threads \
	--protocol=Spectrum \
	--keys=$keys \
	--zipf=$zipf \
	--contract_type=$contract_type \
	--batch_size=$batch_size \
	--time_to_run=$time_to_run >& tmp &
```

Using the original Sparkle protocol as an example, the parameters are as follows:

|   Parameter   |                           Meaning                            |
| :-----------: | :----------------------------------------------------------: |
| contract_type | Contract type (usually an integer, refer to `src/dcc/benchmark/evm/Contract.h`) |
|   protocol    | Protocol type (e.g., Sparkle, Serial, AriaFB, Spectrum etc.) |
|     keys      |                        Number of keys                        |
|    threads    |                      Number of threads                       |
|     zipf      |             Value of theta in Zipf distribution              |
|  batch_size   |                          Batch size                          |
|  time_to_run  |                Total running time in seconds                 |

Additional parameters:

|      Parameter       |                           Meaning                            |
| :------------------: | :----------------------------------------------------------: |
|    partition_num     | Number of partitions (used when `global_key_space` is set to false) |
|   global_key_space   |               Whether it is a global key space               |
| ariaFB_lock_manager  | Number of schedulers needed for Calvin in the AriaFB protocol |
| sparkle_lock_manager | Number of schedulers needed for predictive scheduling in the Spectrum protocol |
|  sche_only_hotspots  |            Whether to schedule only hotspot keys             |
|     pre_sche_num     |       Number of hotspot keys for predictive scheduling       |

## Evaluation

In most cases, you need to compare the performance of different protocols while keeping other parameters constant. To test a single variable, create a folder for it, write a `run-experiment.sh` script inside, and then run the experiment by passing arguments through the command line or by modifying the values directly within the script. Here's an example with the variable "zipf":

You can copy the directory `exp_results` out of the whole project `spectrum`, and command line to run the experiment in each folder in `exp_results`:

```shell
bash run-experiment.sh
```

Contents of `run-experiment.sh`:

```shell
# Set some variables (you can modify these values)
step=10
start=80
limit=140
logf=experiment-log
logv=verbose-log
spectrum_root_path="../../spectrum"	# may need to be modified
current=$(pwd)

# Parse the arguments
if [ ! -n "$1" ]; then keys=1000000; else keys=$1; fi
if [ ! -n "$2" ]; then threads=36; else threads=$2; fi
if [ ! -n "$3" ]; then contract_type=11; else contract_type=$3; fi
if [ ! -n "$4" ]; then partition_num=1; else partition_num=$4; fi	# if partition_num bigger than 1, we need to set global_key_space to false
if [ ! -n "$5" ]; then batch_size=100; else batch_size=$5; fi

# Set the default variables
time_to_init=30 # the time to initialize the database
time_to_run=5 # the time to run the experiment
execute="$spectrum_root_path/build/dcc_bench"	# the path of dcc_bench
timestamp=$(date +"%Y-%m-%d-%H-%M")

# Set the log file names
logf="$logf-$timestamp"
logv="$logv-$timestamp"

# Create log files and write some information
for l in $logf $logv
do
	echo "run experiment! zipf is changing!" > $l
	echo "keys=$keys" >> $l
	echo "threads=$threads" >> $l
	echo "contract=$contract_type" >> $l
	echo "partition_num=$partition_num" >> $l
	echo "batch_size=$batch_size" >> $l
done

# Switch to the with-partial branch and compile dcc_bench
cd $spectrum_root_path
git checkout -f with-partial
rm -rf build
mkdir build
echo "cmake build compile"
cmake -S . -B build
cmake --build build -- -j16
cd $current

# Run the experiment with Spectrum protocol
i=$start
while [ $i -lt $limit ]
do
	zipf=$(python3 -c "print($i / 100 + 0.00001)")
	echo "@ Spectrum; zipf=$zipf" >> $logf
	echo "@ Spectrum; zipf=$zipf" >> $logv
	echo "@ Spectrum; zipf=$zipf"
	$execute \
	--zipf=$zipf \
	--protocol=Spectrum \
	--keys=$keys \
	--threads=$threads \
	--contract_type=$contract_type \
	--partition_num=$partition_num \
	--batch_size=$batch_size \
	--time_to_run=$time_to_run >& tmp &
	sleep $(python3 -c "print($time_to_run + $time_to_init)")
	kill -9 $(pgrep dcc_bench)
	cat tmp >> $logv
	cat tmp | grep -o "average commit.*" >> $logf
	cat tmp | grep -o "average commit.*"
	i=$(($i+$step))
done

# Switch to the no-partial branch and compile dcc_bench
cd $spectrum_root_path
git checkout -f no-partial
rm -rf build
mkdir build
echo "cmake build compile"
cmake -S . -B build
cmake --build build -- -j16
cd $current

# Run the experiment with the Sparkle protocol
i=$start
while [ $i -lt $limit ]
do
	zipf=$(python3 -c "print($i / 100 + 0.00001)")
	echo "@ Sparkle; zipf=$zipf" >> $logf
	echo "@ Sparkle; zipf=$zipf" >> $logv
	echo "@ Sparkle; zipf=$zipf"
	$execute \
	--zipf=$zipf \
	--protocol=Sparkle \
	--keys=$keys \
	--threads=$threads \
	--contract_type=$contract_type \
	--batch_size=$batch_size \
	--time_to_run=$time_to_run >& tmp &
	sleep $(python3 -c "print($time_to_run + $time_to_init)")
	kill -9 $(pgrep dcc_bench)
	cat tmp >> $logv
	cat tmp | grep -o "average commit.*" >> $logf
	cat tmp | grep -o "average commit.*"
	i=$(($i+$step))
done
```

The experiment results are logged in the `experiment-log` file, and the verbose logs during runtime are recorded in the `verbose-log` file.
