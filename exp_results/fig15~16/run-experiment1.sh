# Set some variables
step=10
start=80
limit=140
logf=experiment-log
logv=verbose-log
spectrum_root_path="../../spectrum"
current=$(pwd)

# Parse the arguments
if [ ! -n "$1" ]; then keys=1000000; else keys=$1; fi
if [ ! -n "$2" ]; then threads=18; else threads=$2; fi
if [ ! -n "$3" ]; then contract_type=11; else contract_type=$3; fi
if [ ! -n "$4" ]; then partition_num=1; else partition_num=$4; fi	# if partition_num bigger than 1, we need to set global_key_space to false
if [ ! -n "$5" ]; then batch_size=100; else batch_size=$5; fi

# Set the default variables
time_to_init=30									# the time to initialize the database
time_to_run=5									# the time to run the experiment
execute="$spectrum_root_path/build/dcc_bench"	# the path of dcc_bench
timestamp=$(date +"%Y-%m-%d-%H-%M")
spectrum_lock_div=3								# the number of Spectrum threads per lock manager

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

# Switch to the with-partial-predsched branch and compile dcc_bench
cd $spectrum_root_path
git checkout -f with-partial-predsched
rm -rf build
mkdir build
echo "cmake build compile"
cmake -S . -B build
cmake --build build -- -j16
cd $current

# Run the experiment of Spectrum protocol (with predictive scheduling)
i=$start
while [ $i -lt $limit ]
do
	lock_manager=$(python3 -c "print($threads // $spectrum_lock_div)")
	subkeys=$(python3 -c "print($keys // $lock_manager)")

	zipf=$(python3 -c "print($i / 100 + 0.00001)")
	echo "@ Spectrum-pp; zipf=$zipf" >> $logf
	echo "@ Spectrum-pp; zipf=$zipf" >> $logv
	echo "@ Spectrum-pp; zipf=$zipf"
	$execute \
	--zipf=$zipf \
	--protocol=Spectrum \
	--keys=$subkeys \
	--threads=$(python3 -c "print($threads + $lock_manager)") \
	--contract_type=$contract_type \
	--batch_size=$batch_size \
	--sparkle_lock_manager=$lock_manager \
	--partition_num=$lock_manager \
	--global_key_space=false \
	--sche_only_hotspots=true \
	--pre_sched_num=100 \
	--re_sched_num=200 \
	--time_to_run=$time_to_run >& tmp &
	sleep $(python3 -c "print($time_to_run + $time_to_init)")
	kill -9 $(pgrep dcc_bench)
	cat tmp >> $logv
	cat tmp | grep -o "average commit.*" >> $logf
	cat tmp | grep -o "average commit.*"
	i=$(($i+$step))
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
	echo "@ Spectrum-p; zipf=$zipf" >> $logf
	echo "@ Spectrum-p; zipf=$zipf" >> $logv
	echo "@ Spectrum-p; zipf=$zipf"
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

# Run the experiment with AriaFB protocol
i=$start
while [ $i -lt $limit ]
do
	lock_manager=$partition_num
	subkeys=$(python3 -c "print($keys // $lock_manager)")
	
	zipf=$(python3 -c "print($i / 100 + 0.00001)")
	echo "@ AriaFB; zipf=$zipf" >> $logf
	echo "@ AriaFB; zipf=$zipf" >> $logv
	echo "@ AriaFB; zipf=$zipf"
	$execute \
	--zipf=$zipf \
	--protocol=AriaFB \
	--keys=$subkeys \
	--threads=$threads \
	--contract_type=$contract_type \
	--partition_num=$partition_num \
	--batch_size=$batch_size \
	--ariaFB_lock_manager=$lock_manager \
	--global_key_space=false \
	--time_to_run=$time_to_run >& tmp &
	sleep $(python3 -c "print($time_to_run + $time_to_init)")
	kill -9 $(pgrep dcc_bench)
	cat tmp >> $logv
	cat tmp | grep -o "average commit.*" >> $logf
	cat tmp | grep -o "average commit.*"
	i=$(($i+$step))
done

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
