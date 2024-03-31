EXECUTABLE=$2
T=2s

varthread() {
    for b in $1; do
        echo    "------"
        for i in {1..16}; do
            p=$2
            eval    "p=\"${p//_/$i}\""
            echo    "@$p $b"
	    sleep 	2
	    $EXECUTABLE  $p $b $T || (echo "crash"; exit)
        done
    done
}

varskew() {
    for p in $1; do
        for b in $2; do
            for s in {0..20}; do
                x=$(jq -n 2*$s/20)
                x=${b//_/$x}
                echo    @$p $x
		sleep 	2
                $EXECUTABLE  $p $x $T || (echo "crash"; exit)
            done
        done
    done
}

case $1 in
    thread-tps)
        BENCH="
        Smallbank:1000000:0
        YCSB:1000000:0
        Smallbank:1000000:1
        YCSB:1000000:1
        Smallbank:1000000:2
        YCSB:1000000:2
        "
        # varthread "$BENCH" 'Aria:$((_*4)):1024:4:FALSE'
        varthread "$BENCH" 'Sparkle:$((_*4)):1024'
        varthread "$BENCH" 'Spectrum:$((_*4)):1024:COPYONWRITE'
        # varthread "$BENCH" 'SpectrumSched:$((_*4)):1024:COPYONWRITE'
    ;;
    skew-tps)
        BENCH="
        Smallbank:1000000:_
        YCSB:1000000:_
        "
        PROTOCOL="
        Sparkle:16:1024
        Aria:16:1024:12:FALSE
        Spectrum:16:1024:1:COPYONWRITE
        "
        varskew "$PROTOCOL" "$BENCH"
    ;;
    plot-skew-tps)
        BENCH=""
    ;;
esac
