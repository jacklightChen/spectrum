EXECUTABLE=$2

varthread() {
    for b in $2; do
        echo    "------"
        for i in {1..2}; do
            p=$1
            eval    "p=\"${p//_/$i}\""
            echo    "@$p $b"
            $EXECUTABLE  $p $b 100ms
        done
    done
}

varskew() {
    for p in $2; do
        for b in $1; do
            for s in {0..20}; do
                x=$(jq -n 2*$s/20)
                x=${b//_/$x}
                echo    @$p $x
                $EXECUTABLE  $p $x 100ms
            done
        done
    done
}

case $1 in
    thread-tps)
        BENCH="
        Smallbank:1000000:0
        YCSB:1000000:0
        "
        varthread "$BENCH" 'Aria:$((_*4)):1024:$((50/_)):FALSE'
        varthread "$BENCH" 'Sparkle:$((_*4)):1024'
        varthread "$BENCH" 'Spectrum:$((_*4)):1024:COPYONWRITE'
        varthread "$BENCH" 'SpectrumSched:$((_*4)):1024:COPYONWRITE'
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