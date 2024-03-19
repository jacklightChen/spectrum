import pandas as pd


def parse_record(record):
    record = record.strip()
    main_record = record.split('] ')[0].strip()
    side_record = record.split('] ')[1:]
    # print("SIDERECORD", len(side_record), side_record, "\n")
    def fetch_word_after(prefix, callback, record=main_record):
        if len(main_record.split(prefix)) > 1:
            return callback(record.split(prefix)[1].strip().split(' ')[0].strip(',').split('\n')[0].strip())
        else:
            return None
    return {
        'protocol': record.split(';')[0].strip(),
        'cross_ratio': fetch_word_after("cross_ratio=", int),
        'threads': fetch_word_after("threads=", int),
        "zipf": fetch_word_after("zipf=", float),
        'network size': fetch_word_after("network size: ", int),
        'multi commit network size': fetch_word_after("multi commit network size: ", int),
        'window size': fetch_word_after("windowSize=", int),
        'average commit': fetch_word_after("commit: ", float),
        'average abort': fetch_word_after("abort: ", float),
        'average cascade abort': fetch_word_after("abort cascade: ", float),
        'average operations': fetch_word_after("average operations: ", float),
        'revert length': sum([
            i * fetch_word_after("partial revert " + str(i) + ": ", float)
            for i in range(0, 11) if fetch_word_after("partial revert " + str(i) + ": ", float)
        ]) if "partial revert" in record else 10 * (fetch_word_after("abort: ", float) or 0),
        'original revert length': 10 * (fetch_word_after("abort: ", float) or 0),
        **{f'partial abort {i}': fetch_word_after(f'partial revert {i}: ', float) for i in range(0, 11)},
        **{f'commit at sec {i}': fetch_word_after(f'commit: ', int, record=r) for i, r in enumerate(side_record)},
        **{f'abort at sec {i}': fetch_word_after(f'abort: ', int, record=r) for i, r in enumerate(side_record)}
    }

def parse_records_from_file(content: str):
    return pd.DataFrame(list(map(parse_record, content.split("@")[1:])))

def parse_meta(meta):
    print(meta)