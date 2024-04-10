import subprocess
import matplotlib.pyplot as plt
import pandas as pd
import re
import time

import sys
sys.path.extend(['.', '..', '../..'])
from plot.plot import MyPlot

keys = 1000000
workload = 'YCSB'
repeat = 10
zipf = 1.1
times_to_tun = 2
timestamp = int(time.time())

if __name__ == '__main__':
    df = pd.DataFrame(columns=['protocol', 'threads', 'zipf', 'table_partition', 'commit', 'abort', 'latency_50', 'latency_75', 'latency_95', 'latency_99'])
    conf = {'stdout': subprocess.PIPE, 'stderr': subprocess.PIPE}
    hash = subprocess.run(["git", "rev-parse", "HEAD"], **conf).stdout.decode('utf-8').strip()
    batch_size = 100
    with open(f'./exp_results/bench_results_{timestamp}', 'w') as f:
        for num_threads in list(range(6, 42, 6)):
            table_partitions    = 9973
            protocols       = [
                f"Calvin:{num_threads}:{table_partitions}:{batch_size // num_threads}",
                f"Aria:{num_threads}:{table_partitions}:{batch_size // num_threads}:FALSE", 
                f"Aria:{num_threads}:{table_partitions}:{batch_size // num_threads}:TRUE",
                f"Sparkle:{num_threads}:{table_partitions}", 
                f"Spectrum:{num_threads}:{table_partitions}:COPYONWRITE",
                # f"SpectrumPreSched:{num_threads}:{table_partitions}:COPYONWRITE",
                # f"Dummy:{num_threads}:{table_partitions}:COPYONWRITE",
                f"Serial:BASIC:{1}",
                # f"Spectrum:{num_threads}:{table_partitions}:COPYONWRITE",
                # f"Spectrum:{num_threads}:{table_partitions}:STRAWMAN",
                # f"SpectrumNoPartial:{num_threads}:{table_partitions}:BASIC",
            ]
            for cc in protocols:
                print(f"#COMMIT-{hash}",  f"CONFIG-{cc}")
                f.write(f"#COMMIT-{hash} CONFIG-{cc}\n")
                print(f'../bench {cc} {workload}:{keys}:{zipf} {times_to_tun}s')
                f.write(f'../bench {cc} {workload}:{keys}:{zipf} {times_to_tun}s' + '\n')
                sum_commit = 0
                sum_execution = 0
                sum_latency_50 = 0
                sum_latency_75 = 0
                sum_latency_95 = 0
                sum_latency_99 = 0

                # if cc.split(':')[0] in ['Sparkle', 'Spectrum']:
                #     repeat = 10
                #     times_to_tun = 5
                # else:
                #     repeat = 5
                #     times_to_tun = 2

                succeed_repeat = 0
                for _ in range(repeat):
                    try:
                        result = subprocess.run(["../build/bin/bench", cc, f"{workload}:{keys}:{zipf}", f"{times_to_tun}s"], **conf)
                        result_str = result.stderr.decode('utf-8').strip()
                        f.write(result_str + '\n')
                        sum_commit += float(re.search(r'commit\s+([\d.]+)', result_str).group(1))
                        sum_execution += float(re.search(r'execution\s+([\d.]+)', result_str).group(1))
                        sum_latency_50 += float(re.search(r'latency\(50%\)\s+(\d+)us', result_str).group(1))
                        sum_latency_75 += float(re.search(r'latency\(75%\)\s+(\d+)us', result_str).group(1))
                        sum_latency_95 += float(re.search(r'latency\(95%\)\s+(\d+)us', result_str).group(1))
                        sum_latency_99 += float(re.search(r'latency\(99%\)\s+(\d+)us', result_str).group(1))
                        succeed_repeat += 1
                    except Exception as e:
                        print(e)
                df.loc[len(df)] = {
                    'protocol': cc.split(':')[0] if cc.split(':')[-1] != 'FALSE' else 'AriaFB', 
                    'threads': num_threads, 
                    'zipf': 0, 
                    'table_partition': table_partitions, 
                    'commit': sum_commit / succeed_repeat,
                    'abort': (sum_execution - sum_commit) / succeed_repeat,
                    'latency_50': sum_latency_50 / succeed_repeat,
                    'latency_75': sum_latency_75 / succeed_repeat,
                    'latency_95': sum_latency_95 / succeed_repeat,
                    'latency_99': sum_latency_99 / succeed_repeat,
                }
                print(df)
    df.to_csv(f'./exp_results/bench_results_{timestamp}.csv')

    recs = df
    X, XLABEL = "threads", "Threads"
    Y, YLABEL = "commit", "Troughput(Txn/s)"
    p = MyPlot(1, 1)
    ax: plt.Axes = p.axes
    ax.grid(axis=p.grid, linewidth=p.border_width)
    p.init(ax)
    for idx, schema in enumerate(recs['protocol'].unique()):
        records = recs[recs['protocol'] == schema]
        p.plot(ax, xdata=records[X], ydata=records[Y], color=None, legend_label=schema,)
    ax.set_xticks([int(t) for t in recs['threads'].unique()])
    p.format_yticks(ax, suffix='K')
    # ax.set_ylim(None, p.max_y_data * 1.15)       # 折线图的Y轴上限设置为数据最大值的1.15倍
    p.set_labels(ax, XLABEL, YLABEL)
    p.legend(ax, loc="upper center", ncol=3, anchor=(0.5, 1.25))
    p.save(f'exp_results/bench_results_{timestamp}.pdf')
