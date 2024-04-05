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
zipf = 1.2
times_to_tun = 2
timestamp = int(time.time())

if __name__ == '__main__':
    df = pd.DataFrame(columns=['protocol', 'threads', 'zipf', 'table_partition', 'commit', 'abort'])
    conf = {'stdout': subprocess.PIPE, 'stderr': subprocess.PIPE}
    hash = subprocess.run(["git", "rev-parse", "HEAD"], **conf).stdout.decode('utf-8').strip()
    batch_size = 120
    with open(f'./exp_results/bench_results_{timestamp}', 'w') as f:
        for num_threads in list(range(6, 42, 6)):
            table_partitions    = 9973
            protocols       = [
                # f"Calvin:{num_threads}:{table_partitions}:{batch_size // num_threads}",
                # f"Aria:{num_threads}:{table_partitions}:{batch_size // num_threads}:FALSE", 
                # f"Aria:{num_threads}:{table_partitions}:{batch_size // num_threads}:TRUE",
                f"Sparkle:{num_threads}:{table_partitions}", 
                f"Spectrum:{num_threads}:{table_partitions}:COPYONWRITE",
                f"SpectrumPreSched:{num_threads}:{table_partitions}:COPYONWRITE",
                # f"Dummy:{num_threads}:{table_partitions}:COPYONWRITE",
                # f"Serial:BASIC:{1}",
            ]
            for cc in protocols:
                print(f"#COMMIT-{hash}",  f"CONFIG-{cc}")
                f.write(f"#COMMIT-{hash} CONFIG-{cc}")
                print(f'../bench {cc} {workload}:{keys}:{zipf} {times_to_tun}s')
                f.write(f'../bench {cc} {workload}:{keys}:{zipf} {times_to_tun}s')
                result = subprocess.run(["../build/bin/bench", cc, f"{workload}:{keys}:{zipf}", f"{times_to_tun}s"], **conf)
                result_str = result.stderr.decode('utf-8').strip()
                print(result_str)
                f.write(result_str)
                commit = float(re.search(r'commit\s+([\d.]+)', result_str).group(1))
                execution = float(re.search(r'execution\s+([\d.]+)', result_str).group(1))
                df.loc[len(df)] = {
                    'protocol': cc.split(':')[0] if cc.split(':')[-1] != 'TRUE' else 'AriaRe', 
                    'threads': num_threads, 
                    'zipf': 0, 
                    'table_partition': table_partitions, 
                    'commit': commit,
                    'abort': execution - commit
                }
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
