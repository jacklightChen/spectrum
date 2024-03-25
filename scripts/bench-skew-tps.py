import subprocess
import matplotlib.pyplot as plt
import pandas as pd
import re
import time

import sys
sys.path.extend(['.', '..', '../..'])
from plot.plot import MyPlot

keys = 1000000
workload = 'Smallbank'
threads = 30
times_to_tun = 2
timestamp = int(time.time())

if __name__ == '__main__':
    df = pd.DataFrame(columns=['protocol', 'threads', 'zipf', 'table_partition', 'commit', 'abort'])
    conf = {'stdout': subprocess.PIPE, 'stderr': subprocess.PIPE}
    hash = subprocess.run(["git", "rev-parse", "HEAD"], **conf).stdout.decode('utf-8').strip()
    with open(f'./exp_results/bench_results_{timestamp}', 'w') as f:
        for zipf in [0.7, 0.8, 0.9, 1.0, 1.1, 1.2, 1.3]:
            table_partitions    = 9973
            n_dispatchers       = 6
            # spectrum_dispatcher = num_threads // 6
            protocols       = [
                f"Calvin:{threads}:{n_dispatchers}:{table_partitions}",
                f"Aria:{threads}:{table_partitions}:FALSE", 
                f"Aria:{threads}:{table_partitions}:TRUE",
                f"Sparkle:{threads}:{n_dispatchers}:{table_partitions}", 
                f"Spectrum:{threads}:{n_dispatchers}:{table_partitions}:COPYONWRITE"
            ]
            for cc in protocols:
                print(f"#COMMIT-{hash}",  f"CONFIG-{cc}")
                f.write(f"#COMMIT-{hash} CONFIG-{cc}")
                print(f'../bench {cc} {workload}:{keys}:{zipf} {times_to_tun}s')
                f.write(f'../bench {cc} {workload}:{keys}:{zipf} {times_to_tun}s')
                result = subprocess.run(["../build/bench", cc, f"{workload}:{keys}:{zipf}", f"{times_to_tun}s"], **conf)
                result_str = result.stderr.decode('utf-8').strip()
                print(result_str)
                f.write(result_str)
                commit = float(re.search(r'commit\s+([\d.]+)', result_str).group(1))
                execution = float(re.search(r'execution\s+([\d.]+)', result_str).group(1))
                df.loc[len(df)] = {
                    'protocol': cc.split(':')[0] if cc.split(':')[-1] != 'TRUE' else 'AriaRe', 
                    'threads': threads, 
                    'zipf': zipf, 
                    'table_partition': table_partitions, 
                    'commit': commit,
                    'abort': execution - commit
                }
    df.to_csv(f'./exp_results/bench_results_{timestamp}.csv')

    recs = df
    X, XLABEL = "zipf", "Contention"
    Y, YLABEL = "commit", "Troughput(Txn/s)"
    p = MyPlot(1, 1)
    ax: plt.Axes = p.axes
    ax.grid(axis=p.grid, linewidth=p.border_width)
    p.init(ax)
    for idx, schema in enumerate(recs['protocol'].unique()):
        records = recs[recs['protocol'] == schema]
        p.plot(ax, xdata=records[X], ydata=records[Y], color=None, legend_label=schema,)
    ax.set_xticks(recs['zipf'].unique(), [str(t) for t in recs['zipf'].unique()])
    p.format_yticks(ax, suffix='K')
    # ax.set_ylim(None, p.max_y_data * 1.15)       # 折线图的Y轴上限设置为数据最大值的1.15倍
    p.set_labels(ax, XLABEL, YLABEL)
    p.legend(ax, loc="upper center", ncol=3, anchor=(0.5, 1.25))
    p.save(f'exp_results/bench_results_{timestamp}.pdf')