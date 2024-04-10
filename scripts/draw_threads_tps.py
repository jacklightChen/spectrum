##### run by cmd #####
HELP = 'python draw_threads_tps.py -w workload -c contention'
##### run by cmd #####

X = "threads"
Y = "commit"
XLABEL = "Threads"
YLABEL = "Troughput(Txn/s)"

import pandas as pd
import argparse
import sys
import re

sys.path.extend(['.', '..', '../..'])
from plot.parse import parse_records_from_file
import matplotlib.pyplot as plt
from plot.plot import MyPlot

#################### 参数解析 ####################
parser = argparse.ArgumentParser(HELP)
parser.add_argument('-f', '--file', type=str, required=True, help='file to plot')
# parser.add_argument("-w", "--workload", type=str, required=True, help="workload: smallbank or ycsb")
# parser.add_argument("-c", "--contention", type=str, required=True, help="contention: uniform or skewed")
args = parser.parse_args()
file: str = args.file
# assert args.workload in ['smallbank', 'ycsb']
# workload = args.workload
# assert args.contention in ['uniform', 'skewed']
# contention = args.contention

# savepath = f'threads-tps-{workload}-{contention}.pdf'
savepath = 'test.pdf'

#################### 数据准备 ####################
if (file.endswith('csv')):
    recs = pd.read_csv(file)
else:
    df = pd.DataFrame(columns=['protocol', 'threads', 'zipf', 'table_partition', 'commit', 'abort'])
    with open(file, 'r') as f:
        content = f.read()
        c_list = content.split('#COMMIT-')
        for c in c_list[1:]:
            c = c.split('CONFIG-')
            hash = c[0]
            c = c[1].split('\n')
            cc = c[0]
            result_str = c[1]
            num_threads = int(cc.split(':')[1])
            table_partitions = int(cc.split(':')[2])
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
    recs = df
schemas = recs['protocol'].unique()
print(schemas)

# schemas = [
#     # 里面是 (协议名称, 颜色(RGB格式)的元组)
#     ('Calvin'           ,       '#45C686'),
#     ('Aria'             ,       '#ED9F54'),
#     ('AriaRe'           ,       '#ED9F54'),
#     ('Sparkle'          ,       '#8E5344'),
#     ('Spectrum'         ,       '#8E5344'),
# ]

#################### 画图 ####################
p = MyPlot(1, 1)
ax: plt.Axes = p.axes
ax.grid(axis=p.grid, linewidth=p.border_width)
p.init(ax)

# for idx, (schema, color) in enumerate(schemas):
for idx, schema in enumerate(schemas):
    records = recs[recs['protocol'] == schema]
    print(records[Y])
    p.plot(
        ax,
        xdata=records[X],
        ydata=records[Y],
        color=None, legend_label=schema,
        # marker=['v', 's', 'o'][idx]
    )

print(type(recs['threads'].unique()))
# 设置X轴标签
ax.set_xticks([int(t) for t in recs['threads'].unique()])

# 自适应Y轴变化
p.format_yticks(ax, suffix='K', step_num=4)
# ax.set_ylim(None, p.max_y_data * 1.15)       # 折线图的Y轴上限设置为数据最大值的1.15倍

# 设置label
p.set_labels(ax, XLABEL, YLABEL)

# 设置图例
p.legend(ax, loc="upper center", ncol=3, anchor=(0.5, 1.25))

# 保存
p.save(savepath)