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

sys.path.extend(['.', '..', '../..'])
from plot.parse import parse_records_from_file
import matplotlib.pyplot as plt
from plot.plot import MyPlot

#################### 参数解析 ####################
parser = argparse.ArgumentParser(HELP)
# parser.add_argument("-w", "--workload", type=str, required=True, help="workload: smallbank or ycsb")
# parser.add_argument("-c", "--contention", type=str, required=True, help="contention: uniform or skewed")
# args = parser.parse_args()
# assert args.workload in ['smallbank', 'ycsb']
# workload = args.workload
# assert args.contention in ['uniform', 'skewed']
# contention = args.contention

# savepath = f'threads-tps-{workload}-{contention}.pdf'
savepath = 'test.pdf'

#################### 数据准备 ####################
recs = pd.read_csv('./data/bench_results.csv')
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
p.format_yticks(ax, suffix='K')
ax.set_ylim(None, p.max_y_data * 1.15)       # 折线图的Y轴上限设置为数据最大值的1.15倍

# 设置label
p.set_labels(ax, XLABEL, YLABEL)

# 设置图例
p.legend(ax, loc="upper center", ncol=3, anchor=(0.5, 1.25))

# 保存
p.save(savepath)