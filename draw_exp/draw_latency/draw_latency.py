##### run by cmd #####
HELP = 'python draw_latency.py -w workload -c contention -t threads'
##### run by cmd #####

X = "threads"
Y = "latency"
XLABEL = "Percentile"
YLABEL = "Latency(us)"

import pandas as pd
import argparse
import sys

sys.path.extend(['.', '..', '../..'])
import matplotlib.pyplot as plt
from plot.plot import MyPlot
from Schemas import schemas_for_bar
schemas = schemas_for_bar

#################### 参数解析 ####################
parser = argparse.ArgumentParser(HELP)
parser.add_argument("-w", "--workload", type=str, required=True, help="workload: smallbank or ycsb")
parser.add_argument("-c", "--contention", type=str, required=True, help="contention: uniform or skewed")
parser.add_argument("-t", "--threads", type=int, required=True, help="threads")
args = parser.parse_args()
assert args.workload in ['smallbank', 'ycsb', 'tpcc']
workload = args.workload
assert args.contention in ['uniform', 'skewed']
contention = args.contention

savepath = f'latency-{workload}-{contention}.pdf'

#################### 数据准备 ####################
recs = pd.read_csv(f'./data/{workload}_{contention}.csv')
assert args.threads in recs['threads'].unique()
threads = args.threads
recs = recs[recs['threads'] == threads]
inner_schemas = recs['protocol'].unique()
print(inner_schemas)

#################### 画图 ####################
p = MyPlot(1, 1)
ax: plt.Axes = p.axes
ax.grid(axis=p.grid, linewidth=p.border_width)
p.init(ax)

percentiles = ['50%', '95%']
percentiles_label = ['latency_50', 'latency_95']

# Create a dictionary to store data for each schema
data = {schema: [] for schema in inner_schemas}

# Iterate over each percentile label
for pdx, pl in enumerate(percentiles_label):
    # Iterate over each schema
    for schema in inner_schemas:
        # Filter records for the current schema
        records = recs[recs['protocol'] == schema]
        # Append the mean latency to the data for the current schema
        data[schema].append(records[pl].mean())
print(data)

# Iterate over each schema
for idx, (schema, color) in enumerate(schemas):
    # Plot a bar for the current schema
    p.bar(
        ax,
        xdata=[_ + (idx-2.5) * 0.14 for _ in range(len(percentiles_label))],  # Adjust the offset to avoid overlap
        ydata=data[schema],
        color=color, legend_label=schema,
        width=0.14,
        hatch=['//', '\\\\', '||', '--', '++', 'xx'][idx % 6],
    )

# 设置X轴标签
ax.set_xticks(range(len(percentiles_label)))
ax.set_xticklabels(percentiles)
ax.set_xlim(-0.6, 1.6)

# 自适应Y轴变化
p.format_yticks(ax)
# ax.set_ylim(None, p.max_y_data * 1.15)       # 折线图的Y轴上限设置为数据最大值的1.15倍

# 设置label
p.set_labels(ax, XLABEL, YLABEL)

# 设置图例
p.legend(ax, loc="upper center", ncol=3, anchor=(0.5, 1.25))

# 保存
p.save(savepath)