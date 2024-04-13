##### run by cmd #####
HELP = 'python draw_skew_abort.py -w workload -t threads'
##### run by cmd #####

X = "zipf"
Y = "abort"
XLABEL = "Contention Degree (Zipf)"
YLABEL = "Aborts / Commit"

import pandas as pd
import argparse
import sys

sys.path.extend(['.', '..', '../..'])
import matplotlib.pyplot as plt
from plot.plot import MyPlot
from Schemas import schemas, schemas_for_pre

#################### 参数解析 ####################
parser = argparse.ArgumentParser(HELP)
parser.add_argument("-w", "--workload", type=str, required=True, help="workload: smallbank or ycsb")
parser.add_argument("-t", "--threads", type=str, required=True, help="threads")
args = parser.parse_args()
assert args.workload in ['smallbank', 'ycsb', 'tpcc', 'pre']
workload = args.workload
if workload == 'tpcc':
    XLABEL = "Number of Items"
if workload == 'pre':
    schemas = schemas_for_pre
    schemas_dict = {
        'SpectrumPreSched': 'Spectrum-P$_\mathit{Sched}$',
        'Spectrum': 'Spectrum-P',
        'SpectrumNoPartial': 'Spectrum-C',
    }

threads = args.threads

savepath = f'skew-abort-{workload}-{threads}.pdf'

#################### 数据准备 ####################
recs = pd.read_csv(f'./data/{workload}_{threads}.csv')
inner_schemas = recs['protocol'].unique()
print(inner_schemas)

#################### 画图 ####################
p = MyPlot(1, 1)
ax: plt.Axes = p.axes
ax.grid(axis=p.grid, linewidth=p.border_width)
p.init(ax)

for idx, (schema, color) in enumerate(schemas):
    records = recs[recs['protocol'] == schema]
    # print(schema)
    # print(records[Y] / records['commit'])
    p.bar(
        ax,
        xdata=[_ + (idx-2.5) * 0.14 for _ in range(records[X].size)],
        ydata=records[Y] / records['commit'],
        color=color, legend_label=schemas_dict[schema] if schemas_dict.get(schema) else schema,
        width=0.14,
        hatch=['xx', '//', '\\\\', '||', '--', '++', ][idx % 6],
    )

# print(recs[recs['protocol'] == 'SpectrumPreSched']['commit'].reset_index(drop=True) / recs[recs['protocol'] == 'Spectrum']['commit'].reset_index(drop=True))
# print(recs[recs['protocol'] == 'SpectrumPreSched']['commit'].reset_index(drop=True) / recs[recs['protocol'] == 'SpectrumNoPartial']['commit'].reset_index(drop=True))

print(type(recs['zipf'].unique()), recs['zipf'].unique())
# 设置X轴标签
ax.set_xticks(range(len(recs['zipf'].unique())), [str(t) for t in recs['zipf'].unique()])

# 自适应Y轴变化
p.format_yticks(ax, suffix=None, step=7, max_y_data=29)
# ax.set_ylim(None, p.max_y_data * 1.15)       # 折线图的Y轴上限设置为数据最大值的1.15倍

# 设置label
p.set_labels(ax, XLABEL, YLABEL)

# 设置图例
handles, labels = ax.get_legend_handles_labels()
label_order = ['Spectrum-P$_\mathit{Sched}$', 'Sparkle', 'Spectrum-P', 'AriaFB', 'Spectrum-C', 'Calvin']
# label_order = ['Spectrum-P$_\mathit{Sched}$', 'Spectrum-P', 'Spectrum-C', 'AriaFB', 'Aria', 'Sparkle', 'Calvin']
handles = [handles[i] for i in [labels.index(label) for label in label_order]]
labels = label_order
p.legend(ax, loc="upper center", ncol=3, anchor=(0.5, 1.20), columnspacing=0.5,
         handles=handles, labels=labels, kwargs={ 'size': 10 })

# 保存
p.save(savepath)