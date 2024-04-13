##### run by cmd #####
HELP = 'python draw_memory.py -w workload -c contention -t threads'
##### run by cmd #####

X = "zipf"
Y = "memory"
XLABEL = "Partial Rollback Impl."
YLABEL = "Memory (Bytes) / Commit"

import pandas as pd
import argparse
import sys

sys.path.extend(['.', '..', '../..'])
import matplotlib.pyplot as plt
from plot.plot import MyPlot
from Schemas import schemas
schemas = [
    
    ('SpectrumSTRAWMAN'     ,   '#5072A7'),
    ('SpectrumCOPYONWRITE'  ,   '#D95353'),
]

schemas_dict = {
    'SpectrumSTRAWMAN'      :   'EVMStraw',
    'SpectrumCOPYONWRITE'   :   'EVMCoW'
}

#################### 参数解析 ####################
parser = argparse.ArgumentParser(HELP)
parser.add_argument("-w", "--workload", type=str, required=True, help="workload: smallbank or ycsb")
parser.add_argument("-c", "--contention", type=str, required=True, help="contention: uniform or skewed")
parser.add_argument("-t", "--threads", type=int, required=True, help="threads")
args = parser.parse_args()
assert args.workload in ['smallbank', 'ycsb', 'tpcc']
workload = args.workload
if workload == 'tpcc':
    XLABEL = "Number of Items"

assert args.contention in ['uniform', 'skewed']
contention = args.contention

threads = args.threads

savepath = f'memory-{workload}-{contention}-{threads}.pdf'

#################### 数据准备 ####################
recs = pd.read_csv(f'./data/{workload}_{contention}_{threads}.csv')
inner_schemas = recs['protocol'].unique()
print(inner_schemas)

#################### 画图 ####################
p = MyPlot(1, 1)
ax: plt.Axes = p.axes
ax.grid(axis=p.grid, linewidth=p.border_width)
p.init(ax)

for idx, (schema, color) in enumerate(schemas):
    records = recs[recs['protocol'] == schema]
    print(records[Y] / records['commit'])
    p.bar(
        ax,
        xdata=[_ + (idx-0.5) * 0.4 for _ in range(records[X].size)],
        ydata=records[Y] / records['commit'],
        color=color, legend_label=schemas_dict[schema],
        width=0.2,
        hatch=['//', '\\\\', ][idx],
    )

ax.set_xlim(-0.5, 0.5)

# 设置X轴标签
ax.set_xticks([-0.2, 0.2], [schemas_dict[schema] for schema, _ in schemas])
# ax.set_xticks(range(len(recs['zipf'].unique())), [str(t) for t in recs['zipf'].unique()])

# 自适应Y轴变化
p.format_yticks(ax, suffix='K', step=180 if workload == 'smallbank' else None)
# ax.set_ylim(None, p.max_y_data * 1.15)       # 折线图的Y轴上限设置为数据最大值的1.15倍

# 设置label
p.set_labels(ax, XLABEL, YLABEL)

# 设置图例
p.legend(ax, loc="upper center", ncol=3, anchor=(0.5, 1.15))
# handles, labels = ax.get_legend_handles_labels()
# label_order = ['EVMCoW', 'EVMStraw',]
# handles = [handles[i] for i in [labels.index(label) for label in label_order]]
# labels = label_order
# p.legend(ax, loc="upper center", ncol=3, anchor=(0.5, 1.15), # columnspacing=0.5,
#          handles=handles, labels=labels)

# 保存
p.save(savepath)