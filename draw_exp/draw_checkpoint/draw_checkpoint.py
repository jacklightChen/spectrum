##### run by cmd #####
HELP = 'python draw_memory.py -w workload -c contention -t threads'
##### run by cmd #####

X = "zipf"
Y = "cost"
XLABEL = "Partial Rollback Impl."
YLABEL = "Time (us) / Checkpoint"

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
# parser = argparse.ArgumentParser(HELP)
# parser.add_argument("-w", "--workload", type=str, required=True, help="workload: smallbank or ycsb")
# parser.add_argument("-c", "--contention", type=str, required=True, help="contention: uniform or skewed")
# parser.add_argument("-t", "--threads", type=int, required=True, help="threads")
# args = parser.parse_args()
# assert args.workload in ['smallbank', 'ycsb', 'tpcc']
# workload = args.workload
# if workload == 'tpcc':
#     XLABEL = "Number of Items"

# assert args.contention in ['uniform', 'skewed']
# contention = args.contention

# threads = args.threads

savepath = f'checkpoint-cost.pdf'

#################### 数据准备 ####################
# recs = pd.read_csv(f'./data/{workload}_{contention}_{threads}.csv')
recs = pd.DataFrame({
    'protocol': ['SpectrumCOPYONWRITE', 'SpectrumSTRAWMAN'],
    'cost': [0.626, 4.107],
})
inner_schemas = recs['protocol'].unique()
print(inner_schemas)

#################### 画图 ####################
p = MyPlot(1, 1)
ax: plt.Axes = p.axes
ax.grid(axis=p.grid, linewidth=p.border_width)
p.init(ax)

for idx, (schema, color) in enumerate(schemas):
    records = recs[recs['protocol'] == schema]
    print(records[Y])
    ax.barh(
        [_ + (idx-0.5) * 0.4 for _ in range(2)],
        records[Y],
        color=color, label=schemas_dict[schema],
        height=0.2,
        hatch=['//', '\\\\', ][idx],
        ec='black', ls='-', lw=1
    )

ax.set_ylim(-0.5, 0.5)

ax.set_xlim(0, 5)

# 设置Y轴标签
ax.set_yticks([-0.2, 0.2])
# ax.set_yticklabels([schemas_dict[schema] for schema, _ in schemas])
ax.set_yticklabels([])
ax.tick_params(axis='y', width=0)

# ax.set_xticks(range(len(recs['zipf'].unique())), [str(t) for t in recs['zipf'].unique()])

# 自适应Y轴变化
# p.format_yticks(ax)
# ax.set_ylim(None, p.max_y_data * 1.15)       # 折线图的Y轴上限设置为数据最大值的1.15倍

# 设置label
p.set_labels(ax, YLABEL, XLABEL)

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