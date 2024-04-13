##### run by cmd #####
HELP = 'python draw_block_latency.py -w workload -c contention -t threads'
##### run by cmd #####

Y = "commit"
XLABEL = "Execution Schemes"
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

savepath = f'block-latency-{workload}-{contention}.pdf'

#################### 数据准备 ####################
recs = pd.read_csv(f'./data/{workload}_{contention}.csv')
assert args.threads in recs['threads'].unique()
threads = args.threads
recs = recs[recs['threads'] == threads]
inner_schemas = recs['protocol'].unique()
print(inner_schemas)

#################### 画图 ####################
p = MyPlot(1, 1)
p.fig.clear()
gs = p.fig.add_gridspec(4, 4, hspace=0.4)
ax_bottom: plt.Axes = p.fig.add_subplot(gs[1:, :])
ax_top: plt.Axes = p.fig.add_subplot(gs[:1, :])
p.init(ax_bottom)
p.init(ax_top)
ax_bottom.grid(axis=p.grid, linewidth=p.border_width)
ax_bottom.set_axisbelow(True)
ax_top.grid(axis=p.grid, linewidth=p.border_width)
ax_top.set_axisbelow(True)
ax_bottom.spines.top.set_visible(False)
ax_top.spines.bottom.set_visible(False)
d = 0.5  # proportion of vertical to horizontal extent of the slanted line
kwargs = dict(marker=[(-1, -d), (1, d)], markersize=12,
            linestyle="none", color='k', mec='k', mew=p.tick_width, clip_on=False)
ax_top.plot([0, 1], [0, 0], transform=ax_top.transAxes, **kwargs)
ax_bottom.plot([0, 1], [1, 1], transform=ax_bottom.transAxes, **kwargs)

for idx, (schema, color) in enumerate(schemas):
    # if schema in ['Serial', 'Calvin']: continue
    records = recs[recs['protocol'] == schema]
    # print(schema, '\t', (1000000 / records[Y] * 100).max())
    p.bar(
        ax_bottom,
        xdata=[schema],
        ydata=1000000 / records[Y] * 100,
        color=color, legend_label=schema,
        width=0.7,
        hatch=['//', '\\\\', '||', '--', '++', 'xx'][idx % 6],
    )
    p.bar(
        ax_top,
        xdata=[schema],
        ydata=1000000 / records[Y] * 100,
        color=color, legend_label=schema,
        width=0.7,
        hatch=['//', '\\\\', '||', '--', '++', 'xx'][idx % 6],
    )

# 设置X轴标签
# ax.set_xlim(-0.6, 1.6)
ax_bottom.set_xticks(range(len(schemas)))
ax_bottom.set_xticklabels([schema if schema not in ['Spectrum', 'Sparkle'] else schema[:4] + '.' for (schema, _) in schemas])
# ax_bottom.set_xticklabels([schema[:4] if schema not in ['AriaFB'] else schema for (schema, _) in schemas])
ax_top.set_xticks([])

# 自适应Y轴变化
ax_bottom.set_ylim(0, (1000000 / recs[recs['protocol'] == 'Calvin'][Y].max()) * 100 * 1.25)
ax_top.set_ylim((1000000 / recs[Y]).max() * 100 * 0.9, (1000000 / recs[Y]).max() * 100 * 1.07)
p.format_yticks(ax_bottom, max_y_data=int((1000000 / recs[recs['protocol'] == 'Calvin'][Y].max()) * 100 * 1.2), step_num=4, suffix='K' if workload == 'tpcc' else None)
ax_top.set_yticks([int((1000000 / recs[Y]).max() * 100 * 0.92) // 100 * 100, int((1000000 / recs[Y]).max() * 100 * 1.07) // 100 * 100], ['38K', '44K'] if workload == 'tpcc' else None)

# 设置label
p.set_labels(ax_bottom, XLABEL, YLABEL)
ax_bottom.set_ylabel('Latency(us)', loc='top')

# 设置图例
p.legend(ax_bottom, loc="upper center", ncol=3, anchor=(0.5, 1.7))

# 保存
p.save(savepath)