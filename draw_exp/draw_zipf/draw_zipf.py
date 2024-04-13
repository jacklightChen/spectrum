##### run by cmd #####
HELP = 'python draw_zipf.py -f zipf'
##### run by cmd #####

X = "keys"
Y = "count"
XLABEL = "Keys"
YLABEL = "PMF"

import pandas as pd
import argparse
import sys

sys.path.extend(['.', '..', '../..'])
import matplotlib.pyplot as plt
from plot.plot import MyPlot

#################### 参数解析 ####################
parser = argparse.ArgumentParser(HELP)
parser.add_argument("-fs", "--file_list", type=str, nargs='+', required=True, help="file list path")
args = parser.parse_args()
files = args.file_list
print(files)

savepath = 'zipf.pdf'

#################### 数据准备 ####################
dfs = {}
for file in files:
    file: str = file
    dic = {i: 0 for i in range(1000000)}
    with open(file, 'r') as f:
        lines = f.readlines()
        for line in lines:
            dic[int(line)] += 1
    recs = pd.DataFrame(dic.items(), columns=['keys', 'count'])
    dfs[file.split('\\')[-1]] = recs

d: pd.DataFrame = dfs['1.1']
k_d = 2
k_u = 2
print(f'sched key between {k_d} and {k_u}')
d = d[d['keys'] >= k_d]
d = d[d['keys'] <= k_u]
print('占比:', d['count'].sum() / 1000000)

#################### 画图 ####################
p = MyPlot(1, 1)
ax: plt.Axes = p.axes
ax.grid(axis=p.grid, linewidth=p.border_width)
p.init(ax)

for idx, zipf in enumerate(dfs):
    records = dfs[zipf]
    # records = records[records['keys'] <= 1000]
    records = records[records['keys'] >= 1]
    print(records.shape)
    p.plot(
        ax,
        xdata=records[X],
        ydata=records[Y] / 1000000,
        color=None, legend_label=r'$\mathit{Uniform}$' if str(zipf) == '0' else r"$\mathit{Zipf}_\mathit{" + zipf + r"}$",
        marker='None'
        # marker=['v', 's', 'o'][idx]
    )

# 设置X轴标签
# ax.set_xticks([int(t) for t in recs['threads'].unique()])
ax.set_xscale('log')
# ax.set_yscale('log')

# 自适应Y轴变化
# p.format_yticks(ax, suffix='K')
# ax.set_ylim(None, p.max_y_data * 1.15)       # 折线图的Y轴上限设置为数据最大值的1.15倍

# 设置label
p.set_labels(ax, XLABEL, YLABEL)
ax.set_xlabel(XLABEL, style='italic')

# 设置图例
p.legend(ax, loc="upper center", ncol=4, anchor=(0.5, 1.15), columnspacing=0.5, kwargs={ 'style' : 'italic'})

# 保存
# p.save(savepath)