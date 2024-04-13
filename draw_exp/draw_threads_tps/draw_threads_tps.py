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
import matplotlib.pyplot as plt
from plot.plot import MyPlot
from Schemas import schemas

#################### 参数解析 ####################
parser = argparse.ArgumentParser(HELP)
parser.add_argument("-w", "--workload", type=str, required=True, help="workload: smallbank or ycsb")
parser.add_argument("-c", "--contention", type=str, required=True, help="contention: uniform or skewed")
args = parser.parse_args()
assert args.workload in ['smallbank', 'ycsb', 'tpcc']
workload = args.workload
assert args.contention in ['uniform', 'skewed', '5orderlines', '10orderlines', '20orderlines', 'compare', 'pres']
contention = args.contention

savepath = f'threads-tps-{workload}-{contention}.pdf'

#################### 数据准备 ####################
recs = pd.read_csv(f'./data/{workload}_{contention}.csv')
inner_schemas = recs['protocol'].unique()
print(inner_schemas)

#################### 画图 ####################
p = MyPlot(1, 1, figsize=(12, 5))
ax: plt.Axes = p.axes
ax.grid(axis=p.grid, linewidth=p.border_width)
p.init(ax)

schemas_dict = None
if contention == 'compare':
    schemas = [
        ('SpectrumNoPartialBASIC'   ,   '#595959'),
        ('SpectrumSTRAWMAN'         ,   '#3A5FAD'),
        ('SpectrumCOPYONWRITE'      ,   '#D62728'),
        
        
    ]
    schemas_dict = {
        'SpectrumCOPYONWRITE'       :   'EVMCoW\n (Partial)',
        'SpectrumSTRAWMAN'          :   'EVMStraw\n  (Partial)',
        'SpectrumNoPartialBASIC'    :   '     EVM\n(Complete)',
    }
elif contention == 'pres':
    schemas = [
        
        ('SpectrumPreSched'             ,       '#9400D3'),  # 深紫色
        ('Spectrum'                     ,       '#d62728'),  # 红色
        ('SpectrumNoPartialPreSched'    ,       '#1f77b4'),  # 蓝色
        ('SpectrumNoPartial'            ,       '#595959'),  # 灰色
        ('SparklePreSched'              ,       '#2ca02c'),  # 绿色
        ('Sparkle'                      ,       '#8c564b'),  # 棕色
        ('SpectrumPreSched3'            ,       '#e377c2'),  # 粉色
        ('SpectrumPreSched2'            ,       '#7f7f7f'),  # 灰色
    ]
    schemas_dict = {
        'SpectrumPreSched'              : 'Spectrum-P$_\mathit{Sched}$',
        'Spectrum'                      : 'Spectrum-P',
        'SpectrumNoPartial'             : 'Spectrum-C',
        'Sparkle'                       : 'Sparkle-C',
        'SparklePreSched'               : 'Sparkle-P$_\mathit{Sched}$',
        'SpectrumNoPartialPreSched'     : 'Spectrum-C$_\mathit{Sched}$',
        'SpectrumPreSched3'             : 'Spectrum-P$_\mathit{Sched}$*',
        'SpectrumPreSched2'             : 'Spectrum-P$_\mathit{Sched}$**',
    }

for idx, (schema, color) in enumerate(schemas):
    records = recs[recs['protocol'] == schema]
    # print(records[Y])
    p.plot(
        ax,
        xdata=records[X],
        ydata=records[Y],
        color=color, legend_label=schemas_dict[schema] if schemas_dict else schema,
        # marker=['v', 's', 'o'][idx]
    )

print(type(recs['threads'].unique()), recs['threads'].unique())
# 设置X轴标签
ax.set_xticks([int(t) for t in recs['threads'].unique()])

# 自适应Y轴变化
step = None
if workload == 'smallbank' and contention == 'skewed':
    step = 140000
elif workload == 'tpcc' and contention == '10orderlines':
    step = 11000
p.format_yticks(ax, suffix='K', step=step)
# ax.set_ylim(None, p.max_y_data * 1.15)       # 折线图的Y轴上限设置为数据最大值的1.15倍

# 设置label
p.set_labels(ax, XLABEL, YLABEL, fontdict={ 'size': 22, 'weight': 'bold' } if contention == 'pres' else None)
# ax.set_ylabel(YLABEL, labelpad=-10)
# box1: plt.Bbox = ax.get_window_extent()
# box2: plt.Bbox = ax.get_tightbbox()

# 设置图例
p.legend(
    ax, 
    loc="upper center", 
    ncol=4, 
    anchor=(0.5, 1.18) if contention == 'compare' else (0.5, 1.2), 
    kwargs={ 'size': 10 } if contention == 'compare' else None,
    columnspacing=2
)

# 保存
p.save(savepath)