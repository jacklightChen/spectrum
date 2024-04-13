# README

# README

## 注意
文件中不包含zipf的文件，以及plot中Font的字体文件，字体可以自行更改plot.py中的字体类型进行替换

## 架构

以每个draw_xxx文件夹下的py为画图，画图时数据来源于draw_xxx文件夹下的data，里面是项目中script/python脚本输出的csv文件，重命名了一下，-w 表示workload（也包括一些诸如pre/compare的）， -c 表示contention（也包括一些诸如pre/compare的） -t 表示threads

plot.py为MyPlot类，初始化了很多变量，就不需要在每个函数里重新写一遍全局变量，其中封装了新的plot函数，稍微比ax.plot()方便，也可以使用p.axes成员变量按照plt.Axes类的方式直接画图

## py文件运行

文件现包含-w -c -t三个参数中其中2个或3个或0个，运行对应的python文件后能获取提示：

```shell
python draw_xxx_xxx.py -w workload -c contention -t threads
```

运行命令即可生成与filename同名的pdf文件，需注意每个图的label是写死在代码中的，参考每个py文件的最上方X, XLABEL等全局变量

## 架构

以每个draw_xxx文件夹下的py为画图，画图时数据来源于draw_xxx文件夹下的data，里面是项目中script/python脚本输出的csv文件，重命名了一下，-w 表示workload（也包括一些诸如pre/compare的）， -c 表示contention（也包括一些诸如pre/compare的） -t 表示threads

plot.py为MyPlot类，初始化了很多变量，就不需要在每个函数里重新写一遍全局变量，其中封装了新的plot函数，稍微比ax.plot()方便，也可以使用p.axes成员变量按照plt.Axes类的方式直接画图

## py文件运行

文件现包含-w -c -t三个参数中其中2个或3个或0个，运行对应的python文件后能获取提示：

```shell
python draw_xxx_xxx.py -w workload -c contention -t threads
```

运行命令即可生成与filename同名的pdf文件，需注意每个图的label是写死在代码中的，参考每个py文件的最上方X, XLABEL等全局变量