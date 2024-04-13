# README

## Note
The file does not contain zipf files, nor the Font files in plot. You can replace the font type by modifying it in plot.py.

## Architecture

Each draw_xxx folder contains a py file for drawing. The data for drawing comes from the data in the draw_xxx folder, which are csv files output by the script/python script in the project. They have been renamed, where -w represents workload (including some like pre/compare), -c represents contention (also includes some like pre/compare), -t represents threads.

plot.py is the MyPlot class, which initializes many variables, so you don't need to rewrite global variables in each function. It encapsulates a new plot function, which is slightly more convenient than ax.plot(). You can also use the p.axes member variable to draw directly according to the plt.Axes class.

## Running py files

The file now contains -w -c -t three parameters, two or three or none of them. After running the corresponding python file, you can get a prompt:
