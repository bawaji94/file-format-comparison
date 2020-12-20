import pandas as pd
import matplotlib.pyplot as plt

def anotate_value(ax, postfix=''):
    for p in ax.patches:
        ax.annotate(str(round(p.get_height(), 2))+postfix, 
                    (p.get_x() + p.get_width() / 2., p.get_height()), 
                    ha = 'center', va = 'center', fontsize=8,
                    xytext = (0, 9), 
                    textcoords = 'offset points')

df = pd.read_csv('data_all_int_file_format_benchmark.csv')
df = df[(df.fileFormat!='csv') | (df.library != 'pandas')]

ax_read_write = df.plot.bar(x='fileFormat', y=['write_time', 'read_time'], rot=30, xlabel='')
anotate_value(ax_read_write, postfix='s')

ax_storage_size = df.plot.bar(x='fileFormat', y='sizeOnDisk', rot=30, xlabel='', legend=False)
anotate_value(ax_storage_size, postfix='MB')
ax_storage_size.set_ylabel('Size of file on disk in MB', fontsize=12)
plt.show()