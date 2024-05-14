import hvplot.pandas
from bokeh.sampledata.autompg import autompg_clean as df

table = df.groupby(['origin', 'mfr'])['mpg'].mean().sort_values().tail(5)
table.hvplot.barh('mfr', 'mpg', by='origin', stacked=True)