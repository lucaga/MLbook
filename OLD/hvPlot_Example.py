# this simple example will not show output in the terminal within VisualStudio Code. 
# it has to run in Interactive mode
import hvplot.pandas
from bokeh.sampledata.autompg import autompg_clean as df

import matplotlib.pyplot as plt

table = df.groupby(['origin', 'mfr'])['mpg'].mean().sort_values().tail(5)
hvplotObj = table.hvplot.barh('mfr', 'mpg', by='origin', stacked=True)
hvplotObj
# hvplot.show(hvplotObj)


# plt.plot([1,2,3], [1,2,3])

# plt.show()#block=True) 