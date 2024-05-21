import FetchProductListSmallLIB as fpl
import formatData as fd
import polars as pl
from rich import print
import logging


logging.basicConfig(level=logging.INFO)

path_to_config_file = "../Courses/PythonExercises/config/config.json"

dataFramePL = fpl.obtain_products_list(path_to_config_file)
# print(dataFramePL)
dfR4P = fd.obtain_requests_for_products(dataFramePL)
print(dfR4P)

chart = fpl.obtain_products_data(path_to_config_file,dfR4P)

# print(chart)
# Drop columns using list comprehension
    #df = dfR4P.select([col for col in dfR4P.columns if col not in vwdId_name])
    # Get the column as a list
 
    #col_list = dfR4P[:,dfR4P.get_column_index(concat_str_requests)]
    #print(col_list)  # Output: [1, 2, 3]
