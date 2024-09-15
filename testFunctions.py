import FetchProductListSmallLIB as fpl
import formatData as fd
import polars as pl
from rich import print
import logging


logging.basicConfig(level=logging.INFO)

path_to_config_file = "../Courses/PythonExercises/config/config.json"

dataFramePL = fpl.obtain_products_list(path_to_config_file,maxStockRequests=200)
print(dataFramePL)
# print(dataFramePL) This is the df with the names
dfR4P = fd.obtain_requests_for_products(dataFramePL)
# print(dfR4P) # This is the df with the names, vwdId and the coding for requests.
# it will be used to create the list of requests
# print(dfR4P.columns[3:])

list_of_charts = fpl.obtain_products_data_1by1(
    path_to_configCredential = path_to_config_file,
    dfR4P = dfR4P,
    type_of_request = dfR4P.columns[3:] # one day there will be a possibilty to select
    )
# print(list_of_charts)
dfCofP = fd.obtain_formatted_df_from_list_of_charts(list_of_charts)



dfCofP.write_csv("output.csv")
# print(chart)
# Drop columns using list comprehension
    #df = dfR4P.select([col for col in dfR4P.columns if col not in vwdId_name])
    # Get the column as a list
 
    #col_list = dfR4P[:,dfR4P.get_column_index(concat_str_requests)]
    #print(col_list)  # Output: [1, 2, 3]
