import json
#import logging
#import hvplot.pandas #noqa
import polars as pl
#import pandas as pd

#from pydantic  import  BaseModel
#from rich  import  print
#from rich.console  import  Console
#console=Console(record=True)

from degiro_connector.trading.api import API as TradingAPI
from degiro_connector.trading.models.credentials import build_credentials
#from degiro_connector.trading.models.product_search import LookupRequest
from degiro_connector.trading.models.product_search import StocksRequest
#from degiro_connector.trading.models.product_search import OptionsRequest
#from degiro_connector.trading.models.product_search import LeveragedsRequest
#from degiro_connector.trading.models.product_search import FuturesRequest
#from degiro_connector.trading.models.product_search import FundsRequest
#from degiro_connector.trading.models.product_search import ETFsRequest
#from degiro_connector.trading.models.product_search import BondsRequest

with open("../Courses/PythonExercises/config/config.json") as config_file:
    config_dict=json.load(config_file)

credentials=build_credentials(override=config_dict)#credentialscanalsobebuiltfromthedictionarycontainingthedetailsfromtheconfigfile

trading_api=TradingAPI(credentials=credentials)
trading_api.connect()

#FETCHPRODUCTS
request_stock=StocksRequest(
    exchange_id=200,#EuronextAM
    offset=0,
    limit=5000,
    require_total=False,
    sort_columns="name",
    sort_types="asc",
)
product_search=trading_api.product_search(product_request=request_stock,raw=False)

products_df=pl.DataFrame(product_search.products)
#print(products_df)

#products_df.head(1)

vwdId_name=["name","symbol","vwdId"]

product_df_vwdID_noNulls=products_df.select(pl.col(vwdId_name)).drop_nulls()
#print(product_df_vwdID_noNulls)


concat_str_requests=["issueid:","ohlc:","price:"]

search_string = "vwdId"

try:
  # Get the index using index()
  indexvwdId = vwdId_name.index(search_string)
#   print(f"The index of '{search_string}' is: {indexvwdId}")
except ValueError:
  print(f"'{search_string}' not found in the list.")

# remember to add the "issueid:" before other things you ask

for concat_str_target in concat_str_requests:
    if concat_str_target != "issueid:" :
       concat_str_target = concat_str_target + concat_str_requests[0]
       # print(concat_str_target)
    product_df_vwdID_noNulls = product_df_vwdID_noNulls.with_columns(
            (
                pl.struct(
                    vwdId_name[indexvwdId]
                ).map_batches(
                    lambda x:concat_str_target+x.struct.field(vwdId_name[indexvwdId])
                )
            ).alias(
                concat_str_target+vwdId_name[indexvwdId]
            )
        )
    
#print(product_df_vwdID_noNulls)

# Drop columns using list comprehension
df = product_df_vwdID_noNulls.select([col for col in product_df_vwdID_noNulls.columns if col not in vwdId_name])

#print(df)



# Get the column as a list
col_list = df[:,1]

print(col_list)  # Output: [1, 2, 3]
