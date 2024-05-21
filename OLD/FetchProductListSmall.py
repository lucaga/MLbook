import connection_Handler as ch
import polars as pl

from degiro_connector.trading.models.product_search import StocksRequest

def obtain_products_list(path_to_configCredential = "", exchange_id_desired = 200, bare_min = True):
    trading_api=ch.trading_connect_DeGiro(path_to_configCredential)#"../Courses/PythonExercises/config/config.json")
    
    #FETCHPRODUCTS
    request_stock=StocksRequest(
            exchange_id=exchange_id_desired,#EuronextAM
            offset=0,
            limit=5000,
            require_total=False,
            sort_columns="name",
            sort_types="asc",
        )
    product_search=trading_api.product_search(product_request=request_stock,raw=False)

    vwdId_name=["name","symbol","vwdId"]

    if bare_min != True:
        products_df=pl.DataFrame(product_search.products)
        return products_df
    else:
            product_df_vwdID_noNulls=products_df.select(pl.col(vwdId_name)).drop_nulls()
            return product_df_vwdID_noNulls
    
# as;lkfja;lskdjf        
concat_str_requests=["issueid:","ohlc:","price:"]
search_string = "vwdId"

try:
    # Get the index using index()
    indexvwdId = vwdId_name.index(search_string)
except ValueError:
    print(f"'{search_string}' not found in the list.")

for concat_str_target in concat_str_requests:
    if concat_str_target != "issueid:" :
        concat_str_target = concat_str_target + concat_str_requests[0]
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

# Drop columns using list comprehension
df = product_df_vwdID_noNulls.select([col for col in product_df_vwdID_noNulls.columns if col not in vwdId_name])

# Get the column as a list
col_list = df[:,1]

print(col_list)  # Output: [1, 2, 3]
