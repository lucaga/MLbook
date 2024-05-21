import polars as pl
import logging
    
# the following function might not be at home here
# this is only a beutification function for the table, nothing to do with fetching
# leave it here for now (2024 05 21)

def obtain_requests_for_products(
        dfR4P, 
        concat_str_requests=["issueid:","ohlc:","price:"]
        ):
    #concat_str_requests=["issueid:","ohlc:","price:"]
    search_string = "vwdId"
    # try:
    #     # Get the index using index()
    #     indexvwdId = dfR4P.get_column_index(search_string)
    #     #indexvwdId = vwdId_name.index(search_string)
    # except ValueError:
    #     print(f"'{search_string}' not found in the dataframe.")
    
    for concat_str_target in concat_str_requests:
    
        if concat_str_target != "issueid:" :
            concat_str_target = concat_str_target + concat_str_requests[0]
        dfR4P = dfR4P.with_columns(
                (
                    pl.struct(
                        search_string
                    ).map_batches(
                        lambda x:concat_str_target+x.struct.field(search_string)
                    )
                ).alias(
                    concat_str_target+search_string
                )
            )
    logging.info("request created")
    return dfR4P
    