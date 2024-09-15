import polars as pl
import logging
from rich import print
from degiro_connector.quotecast.tools.chart_fetcher import ChartFetcher, SeriesFormatter 


#this function should expect a chart with one or more items per request.
#It will expect a flag that tell the function that the info in the chart refer to the same vwdId
#needs rework

def results_available_in_chart(amount_of_series) -> bool:
    if  amount_of_series == 0 :
        logging.error("Request lead to empty results")
        return False
    else:
        logging.info("Data available, starting to parse")
        return True

def all_ids_share_same_suffix_vwdId(single_chart) -> bool:
    all_ids_share_suffix_vwdId = True
    suffix_vwdId = None

    for series in single_chart.series:
        if not hasattr(series,"id"):
            all_ids_share_suffix_vwdId = False
            logging.error("Some results of the request are not identifiable")
            break
            
        id_parts = series.id.split(":")
            
        if len(id_parts) > 1: 
            current_suffix_vwdId = id_parts[-1]
        else:
            logging.warning("no prefix found for id, strange")
            current_suffix_vwdId = series.id

        if suffix_vwdId is None or current_suffix_vwdId == suffix_vwdId:
            suffix_vwdId = current_suffix_vwdId
        else:
            all_ids_share_suffix_vwdId = False
            break
    if all_ids_share_suffix_vwdId:
        logging.info("All series have 'id' keys with the same prefix: {}".format(suffix_vwdId))
        return True
    else:
        logging.info("Series 'id' keys do not share a common prefix")
        return False

# def remove_sublist(large_list, sublist) -> list:
#   return [item for item in large_list if item not in sublist]

def check_dataframes(df1, df2) -> bool:
    """
    Checks if two DataFrames have the same number of columns and matching names.

    Args:
        df1 (pl.DataFrame): The first DataFrame.
        df2 (pl.DataFrame): The second DataFrame.

    Returns:
        bool: True if DataFrames have the same columns and names, False otherwise.
    """
    first_check = df1.shape[1] == df2.shape[1]
    if first_check : logging.info("same amount of columns")

    second_check = df1.columns == df2.columns
    if second_check : logging.info("same names of columns")
    else: 
        print(df1.columns)
        print(df2.columns)
        
    return first_check and second_check

def obtain_formatted_df_from_list_of_charts(
        list_of_charts : list,
        what_to_return = "ohlc"
        ) -> pl.DataFrame | None :
    """ THis function will check if charts contained in a list have series data 
    and return a polars dataframe """
    
    dfStack_All_Product_Details = pl.DataFrame()
    
    counterList=len(list_of_charts)

    list_of_dfStack_Single_Product_Details = []
    for single_chart in list_of_charts:
        # the code doesn't know which series (and which order?) are in the series of this chart. 
            
        if results_available_in_chart(len(single_chart.series)):
            pass
        else : return None

        # print(single_chart)
            
            # Code assumes that all series in a chart come from the same product (vwdId)
            # This means that at the end, the name and vwdId column will be filled with the correct values for each chart
           
        if all_ids_share_same_suffix_vwdId(single_chart):
             pass
        else : return None

        dfStack_Single_Product_Details = pl.DataFrame()

        
    
        for series in single_chart.series:
 
            # From a chart, code has to create a 
            # complete (all the data which is received)
            # single column for value (use of descriptor columns)
            # well formatted (timing is good)

            # this means that for each series there will be a check of what it is

            if series.type == "object": 
                # from here I can get info about the product, to make the df more readable
                keep_keys = [
                        'issueId',#: 11797,
                        'companyId',#: 175,
                        'name',#: 'AALBERTS NV',
                        'identifier',#: 'issueid:11797',
                        'isin',#: 'NL0000852564',
                        'alfa',#: 'AALB',
                        'market',#: 'XAMS',
                        'currency',#: 'EUR',
                        'type',#: 'AAN',
                        'quality'#: 'REALTIME',
                    ]
                # Create a new dictionary with only desired keys
                data_for_this_product = {key: series.data[key] for key in keep_keys}
                # print(data_for_this_product)
                colDF=pl.from_dict(data_for_this_product) # it is possible to declare what type is what
                # print(colDF)
            else:
                rowDF = SeriesFormatter.format_series(series=series)
                if len(rowDF) == 0:
                    print(series)
                    break
                if series.type == "time": 
                    # this can be for both the price and the volume
                    rowDF = rowDF.rename({rowDF.columns[1]:"value"})
                    # pl.select([rowDF.columns[0],[rowDF.columns[1].rename("value")]])
                    rowDF = rowDF.with_columns(type1=pl.lit(series.type))  # 'lit' creates a Series with a constant value
                    rowDF = rowDF.with_columns(type2=pl.lit(series.id.split(":")[0]))  # 'lit' creates a Series with a constant value

                    # print(rowDF)
                elif series.type == "ohlc":
                    # this is what
                    value_cols = ["open", "high", "low", "close"]
                    rowDF = rowDF.melt(id_vars="timestamp", value_vars=value_cols, variable_name="type2",value_name="value")
                    # order matters, first melt then "lit"
                    rowDF = rowDF.with_columns(type1=pl.lit(series.type))  # 'lit' creates a Series with a constant value
                    # print(rowDF)
                else:
                    logging.warning("Unknown type of Series {}, will not be handled".format(series.type))
                    pass
                
                if dfStack_Single_Product_Details.is_empty() :
                    dfStack_Single_Product_Details = rowDF
                else:
                    desired_order = rowDF.columns
                    dfStack_Single_Product_Details=dfStack_Single_Product_Details.select(desired_order)
                    check_dataframes(rowDF,dfStack_Single_Product_Details)
                    print("Before")
                   
                    dfStack_Single_Product_Details = pl.concat([rowDF,dfStack_Single_Product_Details],how="vertical")
                    print("After")
                    print(dfStack_Single_Product_Details)
                    pass
                pass
            pass
        
        n_repeats = len(dfStack_Single_Product_Details) - len(colDF) + 1 

        df_list = []

        # Loop and concatenate copies of df2
        for _ in range(n_repeats):
            if len(colDF) == 0 : 
                break
            df_list.append(colDF)
            pass
        if len(df_list) == 0 :
            break
        df_concat = pl.concat(df_list)

        dfStack_Single_Product_Details = pl.concat([dfStack_Single_Product_Details, df_concat],how="horizontal")

        # print(dfStack_Single_Product_Details)

        list_of_dfStack_Single_Product_Details.append(dfStack_Single_Product_Details)
        pass
    dfStack_All_Product_Details = pl.concat(list_of_dfStack_Single_Product_Details)


    return dfStack_All_Product_Details

















                    
    #         if series.type in ["time"]: #"time"means price
    #             logging.info("this part of the data is an " + series.type + ", addressing the right Dataframe")
                
    #             if dfStackingPrice.is_empty() :
    #                 dfStackingPrice = rowDF
    #             else:
    #                 dfStackingPrice = pl.concat([rowDF,dfStackingPrice],how="vertical")
    #             reporting_size = dfStackingPrice.shape
    #         elif series.type in ["ohlc"]:
    #             logging.info("this part of the data is an " + series.type + ", addressing the right Dataframe")
    #             if dfStackingOLHC.is_empty() :
    #                 dfStackingOLHC = rowDF
    #             else:
    #                 dfStackingOLHC = pl.concat([rowDF,dfStackingOLHC],how="vertical")
    #             reporting_size = dfStackingOLHC.shape
    #     counterList = counterList-1

    #     logging.info("One chart done, {} to go".format(counterList))
    #     logging.info("last request was a {}".format(series.type))
    #     logging.info("the size of the dataframe for these requests is now, {} to go".format(reporting_size))


    # if what_to_return == "ohlc" :
    #     return dfStackingOLHC
    # elif what_to_return == ["time"]:
    #     return dfStackingPrice
    # else:  
    #     logging.warning("Not clear what was asked, there isn't a <{}> choice here",what_to_return)
    #     return None


def obtain_requests_for_products(
        dfR4P, 
        concat_str_requests=["issueid:","ohlc:","price:","volume:"],
        search_string = "vwdId"
        ) -> pl.DataFrame:
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
    