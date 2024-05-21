import connection_Handler as ch
import polars as pl
import json
import logging


from degiro_connector.trading.models.product_search import StocksRequest
from degiro_connector.quotecast.models.chart import ChartRequest, Interval


def obtain_products_list(path_to_configCredential = "", exchange_id_desired = 200, bare_min = True):
    trading_api=ch.trading_connect_DeGiro(path_to_configCredential)
    
    #FETCHPRODUCTS
    request_stock=StocksRequest(
            exchange_id=exchange_id_desired,#EuronextAM
            offset=0,
            limit=5000,
            require_total=False,
            sort_columns="name",
            sort_types="asc",
        )
    
    try:
        product_search=trading_api.product_search(product_request=request_stock,raw=False)
    except Exception as e: 
        print(f"While using the API to search for products \n"
              +"An unexpected error occurred: {e}")  

    
    if product_search == None :
        raise ValueError("product search unsuccessful\n"+"Maybe bad request declaration")
    
    try: 
        products_df=pl.DataFrame(product_search.products)
        # logging.info(products_df.glimpse())
    except Exception as e: 
        print(f"While placing any found products in the dataframe \n"
            +"An unexpected error occurred: {e}")  
        
   

    if bare_min != True:
        return products_df
    else:
        vwdId_name=["name","symbol","vwdId"]
        product_df_vwdID_noNulls=products_df.select(pl.col(vwdId_name)).drop_nulls()
        return product_df_vwdID_noNulls

def obtain_products_data(
        path_to_configCredential = "",
        dfR4P = None,
        bare_min = True
        ):
    
      
    logging.info("ready to get a series of strings")
    series_request = dfR4P["ohlc:issueid:vwdId"].to_list()
    print(series_request)
    n_element = 6

    chart_request = ChartRequest(
        culture="fr-FR",
        # override={
        #     "resolution": "P1D",
        #     "period": "P1W",
        # },
        period=Interval.P1D,
        requestid="1",
        resolution=Interval.PT60M,
        series=series_request[:n_element],
        tz="Europe/Paris",
    )
    chart_request_dict = chart_request.model_dump()
    json_data = json.dumps(chart_request_dict)
    estimated_legth_chart_request_dict = len(json_data)
    print("The size of the chart_request:  {}".format(estimated_legth_chart_request_dict))
    chart_fetcher = ch.quotecast_connect_DeGiro(path_to_configCredential)
    chart = chart_fetcher.get_chart(
        chart_request=chart_request,
        raw=False,
    )
    return chart

    # if chart:
    #     for series in chart.series:
    #         if series.times is None or series.type not in ["time", "ohlc"]:
    #             print("this part of the data is an ",series.type,", sorry cannot print")
    #         else :
    #             df = SeriesFormatter.format(series=series)
    #             print(df)
    #             print(type(df))
    #             if series.id.startswith("ohlc"):
    #                 dfPd = df.to_pandas()
    #                 print(dfPd)

    # pass