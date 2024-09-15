import connection_Handler as ch
import polars as pl
import json
import logging


from degiro_connector.trading.models.product_search import StocksRequest
from degiro_connector.quotecast.models.chart import ChartRequest, Interval

def obtain_products_list(path_to_configCredential = "", maxStockRequests = 5,exchange_id_desired = 200, bare_min = True):
    """
    exchange ID 200 is EuronextAM
    """
    trading_api=ch.trading_connect_DeGiro(path_to_configCredential)

    #FETCHPRODUCTS
    request_stock=StocksRequest(
            exchange_id=exchange_id_desired,#EuronextAM
            offset=0,
            limit=maxStockRequests, 
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
    # print(series_request)
    n_element = 6

    chart_request = ChartRequest(
        culture="fr-FR",
        period=Interval.P1D,
        requestid="1",
        resolution=Interval.PT60M,
        series=series_request[:n_element],
        tz="Europe/Paris",
    )
    
    estimated_length_chart_request_dict(chart_request)

    chart_fetcher = ch.quotecast_connect_DeGiro(path_to_configCredential)
    chart = chart_fetcher.get_chart(
        chart_request=chart_request,
        raw=False,
    )
    return chart

def obtain_products_data_1by1(
        path_to_configCredential = "",
        dfR4P = pl.DataFrame,
        type_of_request = ["ohlc:issueid:vwdId","issueid:vwdId"],
        ) -> list:
    
    logging.info("ready to get a series of strings")
    selectedRequests = dfR4P.select(type_of_request)
    # in the next list comprehension, 
    # the variable "col" is extracted by enumerate, but not used. 
    # And it is ok.
    series_request = [[row[i] for i, col in enumerate(type_of_request)] for row in selectedRequests.iter_rows()]
    
    chart_fetcher = ch.quotecast_connect_DeGiro(path_to_configCredential)

    list_of_charts = []
    counter_of_Requests = 0

    for product_chart_request in series_request:
        logging.info(product_chart_request)
        counter_of_Requests = counter_of_Requests+1
        chart_request = ChartRequest(
            culture="nl-NL",
            period=Interval.P1D,
            resolution=Interval.PT30M,
            requestid=str(counter_of_Requests), 
            series=product_chart_request,
            tz="Europe/Amsterdam"
            )
        estimated_length_chart_request_dict(chart_request)

        chart = chart_fetcher.get_chart(
            chart_request=chart_request,
            raw=False,
            )
        list_of_charts.append(chart)
    return list_of_charts
    

def estimated_length_chart_request_dict(chart_request):
    chart_request_dict = chart_request.model_dump()
    json_data = json.dumps(chart_request_dict)
    estimated_legth_chart_request_dict_value = len(json_data)
    logging.info("The size of the chart_request:  {}".format(estimated_legth_chart_request_dict_value))
    return estimated_legth_chart_request_dict_value
