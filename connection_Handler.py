import json
import os
from datetime import datetime
import logging

# This is for copy paste purposes

# logging.info("This is a info message")
# logging.debug("This is a debug message")
# logging.warning("This is a warning message")
# logging.error("This is a error message")
# logging.critical("This is a critical message, no messing around")


from degiro_connector.trading.api import API as TradingAPI
from degiro_connector.trading.models.credentials import build_credentials

from degiro_connector.quotecast.tools.chart_fetcher import ChartFetcher

def init_connection(path_to_config_file):
    """
      This function takes a string argument and opens the 
      config file to create the credentials towards the DeGiro API
      A couple of checks are performed first
    """
    if not isinstance(path_to_config_file, str):
        raise TypeError("Error while reading argument, string expected, obtained : {}".format(type(path_to_config_file)))  # Or a more specific exception
        pass
    if not os.path.exists(path_to_config_file):
        raise FileNotFoundError("Configuration file not found: {}".format(path_to_config_file))
        pass
    try:
        with open(path_to_config_file) as config_file:
            config_dict = json.load(config_file)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        raise ValueError("Error loading configuration file: {}".format(e))  # Or a more specific exception
        pass
    logging.info("All Checks to load the configuration from the JSON file have passed")
    return config_dict
    

def trading_connect_DeGiro(path_to_config_file):
    
    config_dict = init_connection(path_to_config_file=path_to_config_file)

    # credentials can also be built from the dictionary containing the details from
    # the config file
    credentials=build_credentials(override=config_dict)

    try: 
        trading_api=TradingAPI(credentials=credentials)
        ConnectionID = str(trading_api.connect())
        logging.info("Connection ID:\n"
             +ConnectionID
             +"\nSuccessfully open since: {}".format(datetime.now())
             )
        return trading_api
    except Exception as e: 
        # Catch any exception from the unknown function
        error_message = f"An error occurred while calling '{TradingAPI.__name__}': {e}"
        raise ConnectionError(error_message 
                              + "\n Connection unsuccessful")
        pass
    
def quotecast_connect_DeGiro(path_to_config_file):
    
    config_dict = init_connection(path_to_config_file=path_to_config_file)
    user_token = config_dict.get("user_token")  
    try:
        chart_fetcher = ChartFetcher(user_token=user_token)
        logging.info("ChartFetcher OK"
                     +"\nSuccessfully open since: {}".format(datetime.now())
                     )
        return chart_fetcher
    except Exception as e:
        error_message = f"An error occurred while calling '{ChartFetcher.__name__}': {e}"
        raise ConnectionError(error_message
                              +"\n Connection unsuccessful")

