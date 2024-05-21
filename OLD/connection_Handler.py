import json
import os
from datetime import datetime
import error_details as ed

from degiro_connector.trading.api import API as TradingAPI
from degiro_connector.trading.models.credentials import build_credentials

def trading_connect_DeGiro(path_to_config_file):
    """
      This function takes a string argument and opens the 
      config file to create the credentials towards the DeGiro API
      A couple of checks are performed first
    """

    MSGlib = ed.MSG_from_this_library()
    
    if not isinstance(path_to_config_file, str):
        raise TypeError(MSGlib.access_MSG()+
                         "Error while reading argument, string expected, obtained : {}".format(type(path_to_config_file)))  # Or a more specific exception
        pass


    if not os.path.exists(path_to_config_file):
        raise FileNotFoundError(MSGlib.access_MSG()+
                                "Configuration file not found: {}".format(path_to_config_file))
        # warnings.warn("Configuration file not found: {}".format(path_to_config_file))
        # return None  # Or an empty dictionary if preferred
        pass
    
    try:
        with open(path_to_config_file) as config_file:
            config_dict = json.load(config_file)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        raise ValueError(MSGlib.access_MSG()+
                         "Error loading configuration file: {}".format(e))  # Or a more specific exception
        # warnings.warn("Error loading configuration file: {}".format(e))
        # return None  # Or an empty dictionary if preferred
        pass
    
    credentials=build_credentials(override=config_dict)
    # credentials can also be built from the dictionary containing the details from
    # the config file

    try: 
        
        trading_api=TradingAPI(credentials=credentials)
        ConnectionID = str(trading_api.connect())
        print(MSGlib.access_MSG()
              +"Connection ID:\n"
              +ConnectionID
              +"\nOpen since: {}".format(datetime.now())
              )
        return trading_api
    except Exception as e: 
        # Catch any exception from the unknown function
        error_message = f"An error occurred while calling '{TradingAPI.__name__}': {e}"
        raise ed.LibSpecificError(MSGlib.access_MSG()+ error_message 
                               + "\n Connection unsuccessful")
        pass
    

    



