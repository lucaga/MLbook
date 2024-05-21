import os
# # Get the absolute path of the current module (for example a library)
# # Extract the filename from the path (optional)
# # create a custom epilogue message to use when throwing an error
# current_file_path = __file__
# library_filename = os.path.basename(current_file_path)
# Epilogue_MSG_internal = "Message from " + library_filename +":\n"

# in the following a couple of classes to make this code
# possibly easier to run and debug

# This might be something to inherit from a overall library. 
# This would avoid copy-paste omission

class MSG_from_this_library():
    # Get the absolute path of the current module (for example a library)
    # Extract the filename from the path (optional)
    # create a custom epilogue message to use when throwing an error    

    def __init__(self):
        current_file_path = __file__
        library_filename = os.path.basename(current_file_path)
        self._Epilogue_MSG_internal = "Message from " + library_filename + ":\n"    

    def access_MSG(self):
        return self._Epilogue_MSG_internal
    
class LibSpecificError(Exception):
    """
    This is a custom exception class you can define for your specific error handling.
    """
    pass