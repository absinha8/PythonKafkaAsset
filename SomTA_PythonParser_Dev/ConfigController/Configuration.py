import yaml
import traceback
import logging
import os

class Configuration:

    def __init__(self):
        #print("welcome to Configuration class")
        self.CONFIG_FILE = os.environ['CONFIG_FILE']


    def getConfiguration(self, strKey):
        """
            This routine is intended to read items under 
            a pre-defined configuration structure 
            YML config file and return the object as per.
            :param: Key in Configuration file
            :return: object list 
        """   
        try:

            yml_file_path = self.CONFIG_FILE
                
            with open(yml_file_path, 'r') as ymlfile:
                cfg = yaml.load(ymlfile)
    
            i = 0
            obj = []
    
            for item in strKey.split("|"):
                if i == 0:
                    obj = cfg[str(item)]
                else:
                    obj = obj[str(item)]
    
                i = i + 1
                
            return str(obj)
        
        except Exception as e:
            print(e)
            logging.error(traceback.format_exc())
            # Logs the error appropriately.
            
        
