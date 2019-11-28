import logging
from datetime import date, datetime
import time
import ConfigController
import traceback
import json
import os
from collections import OrderedDict


class LogObject:

    def __init__(self):
        #print("welcome to LogObject class")
        self.objConfig = ConfigController.Configuration()
        self.logschema = self.objConfig.getConfiguration("Log|Log_Schema")
        self.Log_Path = self.objConfig.getConfiguration("Log|log_path")

    # For JsonLogMsg
    def fn_ProduceJsonLogMessage(self, domainname, eventId, message, module, lineno, filename, clas):
        try:
            LogSchema = open(self.logschema).read()

            # Convert json to python object.
                # OrderedDict is to maintain same key ordering of Log_Schema while printing log message
            LogJsonObj = json.loads(LogSchema, object_pairs_hook=OrderedDict)
            # JsonObj["lastModified"] = str(Obj["eventTimestamp"])

            #LogJsonObj["timestamp"] = time.asctime(time.localtime(time.time()))
            LogJsonObj["timestamp"] = datetime.now().strftime("%a %d-%b-%Y %H:%M:%S:%f")[:-3]
            LogJsonObj["application"] = domainname
            LogJsonObj["environment"] = "Python_Preprocessor"
            LogJsonObj["Identity"] = eventId
            LogJsonObj["message"] = message
            LogJsonObj["caller"]["line"] = lineno
            # to print only filename instead of entire filepath
            filenm = os.path.basename(filename)
            LogJsonObj["caller"]["file"] = str(filenm)
            LogJsonObj["caller"]["method"] = str(module)
            LogJsonObj["caller"]["class"] = str(clas)

            #print (LogJsonObj)
            # to remove unicode char while printing message
            LogJsonObj_print = json.dumps(LogJsonObj)
            return LogJsonObj_print

        except Exception as e:
            print(e)
            logging.error(traceback.format_exc())
        # Logs the error appropriately.

    def get_logger(self):

        """ This routine is intended to initiate and return the logger object
        and place the .log file in pre-defined path.
        :param: None
        :return: logger object
        """
        try:
            # no log file will be generated in log_path
            '''today = str(date.today())
            now = str(time.time())[:-3]

            objConfig = ConfigController.Configuration()
            outputPath = objConfig.getConfiguration("Log|log_path")
            sPath = str(outputPath) + "myapp_" + today + "-" + now + ".log"

            #logging.basicConfig(filename=sPath,format='%(asctime)s|%(levelname)s:%(message)s', level=logging.DEBUG)
            logging.basicConfig(filename=sPath, format='%(asctime)s|%(levelname)s:%(message)s', level=logging.INFO)'''

            #logger = logging.getLogger('simple_example')
            logger = logging.getLogger('PreProcessor_Log')
            # logger.setLevel(logging.DEBUG)
            logger.setLevel(logging.INFO)

            # to prevent repeated printing of same logger info in console
            if logger.handlers:
                #logger.handlers = []
                logger.handlers.pop()

            # create console handler and set level to debug
            ch = logging.StreamHandler()
            # ch.setLevel(logging.DEBUG)
            ch.setLevel(logging.INFO)

            # create formatter
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

            # add formatter to ch
            ch.setFormatter(formatter)

            # add ch to logger
            logger.addHandler(ch)
            return logger

        except Exception as e:
            print(e)
            logging.error(traceback.format_exc())
            # Logs the error appropriately.



