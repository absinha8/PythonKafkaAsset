import traceback
import logging
import DBConnectionController
import AuditTrailController

# For JsonLogMsg
import LoggingController
import inspect
from inspect import currentframe, getframeinfo

class RIController:
    # Self Declaration
    def __init__(self):
        print("welcome to RI-Validation Controller class")
        self.objDBConnetionRI = DBConnectionController.DBConnection()
        self.objRIValAuditController = AuditTrailController.AuditTrail()
        self.Ri_ST = 'R'

        # For JsonLogMsg
        self.objLog = LoggingController.LogObject()
        self.logger = self.objLog.get_logger()

    def Select_Records_With_Key(self, tableName, key, keyValue):
        """
        retrun the rows from respective database table
        based on the given key column and respective value .
        :param connStr: the Connection object
        :param tableName: source table name
        :param key: key for RI checking
        :param keyValue: key value obtained from json object
        :return: rows generated from query on given database table considering where clauses
        """
        try:

            selectStr = "SELECT  * FROM" + " " + tableName + " " + "WHERE 1 = 1" + " "
            #            print(type(where_col), len(where_col))
            if ((len(key)) > 0):
                selectStr += " " + "AND" + " " + str(key) + "=" + "'" + str(keyValue) + "'"

            #print(selectStr)

            rows = self.objDBConnetionRI.fn_fetch_record(selectStr)

            return rows

        except Exception as e:
            print(e)
            logging.error(traceback.format_exc())
            # Logs the error appropriately.

    def RI_Key_Val_Determination(self, obj, key):
        '''
        determine the key value for the given key within the python object given
        expected key value will process further with the database table and
        complete the RI processing.
        :param obj: python object / terminal object
        :param key: given key for RI checking
        '''
        try:
            riKeyVal = ''
            if (isinstance(obj, dict)):
                riKeyVal = obj[str(key)]
            elif (isinstance(obj, list)):
                obj = obj[0]
                riKeyVal = obj[str(key)]
            else:
                print("Error!!! python object is not either dict or list, not acceptable!!!")

            return riKeyVal

        except Exception as e:
            print(e)
            logging.error(traceback.format_exc())
            # Logs the error appropriately.
            # Return terminal object
            return None

    def RI_Object_Determination(self, obj, path):
        '''
        determine the terminal object based on the given path in metadta / schema / data dictionary
        expected object will be either dictionary or list of dictionaries
        per the abstarction of object was structured
        in json
        find the key value within the object and return the same
        :param obj: python object
        :param path: json traverse path, already pre-defined in metadata / schema /data dictionary
        :return: terminal object in metdata for RI checking
        Advancement: Needs to evaluate

        '''
        try:
            #print(path)
            pathItem = []
            if (path != "" and len(path) > 0):
                if (path.find('|') != -1):
                    index = path.find('|')
                    pathItem = path.split("|")

                    if (len(pathItem) > 0):
                        if (isinstance(obj[str(pathItem[0])], dict)):
                            obj = obj[str(pathItem[0])]
                            path = path[(index + 1):]
                            # return key word ensure that returned object from recursion function will propagate in order.
                            return self.RI_Object_Determination(obj, path)

                        elif (isinstance(obj[str(pathItem[0])], list)):

                            if (len(obj[str(pathItem[0])]) > 0):
                                # Terminal list object reached
                                obj = obj[str(pathItem[0])]
                                obj = obj[0]
                                path = path[(index + 1):]
                                return self.RI_Object_Determination(obj, path)
                else:
                    #Terminal list object reached
                    #print(path)
                    obj = obj[str(path)]
                    #print(obj)

            return obj

        except Exception as e:
            print(e)
            logging.error(traceback.format_exc())
            # Logs the error appropriately.

    def fn_RI_validation_process(self, obj, finalList, eventId, domainName):
        '''
        validation routine intended to chceck Referrential Integrity
        between the schema attribute and value with
        respective database table and column
        :param connObj: connection string given
        :param obj: given json from Kafka consumer
        :param finalList:
        :return: validation flag based on RI process routine
        Advancement: None as of now 14 Jan 2019
        '''

        try:
            # For JsonLogMsg
            cf = currentframe()
            module = cf.f_code.co_name
            filename = cf.f_code.co_filename
            clas = self.__class__
            log_message = "RI Validation is started"
            self.logger.info(self.objLog.fn_ProduceJsonLogMessage(domainName, eventId, log_message, module, cf.f_lineno, filename, clas))

            # initial flag setting for RI validation
            riValidationFlag = 'F'
            if len(finalList) > 0:
                for item in finalList:
                    #                if (item[10] == "Y"):
                    # print(item[9])
                    if (item[9] != "" and len(item[9]) > 0):
                        #riColTotals = str(item[9]).split("~")
                        riCol = str(str(item[9]).split("~")[0])
                        riSourceTab = str(item[11])
                        riSourceTabCol = str(item[12])
                        nodePath = str(item[8])
                        #               print(RI_col,RI_source_tab,RI_source_tab_col,Node_Path)
                        '''
                        identify and narrow down the scope of obj from given json/finaljson
                        separate processing for level 0 and 1
                        to make the routine faster
                        '''
                        # process json object separately for level 0 and 1
                        if item[2] == 0:
                            jsonobj = obj

                        elif item[2] == 1:
                            #jsonobj = json.loads(obj["payload"])
                            jsonobj = obj["payload"]
                            #jsonobj = jsonobj["entityValue"]
                            #jsonobj = jsonobj["Fund"]

                        else:
                            #jsonobj = json.loads(obj["payload"])
                            jsonobj = obj["payload"]
                            #jsonobj = obj

                        # Call terminal object determination process (at leaf level), in otherway direct to array item in json
                        terminalObj = self.RI_Object_Determination(jsonobj, nodePath)

                        #               if isinstance(terminal_obj, list):
                        #                   #consider first item of list
                        #                   #as RI value of key fields expected to be same for all items in list
                        #                   terminal_obj = terminal_obj[0]

                        keyVal = self.RI_Key_Val_Determination(terminalObj, riCol)
                        #print(keyVal)
                        r = self.Select_Records_With_Key(riSourceTab, riSourceTabCol, keyVal)

                        if len(r) == 0:
                            riValidationFlag = 'T'
                            processStatus = 'RI Validation Fail'
                            self.objRIValAuditController.fn_AuditTrailInsertProcess(eventId, domainName, processStatus, self.Ri_ST, '0')
                            break

            # For JsonLogMsg
            log_message = "RI Validation is completed with RiValidationFlag = " + riValidationFlag
            self.logger.info(self.objLog.fn_ProduceJsonLogMessage(domainName, eventId, log_message, module, cf.f_lineno, filename, clas))

            return riValidationFlag
        
        except Exception as e:
            print(e)
            logging.error(traceback.format_exc())
            # Logs the error appropriately.

            return None
