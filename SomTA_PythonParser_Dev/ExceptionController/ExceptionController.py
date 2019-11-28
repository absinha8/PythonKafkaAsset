import traceback
import logging
import DBConnectionController
import AuditTrailController
import ConfigController
import ProducerController
import cx_Oracle
import json
from datetime import date
import time
import os
import pandas as pd

# For JsonLogMsg
import LoggingController
import inspect
from inspect import currentframe, getframeinfo


class ExceptionController:
    # Self Declaration
    def __init__(self):
        #print("welcome to Exception Controller class")
        self.objConfig = ConfigController.Configuration()
        self.Exception_Tab = self.objConfig.getConfiguration("Exception|Exception_Tab")
        self.Exception_Path = self.objConfig.getConfiguration("Exception|Exception_Path")
        self.Exception_Schema = self.objConfig.getConfiguration("Exception|Exception_Schema")
        self.Exception_Topic = self.objConfig.getConfiguration("Exception|Exception_Topic")
        self.BASE_EXCEP_DIR = self.objConfig.getConfiguration("Exception|Exception_Path")

        self.objDBConnetionRI = DBConnectionController.DBConnection()
        self.objRIValAuditController = AuditTrailController.AuditTrail()
        self.objExcepProducer = ProducerController.ProducerDS()

        # For JsonLogMsg
        self.objLog = LoggingController.LogObject()
        self.logger = self.objLog.get_logger()

    def fn_ProducceJsonErrorDoc(self, Obj, errorCode, errorDescription):
        try:
            ErrorSchema = open(self.Exception_Schema).read()

            # Convert json to python object.

            JsonObj = json.loads(ErrorSchema)
            JsonPayLoadobj = JsonObj["payload"]

            # Extract the attributes and objects from Error Schema
            PayloadObj = Obj["payload"]
            EventHeaderObj = Obj["eventHeader"]

            JsonObj["eventId"] = str(Obj["eventId"])
            JsonObj["domainName"] = str(Obj["domainName"])
            JsonObj["eventType"] = "Exception"
            JsonObj["createdAt"] = str(Obj["eventTimestamp"])
            #JsonObj["lastModified"] = str(EventHeaderObj["sourceEventTimestamp"])
            JsonObj["lastModified"] = str(time.time())

            JsonObj["payload"]["domainName"] = str(Obj["domainName"])
            JsonObj["payload"]["eventType"] = str(Obj["eventType"])
            JsonObj["payload"]["eventClass"] = str(Obj["eventClass"])
            JsonObj["payload"]["sourceEventId"] = str(EventHeaderObj["sourceEventId"])
            JsonObj["payload"]["sourceEntityId"] = "sourceEntityId"
            JsonObj["payload"]["sourcePublisherId"] = str(EventHeaderObj["publisherId"])

            JsonObj["errorList"][0]["errorCode"] = str(errorCode)
            JsonObj["errorList"][0]["errorDescription"] = str(errorDescription)

            print(JsonObj)
            return JsonObj

        except Exception as e:
            print(e)
            logging.error(traceback.format_exc())
        # Logs the error appropriately.


    def select_errorcode(self, tableName, domainName, expType):
        """This routine is intended to
        select records in the database table
        :param connStr: the Connection object
        :param tableName: source table name
        :param whereCol: argument as a dictionary object having column name-value pairs needs to be checked in WHERE clause
        :return: rows generated from query on given table considering specific where clauses
        """
        try:
            selectStr1 = "SELECT DECODE((EXCEPTION_CODE || '~' || EXCEPTION_MSG),NULL,'NO',(EXCEPTION_CODE || '~' || EXCEPTION_MSG)) AS EXCEP FROM" + " " + tableName + " "
            wherecl = "WHERE DOMAIN_NAME  = '" + domainName + "' AND EXCEPTION_TYPE = '" + expType + "'"
            selectStr = selectStr1 + " " + wherecl

            print(selectStr)

            rows = self.objDBConnetionRI.fn_fetch_record(selectStr)

            print(rows)

            return str(rows)

        except Exception as e:
            print(e)


    def fn_createexceptionlist(self):
        """ This routine is intended to create a
        list object represents Exception metadata
        from the pre-defined SQL script. Flexible to connect in
        different database instacne based on the "Connection" attribute
        of the configuration object utilizing DSN based connectivity.
        :param:None
        :return: List object represents metadata / data dictionary / schema object
        """
        try:
            ip = self.objConfig.getConfiguration("Connection|ip")
            port = self.objConfig.getConfiguration("Connection|port")
            SERVICE_NAME = self.objConfig.getConfiguration("Connection|SID")
            DB_ORA_USER = os.environ['DB_ORA_USER']
            DB_ORA_PWD = os.environ['DB_ORA_PWD']

            connection = DB_ORA_USER + "/" + DB_ORA_PWD + "@" + ip + ":" + port + "/" + SERVICE_NAME

            sqlQuery = """SELECT 
                            TRIM(UPPER(DOMAIN_NAME)) AS DOMAIN_NAME,
                            TRIM(EXCEPTION_TYPE) AS EXCEPTION_TYPE,
                            TRIM(EXCEPTION_CODE) AS EXCEPTION_CODE,
                            TRIM(EXCEPTION_MSG) AS EXCEPTION_MSG 
                            FROM T_EVENT_EXCEPTION
                             WHERE IS_ACTIVE='Y'"""

            connect = cx_Oracle.connect(connection)
            dframe = pd.read_sql_query(sqlQuery, connect)
            connect.close()

            finalList = []
            for index, row in dframe.iterrows():
                myList = list()
                myList.append(str(row["DOMAIN_NAME"]))
                myList.append(str(row["EXCEPTION_TYPE"]))
                myList.append(str(row["EXCEPTION_CODE"]))
                myList.append(str(row["EXCEPTION_MSG"]))

                finalList.append(myList)

            finalMetaDatalist = finalList
            #print(finalMetaDatalist)
            return finalMetaDatalist

        except Exception as e:
            print(e)
        # logging.error(traceback.format_exc())
        # Logs the error appropriately.


    def del_err_file(self, err_path, domainName):
        try:
            my_dir = err_path
            for fname in os.listdir(my_dir):
                print(fname)
                if fname.find(str(domainName)):
                    fileNm = str(err_path) + str(fname)
                    print(fileNm)
                    os.remove(fileNm)

            print("File remove with Domain Name " + domainName)
            # For JsonLogMsg
            cf = currentframe()
            module = cf.f_code.co_name
            file_name = cf.f_code.co_filename
            clas = self.__class__
            log_message = "Existing Error file with Domain Name - " + domainName + " - is removed from Exception_Path"
            self.logger.info(self.objLog.fn_ProduceJsonLogMessage(domainName, None, log_message, module, cf.f_lineno, file_name, clas))

        except Exception as e:
            print(e)
            logging.error(traceback.format_exc())
        # Logs the error appropriately.
        return None


    def fn_error_file_create(self, filename, jsonobj):
        try:
            ErrfileObj = open(filename, 'w+')
            json.dump(jsonobj, ErrfileObj)
            ErrfileObj.close()
            print(filename + " has created")
            # For JsonLogMsg
            cf = currentframe()
            module = cf.f_code.co_name
            file_name = cf.f_code.co_filename
            clas = self.__class__
            log_message = "Error file - " + os.path.basename(filename) + " - is created in Exception_Path"
            self.logger.info(self.objLog.fn_ProduceJsonLogMessage(None, None, log_message, module, cf.f_lineno, file_name, clas))

        except Exception as e:
            print(e)
            logging.error(traceback.format_exc())
        # Logs the error appropriately.
        return None


    def fn_exception_process(self, Obj, expType, domainName, exception_list):

        '''
        validation routine to chceck audit trail in first step
        insert record into audit_trail table utilizing the audit_trail_process routine / table mentioned through tab1
        in next step consider exclusion records in exclusion_event table mentioned through parameter tab2
        utilizing the exclusion_process routine
        next step is to evaluate Out of Order sequence by evaluating uBERFundId in Fund table NTRAW.FD
        intended for complete validation and returns a flag
        based on the flag value, decide whether to proceed for subsequent parsing process
        :param connObj: connection string given
        :param tab1: source table name :: 'NTDM_AUDIT.DS_AUDIT_TAB'
        :param tab2: source table name :: 'NTMETADATA.EVENT_EXCLUTION'
        :param tab3: source table name :: 'NTRAW.FD'
        :param jsonDicObj: given json from Kafka consumer
        :param status: process status given (to insert process status for testing)
        :return: validation flag based on audit_trail and exclusion process
        Advancement: None as of now 13 Feb 2019
        '''

        # initial flag setting for validation
        exceptionFlag = 'F'

        try:
            # Delete SQL File creation
            # =============================================================================================
            err_path = self.Exception_Path
            today = str(date.today())
            strTime = str(time.time())
            strTm = strTime[:-3]
            Error_filePath = str(err_path) + "Error_" + domainName + "_" + today + "-" + strTm + ".json"
            #print(Error_filePath)
            #print(expType)

            safeDir = self.BASE_EXCEP_DIR
            if os.path.commonprefix([Error_filePath, safeDir]) != safeDir:
                print("Bad request")
            else:
                # ====================== Create exception specific ErrorCode =================================================
                df_filter_Excep_Metadata = pd.DataFrame(exception_list)

                # For JsonLogMsg
                cf = currentframe()
                module = cf.f_code.co_name
                file_name = cf.f_code.co_filename
                clas = self.__class__

                if expType != "DEFAULT":
                    df_filtered_Excepdata = df_filter_Excep_Metadata.loc[df_filter_Excep_Metadata[1] == expType]
                    final_excep_list = df_filtered_Excepdata.values.tolist()

                    #print(final_excep_list)

                    ExpCode = ""
                    ExpMsg = ""
                    for item in final_excep_list:
                        ExpCode = item[2]
                        ExpMsg = item[3]

                    #print(ExpCode)
                    #print(ExpMsg)
                    # For JsonLogMsg
                    log_message = "ErrorType = " + expType + "; ErrorCode = " + ExpCode + "; ErrorDescription = " + ExpMsg
                    self.logger.info(self.objLog.fn_ProduceJsonLogMessage(domainName, None, log_message, module, cf.f_lineno, file_name, clas))

                    expjson = self.fn_ProducceJsonErrorDoc(Obj, ExpCode, ExpMsg)

                    self.del_err_file(err_path, domainName)

                    self.fn_error_file_create(Error_filePath, expjson)

                    self.objExcepProducer.fn_producer_excep(expjson, self.Exception_Topic)

                    print(expjson)

                else:
                    print("Exception type does not exist")
                    # For JsonLogMsg
                    log_message = "Exception type does not exist"
                    self.logger.info(self.objLog.fn_ProduceJsonLogMessage(domainName, None, log_message, module, cf.f_lineno, file_name, clas))


        except Exception as e:
            print(e)
            logging.error(traceback.format_exc())
        # Logs the error appropriately.

        return exceptionFlag

