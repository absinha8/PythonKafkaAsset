import traceback
import logging
import DBConnectionController
import ConfigController
import AuditTrailController

# For JsonLogMsg
import LoggingController
import inspect
from inspect import currentframe, getframeinfo

class Validation:
    # Self Declaration
    def __init__(self):
        #print("welcome to Validation class")
        objConfig = ConfigController.Configuration()
        self.Aud_Tab = objConfig.getConfiguration("Audit|Aud_Tab")
        self.Exclu_Tab = objConfig.getConfiguration("Audit|Exclu_Tab")
        #self.Fund_Tab = objConfig.getConfiguration("Audit|Fund_Tab")
        self.Fund_Tab = 'T_FD'
        self.objDBConnetion = DBConnectionController.DBConnection()
        self.objValAuditController = AuditTrailController.AuditTrail()
        self.ST = 'R'

        # For JsonLogMsg
        self.objLog = LoggingController.LogObject()
        self.logger = self.objLog.get_logger()

    def fn_validation_process(self, jsonDicObj, msg_offset, msg_partition):
        
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
        validationFlag = 'F'  + "|" + "DEFAULT"

        try:
            # fetch system date and time
            # system date time is not anymore required to capture as
            # data column AUDIT_TS@NTDM_AUDIT.DS_AUDIT_TAB provides system date time by default
            # update as on 18 Feb 2019 - Shouvik
            # current_date_time = get_datetime()

            # remarks argument is hard-coded as on 13 Feb 2019
            # that entry is going to JOB_ID column in database table

            # extract elemenatry outer attributes of json from given directory structure
            eventId = str(jsonDicObj["eventId"])
            domainName = str.upper(str(jsonDicObj["domainName"]))

            # For JsonLogMsg
            cf = currentframe()
            module = cf.f_code.co_name
            filename = cf.f_code.co_filename
            clas = self.__class__
            log_message = "Validation is started"
            self.logger.info(self.objLog.fn_ProduceJsonLogMessage(domainName, eventId, log_message, module, cf.f_lineno, filename, clas))

            if 'eventType' in jsonDicObj and 'eventClass' in jsonDicObj:
                eventType = str.upper(str(jsonDicObj["eventType"]))
                eventClass = str.upper(str(jsonDicObj["eventClass"]))
                print("eventType: " + eventType)

                # create insert list for audit trail table :NTDM_AUDIT.DS_AUDIT_TAB
                # immutable list will be changed for 'dup' argument based on whether duplicate event
                # Remove Audit entry 18 Feb 2019 - Shouvik

                # calling the audit_trail_process routine
                auditTrailFlag = self.audit_trail_process(self.Aud_Tab, eventId)
                #
                print(auditTrailFlag)

                if (auditTrailFlag) != 'Y':
                    # trigger the exclusion level validation
                    # create where clause entire dynamically
                    # whereDict is a dictionary object having pre-defined three keys [DOMAIN_NAME,EVENT_CLASS,EVENT_TYPE]
                    # based on the target look up table NTMETADATA.EVENT_EXCLUTION
                    whereDict = dict(
                        {'UPPER(DOMAIN_NAME)': str(domainName), 'UPPER(EVENT_CLASS)': str(eventClass), 'UPPER(EVENT_TYPE)': str(eventType)})
                    # keys = ['domainName', 'eventClass', 'eventType']
                    # whereDict = get_dict(jsonDicObj, keys)
                    # print(whereDict)
                    exclusionFlag = self.exclusion_process(self.Exclu_Tab, whereDict)
                    # whereDict = {}
                    print(exclusionFlag)
                    if (exclusionFlag) != 'T':
                        outoforderFlag = self.fn_validation_process_out_of_order(self.Fund_Tab, jsonDicObj)
                        print(outoforderFlag)
                        if outoforderFlag != 'T':
                            validationFlag = 'T' + "|" + "None"
                        else:
                            processStatus = 'Out of Order - Validation Fail'
                            self.objValAuditController.fn_AuditTrailInsertProcess(eventId, domainName, processStatus, self.ST, '0', msg_offset, msg_partition)
                            validationFlag = 'F' + "|" + "OOFORDR"
                    else:
                        processStatus = 'Exclusion - Validation Fail'
                        self.objValAuditController.fn_AuditTrailInsertProcess(eventId, domainName, processStatus, self.ST, '0', msg_offset, msg_partition)
                        validationFlag = 'F' + "|" + "EXCLUSION"
                else:
                    processStatus = 'Duplicate Event ID - ' + eventId
                    self.objValAuditController.fn_AuditTrailInsertProcess(eventId, domainName, processStatus, self.ST, '0', msg_offset, msg_partition)
                    validationFlag = 'F' + "|" + "DUP"
            else:
                processStatus = 'eventType or eventClass Not found - ' + eventId
                self.objValAuditController.fn_AuditTrailInsertProcess(eventId, domainName, processStatus, self.ST, '0', msg_offset, msg_partition)
                validationFlag = 'F' + "|" + "ETORECMISSING"

            # For JsonLogMsg
            log_message = "Validation is completed with ValidationFlag = " + validationFlag
            self.logger.info(self.objLog.fn_ProduceJsonLogMessage(domainName, eventId, log_message, module, cf.f_lineno, filename, clas))

        except Exception as e:
            print(e)
            logging.error(traceback.format_exc())
            # Logs the error appropriately.

        return validationFlag

    def audit_trail_process(self, tab, evntId):
        
        """This routine is intended to 
        select event id in the database table named AUDIT_TRAIL,
        if return values mark the entry as duplicate [IS_DUPLICATE = 'Y'], otherwise [IS_DUPLICATE = 'N']
        :param conn: connection string given
        :param tab: source table name
        :param evntId: given event id in json
        :param insertLst: Insert List
        :param remarks: user given remarks
        :return: None
        :Advancement: None
        """
        
        try:

            r = self.select_records_with_eventid(tab, '', evntId)
            #        print(len(r))
            #        for item in r:
            #            print(r)
            if len(r) > 0:
                dup = 'Y'
            else:
                dup = 'N'

            return dup

        except Exception as e:
            print(e)
            logging.error(traceback.format_exc())
            # Logs the error appropriately.

    def exclusion_process(self, tab, whereDict):
        
        """This routine is intended to 
        select a combination of value with domain name, event class and event type
        in the database table named EXCLUSION_EVENT,
        if return values, nothing to process further
        :param conn: connection string given
        :param tab: source table name
        :param whereDict: Where Clause in SQL query represents with Column Name and Values as a dictionary
        :return: rows generated from query on given table with with domain name, event class and event type
        """

        exclFlag = 'F'
        try:
            exclusionRows = self.select_records_with_exclusion(tab, whereDict)
            if len(exclusionRows) > 0:
                exclFlag = 'T'

        except Exception as e:
            print(e)
            logging.error(traceback.format_exc())
            # Logs the error appropriately.

        return exclFlag


    def select_records_with_eventid(self, tableName, selectCol, evntId):
        
        """This routine is intended to 
        select records in the database table
        :param connStr: the Connection object
        :param tableName: source table name
        :param selectCol: column values needs to be fetched (in a comma separated string format) for from the source table_name
        :param evntId: given event id in json
        :return: rows generated from query on given table with event id
        :Advancement: make the routine more generic than only eventid
        """
        try:

            if ((len(selectCol)) > 0):
                selectStr = "SELECT " + selectCol + " FROM " + " " + tableName
            else:
                selectStr = "SELECT  * FROM" + " " + tableName

            if ((len(evntId)) > 0):
                selectStr = selectStr + " " + "WHERE EVNT_ID =" + "'" + str(evntId) + "'"
                selectStr += " " + "AND STAT =" + "'" + "Process Finished" + "'"

            records = self.objDBConnetion.fn_fetch_record(selectStr)

            return records

        except Exception as e:
            print(e)
            logging.error(traceback.format_exc())
            # Logs the error appropriately.

    def select_records_with_exclusion(self, tableName, whereCol):
        
        """This routine is intended to
        select records in the database table
        :param connStr: the Connection object
        :param tableName: source table name
        :param whereCol: argument as a dictionary object having column name-value pairs needs to be checked in WHERE clause
        :return: rows generated from query on given table considering specific where clauses
        """
        try:

            selectStr = "SELECT  * FROM" + " " + tableName + " " + "WHERE 1 = 1" + " "
            #        print(type(where_col), len(where_col))
            if ((len(whereCol)) > 0):
                for i in whereCol:
                    #                print(i, where_col[i])
                    selectStr += " " + "AND" + " " + i + "=" + "'" + str(whereCol[i]) + "'"

            #print(selectStr)

            rows = self.objDBConnetion.fn_fetch_record(selectStr)

            return rows

        except Exception as e:
            print(e)
            logging.error(traceback.format_exc())
            # Logs the error appropriately.

    def get_dict(self, obj, listOfKeys):
        
        '''This routine is intended to
        return a disctionary object from json object
        having column-value pair at database table level
        :param obj: given json object
        :param listOfKeys: given list of keys
        :return: a dictionary object
        :Advancement : make the routine more generic [with list of inputs to fetch values from json] + list_of_keys will be the input of the function
        '''
        try:

            whereClauseDict = {}
            whereClauseDict = dict(map(lambda key: (key, obj.get(key, None)), listOfKeys))

            #print (where_clause_dict)

            return whereClauseDict

        except Exception as e:
            print(e)
            logging.error(traceback.format_exc())
            # Logs the error appropriately.


    def fn_validation_process_out_of_order(self, tab, jsonDicObj):
        '''
        validation routine to check uberfundid validity with the event type
        when event type is "FUND_ADD", check whether records in fund_master table
        exists having same uberfundid.
        utilizing the uberfundid validation_process routine
        intended to perform validation and returns a flag
        based on the flag value, decide whether to proceed for subsequent parsing process
        :param conn: connection string given
        :param tab: source table name :: 'NTRAW.FD / FUND Table'
        :param json_dic_obj: given json from Kafka consumer
        :return: validation flag based on uberfundid validation process
        Advancement: None as of now 13 Feb 2019
        '''

        # initial OOS_Flag set as "F = False"
        oooFlag = "F"

        try:
            # Json traverse and get in directory type
            # as of now hard-coded dated 13 Feb 2019

            obj1 = jsonDicObj["payload"]
            #obj2 = obj1["Fund"]
            obj2 = obj1

            # Checking whether eventtype is "FUND_ADD"
            #eventType = str(obj2["eventType"])
            eventType = str(jsonDicObj["eventType"])
            #        event_Type = "FUND_ADD"
            # print(eventType)

            if (eventType == "FUND_ADD"):
                UB_FD_ID = str(obj2["uBERFundId"])
                #            keys = ['uBERFundId']
                #            whereDict = get_dict(obj2, keys)
                whereDict = dict({'UB_FD_ID': str(UB_FD_ID)})
                # call routine to check whether any other records exists in data table before "FUND_ADD" event type.
                r = self.select_records_with_uberfundid(tab, whereDict)
                # print(len(r))
                if len(r) > 0:
                    oooFlag = 'T'

        except Exception as e:
            print(e)
            logging.error(traceback.format_exc())
            # Logs the error appropriately.

        return oooFlag

    def select_records_with_uberfundid(self, tableName, whereCol):
        """
        select records in the database table
        :param connStr: the Connection object
        :param tableName: source table name
        :param whereCol: argument as a dictionary object having column name-value pairs needs to be checked in WHERE clause
        :return: rows generated from query on given table considering specific where clauses
        """
        try:

            selectStr = "SELECT  * FROM" + " " + tableName + " " + "WHERE 1 = 1" + " "
            #        print(type(whereCol), len(whereCol))
            if ((len(whereCol)) > 0):
                for i in whereCol:
                    #                print(i, where_col[i])
                    selectStr += " " + "AND" + " " + i + "=" + "'" + str(whereCol[i]) + "'"

            #print(selectStr)

            rows = self.objDBConnetion.fn_fetch_record(selectStr)

            return rows

        except Exception as e:
            print(e)
            logging.error(traceback.format_exc())
            # Logs the error appropriately.