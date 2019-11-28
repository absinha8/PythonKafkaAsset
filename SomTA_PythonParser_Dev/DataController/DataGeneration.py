import PublishToDBController
import json
import ConfigController
import LoggingController
import DBConnectionController
import traceback
import logging
import os
import time
#from datetime import date
import datetime

# For JsonLogMsg
import inspect
from inspect import currentframe, getframeinfo

class DataGeneration:
    # Self Declaration
    def __init__(self):
        #print("welcome to Data Generation class")
        self.objDBConnetion = DBConnectionController.DBConnection()
        self.objPublishDB = PublishToDBController.PublishDB()
        self.objConfig = ConfigController.Configuration()
        self.TimestampFormat = "RRRR-MM-DD HH24:MI:SS.FF"
        self.BASE_SQL_DIR = self.objConfig.getConfiguration("SQL|sql_path")

        # For JsonLogMsg
        self.objLog = LoggingController.LogObject()
        self.logger = self.objLog.get_logger()

    # Delete only if file exists ##
    def del_sql_file(self, filename):
        try:
            # For JsonLogMsg
            cf = currentframe()
            module = cf.f_code.co_name
            file_name = cf.f_code.co_filename
            clas = self.__class__

            if os.path.exists(filename):
                os.remove(filename)
                print("SQL Script file " + filename + " has deleted from system")
                # For JsonLogMsg
                log_message = "SQL Script file - " + os.path.basename(filename) + " - is removed from Sql_Path"
                self.logger.info(self.objLog.fn_ProduceJsonLogMessage(None, None, log_message, module, cf.f_lineno, file_name, clas))
            else:
                print("SQL file doesn't exists in system with file name %s file." % filename)
                # For JsonLogMsg
                log_message = "SQL Script file - " + os.path.basename(filename) + " - doesn't exist in Sql_Path"
                self.logger.info(self.objLog.fn_ProduceJsonLogMessage(None, None, log_message, module, cf.f_lineno, file_name, clas))

        except Exception as e:
            print(e)
            logging.error(traceback.format_exc())
            # Logs the error appropriately.
        return None

    # Delete query execution ##
    def fn_del_sql_exec(self, filename, final_list, eventId, domainName, msg_offset, msg_partition):
        try:
            # For JsonLogMsg
            cf = currentframe()
            module = cf.f_code.co_name
            file_name = cf.f_code.co_filename
            clas = self.__class__

            safeDir = self.BASE_SQL_DIR
            if os.path.commonprefix([filename, safeDir]) != safeDir:
                print("Bad request")
            else:
                delfileObj = open(filename, 'w+')

                for item in final_list:
                    delTableNm = str(item[4])
                    if delTableNm != 'DUMMY':
                        # change in delete string generation using parameterised query
                        delstr = "Delete From {} Where BN_EVENT_ID ='{}'".format(delTableNm, eventId) + "|" + '\n'
                        delfileObj.write(delstr)
                str_sql_footer = """select sysdate from dual"""
                delfileObj.write(str_sql_footer)
                delfileObj.close()
                #print("Data deletion is being started")
                # Run Delete SQL file
                print("Deletion start from file.... ")
                # For JsonLogMsg
                log_message = "Data deletion from file is started"
                self.logger.info(self.objLog.fn_ProduceJsonLogMessage(domainName, eventId, log_message, module, cf.f_lineno, file_name, clas))

                sql_out = self.objDBConnetion.fn_run_sql_script(eventId, domainName, filename, msg_offset, msg_partition)
                print(sql_out)

                if str(sql_out) != "E":
                    print("Delete SQL Script run complete")
                    # For JsonLogMsg
                    log_message = "Delete SQL script execution is completed"
                    self.logger.info(self.objLog.fn_ProduceJsonLogMessage(domainName, eventId, log_message, module, cf.f_lineno, file_name, clas))
                else:
                    print("Delete SQL Script run has been failed")
                    # For JsonLogMsg
                    log_message = "Delete SQL script execution is failed"
                    self.logger.info(self.objLog.fn_ProduceJsonLogMessage(domainName, eventId, log_message, module, cf.f_lineno, file_name, clas))

                #print("Deletion complete from file.... ")
                #print("Data Deletion has been completed successfully")

                self.del_sql_file(filename)


        except Exception as e:
            print(e)
            logging.error(traceback.format_exc())
            # Logs the error appropriately.
        return None

    def print_all_nodes(self, obj, strList1, eventId, entityId,strList, sPath, strtCollist, fileObj, keyList, strAllKey, upd_fields, updfileObj, full_metadata, main_obj, entity_transfer_list):
        
        """
        This routine is intended to print all nodes in the json objects
        and publish through "PublishToDBController" for either dictionary or list object.
        :param obj: the given JSON object
        :param strList1: Process traversed path in JSON / a runtime adjustment through program
        :param eventId: event ID an unique reference given for the message
        :param entityId: entity ID an unique reference given for the message  
        :param strList: the given JSON fields per level 
        :param sPath: Target table name given.
        :param strtCollist: Database table columns.
        :return: None
        :Advancement:None as of now
        """   
        try:
        
            item = strList1.split("|")
            new_items = item[1:]
            strFinal = ''

            for i in new_items:
                strFinal = strFinal + i + "|"
    
            if(item[0].find('$') > -1):
                str1 = item[0]
                if str(str1[:-1]) in obj :
                    obj1 = obj[str(str1[:-1])]
                else:
                    return
            else:
                if str(item[0]) in obj:
                    obj1 = obj[str(item[0])]
                else:
                    return
    
            if item[0].find('$') > -1:
                if isinstance(obj1, dict):
                    if sPath != 'DUMMY':
                        self.objPublishDB.fn_json_wr_dict(eventId, entityId, obj1, strList, sPath,strtCollist, fileObj, obj, keyList, upd_fields, updfileObj, full_metadata, main_obj, entity_transfer_list)
                elif isinstance(obj1, list):
                    if sPath != 'DUMMY':
                        self.objPublishDB.fn_json_wr_list(eventId, entityId, obj1, strList, sPath,strtCollist, fileObj, obj, keyList, full_metadata, main_obj, entity_transfer_list)
                else:
                    print(obj1)
            else:
                Json_Arr_Nm = item[0]
                if isinstance(obj1, dict):
                    if strAllKey != "NO":
                        All_Key = strAllKey.split("|")
                        i = 0
                        for key in All_Key:
                            Arr_Nm = key.split(".")[0]
                            key_Nm = key.split(".")[1]
                            if key_Nm in obj1:
                                if Json_Arr_Nm == Arr_Nm:
                                    keyList[i] = obj1[str(key_Nm)]
                            i = i + 1
                    self.print_all_nodes(obj1, strFinal, eventId, entityId, strList, sPath, strtCollist, fileObj, keyList, strAllKey, upd_fields, updfileObj, full_metadata, main_obj, entity_transfer_list)
                elif isinstance(obj1, list):
                    for item in obj1:
                        if strAllKey != "NO":
                            All_Key = strAllKey.split("|")
                            i = 0
                            for key in All_Key:
                                Arr_Nm = str(key.split(".")[0])
                                key_Nm = key.split(".")[1]

                                if key_Nm in item:
                                    if Arr_Nm == Json_Arr_Nm:
                                        keyList[i] = item[str(key_Nm)]
                                i = i + 1
                                #print(keyList)
                        self.print_all_nodes(item, strFinal, eventId, entityId, strList, sPath, strtCollist, fileObj, keyList, strAllKey, upd_fields, updfileObj, full_metadata, main_obj, entity_transfer_list)
                else:
                    print(obj1)

        except Exception as e:
            print(e)
            logging.error(traceback.format_exc())
            # Logs the error appropriately.            

        return None

    def fn_generatedata(self, obj, final_list, eventId, entityId, domainName, msg_offset, msg_partition):

        """
        This routine is intended to generate data from the json objects
        by referencing data dictionary dictionary or list object.
        :param obj: the given JSON object
        :param final_list: JSON metadata / schema / data dictionary
        :param eventId: event ID an unique reference given for the message
        :param entityId: entity ID an unique reference given for the message  
        :return: None
        :Advancement:None as now 
        """           

        try:

            returnValue = 0
            # For JsonLogMsg - commented as logger object defined in _init_
            # objLog = LoggingController.LogObject()
            # logger = objLog.get_logger()
            print("Data generation start")
            # For JsonLogMsg
            cf = currentframe()
            module = cf.f_code.co_name
            file_name = cf.f_code.co_filename
            clas = self.__class__
            log_message = "Data generation is started"
            self.logger.info(self.objLog.fn_ProduceJsonLogMessage(domainName, eventId, log_message, module, cf.f_lineno, file_name, clas))

            main_obj = obj
            full_metadata = final_list

            #SQL File creation
            #+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
            sql_path = self.objConfig.getConfiguration("SQL|sql_path")
            today = str(datetime.date.today())
            strTime = str(time.time())
            strTm = strTime[:-3]
            filePath = str(sql_path) + "Ins_" + today + "-" + strTm + ".sql"
            updfilePath = str(sql_path) + "Upd_" + today + "-" + strTm + "_upd.sql"

            safeDir = self.BASE_SQL_DIR
            if os.path.commonprefix([filePath, safeDir]) != safeDir or os.path.commonprefix([updfilePath, safeDir]) != safeDir:
                print("Bad request")
                returnValue = -1
            else:
                #str_sql_header = "INSERT ALL" + '\n'
                str_sql_footer = """select sysdate from dual"""
                fileObj = open(filePath, 'w+')
                updfileObj = open(updfilePath, 'w+')
                #self.objDBConnetion.fn_sql_file_append(fileObj, str_sql_header)
                # +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
                for item in final_list:
                    if item[2] == "0":
                        str1 = item[5]
                        str2 = item[6]
                        strMasterValue = ''

                        for row in str1.split(","):
                            col_nm = str(row.split("~")[0])
                            col_datatype = str(row.split("~")[1])
                            if col_nm in obj:
                                strMasterValue = strMasterValue + self.objDBConnetion.fn_col_val_gen(str(obj[col_nm]), col_datatype) + "|"
                            else:
                                strMasterValue = strMasterValue + "NULL" + "|"
                        self.objDBConnetion.fn_str_insert_records(str(item[4]), strMasterValue[:-1], str2, fileObj)

                    else:
                        #obj1 = json.loads(obj["payload"])
                        obj1 = obj
                        #obj1 = obj
                        strNodePath = item[8] + "$"
                        strList = item[5]
                        strtCollist = item[6]
                        sPath = str(item[4])
                        all_keys = item[13]
                        upd_fields = item[14]
                        entity_transfer_list = item[16]
                        #===========================================================
                        keyList = []

                        for key in all_keys.split("|"):
                                keyList.append("None")
                        #===========================================================
                        self.print_all_nodes(obj1, strNodePath, eventId, entityId, strList, sPath, strtCollist, fileObj, keyList, all_keys, upd_fields, updfileObj, full_metadata, main_obj, entity_transfer_list)
                # Add Footer part of SQL file
                self.objDBConnetion.fn_sql_file_append(fileObj, str_sql_footer)
                self.objDBConnetion.fn_sql_file_append(updfileObj, str_sql_footer)
                # SQL file close
                fileObj.close()
                updfileObj.close()

                print("Data publication is being started")
                # logger.info("Data publication is being started")
                # For JsonLogMsg
                log_message = "Data publication is being started"
                self.logger.info(self.objLog.fn_ProduceJsonLogMessage(domainName, eventId, log_message, module, cf.f_lineno, file_name, clas))

                # Run SQL file
                ins_cnt = len(open(filePath).readlines())
                # For JsonLogMsg - sql_footer won't be added in file if there is no sql statement
                if ins_cnt > 0 and open(filePath).read() != 'select sysdate from dual':
                    print("Insertion start from file.... ")
                    # For JsonLogMsg
                    log_message = "Data insertion from file is started"
                    self.logger.info(self.objLog.fn_ProduceJsonLogMessage(domainName, eventId, log_message, module, cf.f_lineno, file_name, clas))

                    sql_out = self.objDBConnetion.fn_run_sql_script(eventId, domainName, filePath, msg_offset, msg_partition)
                    print("Insert SQL Output : " + sql_out)
                    # For JsonLogMsg
                    log_message = "No. of Insert SQL Output = " + sql_out
                    self.logger.info(self.objLog.fn_ProduceJsonLogMessage(domainName, eventId, log_message, module, cf.f_lineno, file_name, clas))

                    #print("Sql Error Output" + sql_err)
                    #if str(sql_out).find("ERROR") == -1:
                    if str(sql_out) != "E":
                        print("Insert SQL Script run complete")
                        # For JsonLogMsg
                        log_message = "Insert SQL script execution is completed"
                        self.logger.info(self.objLog.fn_ProduceJsonLogMessage(domainName, eventId, log_message, module, cf.f_lineno, file_name, clas))

                        returnValue = int(sql_out)
                        # Run update sql file
                        upd_cnt = len(open(updfilePath).readlines())
                        if upd_cnt > 0:
                            print("Data update from update file has started")
                            # For JsonLogMsg
                            log_message = "Data update from file is started"
                            self.logger.info(self.objLog.fn_ProduceJsonLogMessage(domainName, eventId, log_message, module, cf.f_lineno, file_name, clas))

                            sql_err = self.objDBConnetion.fn_run_sql_script(eventId, domainName, updfilePath, msg_offset, msg_partition)
                            print("Update SQL Output : " + sql_err)
                            # For JsonLogMsg
                            log_message = "No. of Update SQL Output = " + sql_err
                            self.logger.info(self.objLog.fn_ProduceJsonLogMessage(domainName, eventId, log_message, module, cf.f_lineno, file_name, clas))

                            if str(sql_out) != "E":
                                print("Update SQL Script run complete")
                                # For JsonLogMsg
                                log_message = "Update SQL script execution is completed"
                                self.logger.info(self.objLog.fn_ProduceJsonLogMessage(domainName, eventId, log_message, module, cf.f_lineno, file_name, clas))
                            else:
                                print("Update SQL Script run has been failed")
                                # For JsonLogMsg
                                log_message = "Update SQL script execution is failed"
                                self.logger.info(self.objLog.fn_ProduceJsonLogMessage(domainName, eventId, log_message, module, cf.f_lineno, file_name, clas))
                            #print("Data Update from file has been completed successfully")
                        else:
                            print("Update Data file has no record to update")
                            # For JsonLogMsg
                            log_message = "Update data file has no record to update"
                            self.logger.info(self.objLog.fn_ProduceJsonLogMessage(domainName, eventId, log_message, module, cf.f_lineno, file_name, clas))
                    else:
                        print("Insert SQL Script run has been failed")
                        # For JsonLogMsg
                        log_message = "Insert SQL script execution is failed"
                        self.logger.info(self.objLog.fn_ProduceJsonLogMessage(domainName, eventId, log_message, module, cf.f_lineno, file_name, clas))

                        returnValue = -1

                    #print("Insertion complete from file.... ")
                else:
                    print("Insert Data file has no record to Insert")
                    # For JsonLogMsg
                    log_message = "Insert data file has no record to update"
                    self.logger.info(self.objLog.fn_ProduceJsonLogMessage(domainName, eventId, log_message, module, cf.f_lineno, file_name, clas))

                print("Data publication is completed")
                # logger.info("Data publication has been completed successfully")
                # For JsonLogMsg
                log_message = "Data publication is completed"
                self.logger.info(self.objLog.fn_ProduceJsonLogMessage(domainName, eventId, log_message, module, cf.f_lineno, file_name, clas))

                # SQL file deletion
                self.del_sql_file(filePath)
                self.del_sql_file(updfilePath)

            return returnValue

        except Exception as e:
            print(e)
            logging.error(traceback.format_exc())
            # Logs the error appropriately.

            return -1
