import DBConnectionController
import traceback
import logging
import datetime


class PublishDB:

    def __init__(self):
        #print("welcome to PublishDB class")
        self.objDBConnetion = DBConnectionController.DBConnection()
        self.TimestampFormat = "RRRR-MM-DD HH24:MI:SS.FF"

    def find_json_value(self, obj, nodepath, json_attribute):
        """ this routine is intended to find out the value for the json attribute name.
        This attribute can come from any entity of any level throughout the json"""

        try:
            item = nodepath.split("|")
            new_items = item[1:]
            remaining_path = ''

            for i in new_items:
                remaining_path = remaining_path + i + "|"

            if item[0].find('$') > -1:
                str1 = item[0]

                attribute_val = ''

                if str(str1[:-1]) in obj:
                    obj1 = obj[str(str1[:-1])]
                    if isinstance(obj1, dict):
                        if json_attribute in obj1:
                            attribute_val = str(obj1[json_attribute])
                            return attribute_val
                    elif isinstance(obj1, list):
                        for item in obj1:
                            attribute_val = str(item[json_attribute])
                        return attribute_val
                    else:
                        return

            else:
                if str(item[0]) in obj:
                    obj1 = obj[str(item[0])]
                    if isinstance(obj1, dict):
                        return self.find_json_value(obj1, remaining_path[:-1], json_attribute)
                    elif isinstance(obj1, list):
                        for item in obj1:
                            return self.find_json_value(item, remaining_path[:-1], json_attribute)
                    else:
                        return

        except Exception as e:
            print(e)
            logging.error(traceback.format_exc())
            # Logs the error appropriately.
        return None

    def fn_find_value_for_entity_transfer(self, json_obj, final_metadata_list, entity_transfer_list, col_for_entity_transfer):
        """ this routine is intended to find out the value for the json attribute name.
        This attribute can come from any entity of any level throughout the json"""

        jsonvalue = ''

        try:
            index = int(col_for_entity_transfer[-1])

            entity_transfer_detail_list = []

            for item in entity_transfer_list.split("|"):
                entity_transfer_detail_list.append(item)

            entity_transfer_key = entity_transfer_detail_list[index]

            entity_name = str(entity_transfer_key.split("_")[0])
            json_atrribute_name = str(entity_transfer_key.split("_")[1])
            metadata_field_id = str(entity_transfer_key.split("_")[2])
            ishash = str(entity_transfer_key.split("_")[3])

            metadata_nodepath = ''
            for item1 in final_metadata_list:
                if item1[15] == metadata_field_id and item1[1] == entity_name:
                    metadata_nodepath = item1[8] + '$'

            jsonvalue = self.find_json_value(json_obj, metadata_nodepath, json_atrribute_name)

            if ishash == 'T':
                jsonvalue = str(hash(jsonvalue))

            return jsonvalue

        except Exception as e:
            print(e)
            logging.error(traceback.format_exc())
            # Logs the error appropriately.
        return jsonvalue

    def fn_json_wr_dict(self, eventId, entityId, dataVal, colList, sPath, strtCollist, fileObj, parentObj, keyList, upd_fields, updfileObj, full_metadata, main_obj, entity_transfer_list):

        """ This routine is intended to create comma separated file (.csv extension)
        from dictionary object.
        :param:eventId: given eventId in the JSON object
        :param:entityId: given entityId in the JSON object
        :param:dataVal: given python dictionary object
        :param:colList: given json fields in the JSON object
        :param:sPath: given "Table" attribute in the JSON object
        :param:strtCollist: given table column names in the JSON object
        :return: string object
        """
        try:
            strDictValue = ''

            for item in colList.split(","):
                col_nm = str(item.split("~")[0])
                col_datatype = str(item.split("~")[1])
                col_parent = str(item.split("~")[2])
                col_key_gen = str(item.split("~")[3])
                col_for_key = str(item.split("~")[4])
                col_key_tran = str(item.split("~")[5])
                col_key_moveup = str(item.split("~")[6])
                col_for_entity_transfer = str(item.split("~")[7])

                if col_nm == "eventId":
                    strDictValue = strDictValue + "'" + eventId + "'" + "|"
                elif col_nm == "entityId":
                    strDictValue = strDictValue + "'" + entityId + "'" + "|"
                elif col_nm == "LD_DT_TM":
                    strDictValue = strDictValue + "TO_TIMESTAMP('" + str(datetime.datetime.now()) + "', '" + self.TimestampFormat + "')" + "|"
                elif col_key_gen == "T" and col_key_moveup != "T":
                    new_key = ''
                    col_for_key_new = ''
                    if col_key_tran != 'NO':
                        for key in col_key_tran.split("|"):
                            index = key[3:]
                            new_key = new_key + str(keyList[int(index)]) + "-"
                    if col_for_key != 'NO':
                        for key_col in col_for_key.split("|"):
                            if key_col in dataVal:
                                col_for_key_new = col_for_key_new + str(dataVal[key_col]) + "-"

                    new_key1 = new_key + col_for_key_new
                    new_key1 = new_key1[:-1]
                    strDictValue = strDictValue + self.objDBConnetion.fn_col_val_gen(str(hash(new_key1)), col_datatype) + "|"

                elif col_key_gen == "T" and col_key_moveup == "T":
                    new_key = ''
                    col_for_key_new = ''
                    if col_key_tran != 'NO':
                        for key in col_key_tran.split("|"):
                            index = key[3:]
                            new_key = new_key + str(keyList[int(index)]) + "-"
                    if col_for_key != 'NO':
                        for key_col in col_for_key.split("|"):
                            if key_col in dataVal:
                                col_for_key_new = col_for_key_new + str(dataVal[key_col]) + "-"
                    new_key1 = new_key + col_for_key_new
                    new_key1 = new_key1[:-1]
                    strDictValue = strDictValue + self.objDBConnetion.fn_col_val_gen(str(hash(new_key1)), col_datatype) + "|"
                    updkeyval = self.objDBConnetion.fn_col_val_gen(str(hash(new_key1)), col_datatype)
                    self.objDBConnetion.fn_str_update_records(eventId, updkeyval, parentObj, upd_fields, updfileObj)
                elif col_key_gen == "F" and col_key_tran != 'NO':
                    index = col_key_tran[3:]
                    new_key = str(keyList[int(index)])
                    strDictValue = strDictValue + self.objDBConnetion.fn_col_val_gen(new_key, col_datatype) + "|"
                elif col_for_entity_transfer != 'NO':
                    value_for_entity_transfer_col = self.fn_find_value_for_entity_transfer(main_obj, full_metadata, entity_transfer_list, col_for_entity_transfer)
                    strDictValue = strDictValue + self.objDBConnetion.fn_col_val_gen(str(value_for_entity_transfer_col), col_datatype) + "|"
                else:
                    if col_nm in dataVal:
                        if col_parent == 'NO':
                            strDictValue = strDictValue + self.objDBConnetion.fn_col_val_gen(str(dataVal[col_nm]), col_datatype) + "|"
                        else:
                            if col_parent in parentObj:
                                strDictValue = strDictValue + self.objDBConnetion.fn_col_val_gen(str(parentObj[col_parent]), col_datatype) + "|"
                            else:
                                strDictValue = strDictValue + "NULL" + "|"
                    else:
                        if col_parent != 'NO':
                            if col_parent in parentObj:
                                strDictValue = strDictValue + self.objDBConnetion.fn_col_val_gen(str(parentObj[col_parent]), col_datatype) + "|"
                            else:
                                strDictValue = strDictValue + "NULL" + "|"
                        else:
                            strDictValue = strDictValue + "NULL" + "|"
    
            strDictValue = strDictValue[:-1]
            # Insert record to .sql file
            self.objDBConnetion.fn_str_insert_records(sPath, strDictValue, strtCollist, fileObj)

        except Exception as e:
            print(e)
            logging.error(traceback.format_exc())
            # Logs the error appropriately.

        return None

        

    def fn_json_wr_list(self,eventId, entityId, dataVal, colList, sPath,strtCollist, fileObj, parentObj, keyList, full_metadata, main_obj, entity_transfer_list):
        
        """ This routine is intended to create comma separated file (.csv extension)
        from list object.
        :param:eventId: given eventId in the JSON object
        :param:entityId: given entityId in the JSON object
        :param:dataVal: given python dictionary object
        :param:colList: given json fields in the JSON object
        :param:sPath: given "Table" attribute in the JSON object
        :param:strtCollist: given table column names in the JSON object
        :return: string object
        """        
        
        try:
            for raw1 in dataVal:
                strListValue = ''
                for item in colList.split(","):
                    col_nm = str(item.split("~")[0])
                    col_datatype = str(item.split("~")[1])
                    col_parent = str(item.split("~")[2])
                    col_key_gen = str(item.split("~")[3])
                    col_for_key = str(item.split("~")[4])
                    col_key_tran = str(item.split("~")[5])
                    col_for_entity_transfer = str(item.split("~")[7])

                    if col_nm == "eventId":
                        strListValue = strListValue + "'" + eventId + "'" + "|"
                    elif col_nm == "entityId":
                        strListValue = strListValue + "'" + entityId + "'" + "|"
                    elif col_nm == "LD_DT_TM":
                        strListValue = strListValue + "TO_TIMESTAMP('" + str(datetime.datetime.now()) + "', '" + self.TimestampFormat + "')" + "|"
                    elif col_key_gen == "T":
                        new_key = ''
                        col_for_key_new = ''
                        if col_key_tran != 'NO':
                            for key in col_key_tran.split("|"):
                                index = key[3:]
                                new_key = new_key + str(keyList[int(index)]) + "-"
                        if col_for_key != 'NO':
                            for key_col in col_for_key.split("|"):
                                if key_col in raw1:
                                    col_for_key_new = col_for_key_new + str(raw1[key_col]) + "-"
                        new_key1 = new_key + col_for_key_new
                        new_key1 = new_key1[:-1]
                        strListValue = strListValue + self.objDBConnetion.fn_col_val_gen(str(hash(new_key1)), col_datatype) + "|"
                    elif col_key_gen == "F" and col_key_tran != 'NO':
                        index = col_key_tran[3:]
                        new_key = str(keyList[int(index)])
                        strListValue = strListValue + self.objDBConnetion.fn_col_val_gen(new_key, col_datatype) + "|"
                    elif col_for_entity_transfer != 'NO':
                        value_for_entity_transfer_col = self.fn_find_value_for_entity_transfer(main_obj, full_metadata, entity_transfer_list, col_for_entity_transfer)
                        strListValue = strListValue + self.objDBConnetion.fn_col_val_gen(value_for_entity_transfer_col, col_datatype) + "|"
                    else:
                        if col_nm in raw1:
                            if col_parent == 'NO':
                                strListValue = strListValue + self.objDBConnetion.fn_col_val_gen(str(raw1[col_nm]), col_datatype) + "|"
                            else:
                                if col_parent in parentObj:
                                    strListValue = strListValue + self.objDBConnetion.fn_col_val_gen(str(parentObj[col_parent]), col_datatype) + "|"
                                else:
                                    strListValue = strListValue + "NULL" + "|"
                        else:
                            if col_parent != 'NO':
                                if col_parent in parentObj:
                                    strListValue = strListValue + self.objDBConnetion.fn_col_val_gen(str(parentObj[col_parent]), col_datatype) + "|"
                                else:
                                    strListValue = strListValue + "NULL" + "|"
                            else:
                                strListValue = strListValue + "NULL" + "|"
    
                strListValue = strListValue[:-1]
                # Insert record to .sql file
                self.objDBConnetion.fn_str_insert_records(sPath, strListValue, strtCollist, fileObj)

        except Exception as e:
            print(e)
            logging.error(traceback.format_exc())
            # Logs the error appropriately.

        return None
        

    def fn_json_wr_single(self, eventId, entityId, dataVal, colList, sPath, strtCollist, fileObj):
        
        """ This routine is intended to create comma separated file (.csv extension)
        :param:eventId: given eventId in the JSON object
        :param:entityId: given entityId in the JSON object
        :param:dataVal: given python dictionary object
        :param:colList: given json fields in the JSON object
        :param:sPath: given "Table" attribute in the JSON object
        :param:strtCollist: given table column names in the JSON object
        :return: None
        """         
        try:
            strSingleValue = ''
            for item in colList.split(","):
                col_nm = str(item.split("~")[0])
                col_datatype = str(item.split("~")[1])
                if col_nm == "eventId":
                    strSingleValue = strSingleValue + "'" + eventId + "'" + "|"
                elif col_nm == "entityId":
                    strSingleValue = strSingleValue + entityId + "|"
                else:
                    strSingleValue = strSingleValue + dataVal + "|"
    
            strSingleValue = strSingleValue[:-1]
            # Insert record to .sql file
            self.objDBConnetion.fn_str_insert_records(sPath, strSingleValue, strtCollist, fileObj)

        except Exception as e:
            print(e)
            logging.error(traceback.format_exc())
            # Logs the error appropriately.

        return None
