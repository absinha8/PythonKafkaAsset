import ConfigController
import LoggingController
import pandas as pd
import cx_Oracle
import traceback
import logging
import os

# For JsonLogMsg
import inspect
from inspect import currentframe, getframeinfo

class MetadataList:

    def __init__(self):
        print("welcome to Metadata class")

        # For JsonLogMsg
        self.objLog = LoggingController.LogObject()
        self.logger = self.objLog.get_logger()

    def get_path(self, strParent, list):
        
        """ This routine is intended to determine the path object in run time.
        and use that path ("|" separated) to traverse the JSON object.
        :param:strParent: parent level tag / label to identify parent of the child
        :param:list:json metadata / schema / data dictionary
        :return: path label in string format
        """
        try:
            strFinal = ''
            Parent_Nm = strParent.split("|")[0]
            Parent_fld = strParent.split("|")[1]

            for item in list:
                    if item[1] == Parent_Nm and item[15] == Parent_fld:
                        strFinal = item[8]
            return strFinal

        except Exception as e:
            print(e)
            logging.error(traceback.format_exc())
            # Logs the error appropriately.        
            return None

    def fn_createlist(self, myList):
        
        """ This routine is intended to create a list object 
        from the metadata / schema / data dictionary to traverse the JSON object accordingly.
        incorporate a runtime path in the metadata / schema per level in the given json
        :param:myList: list object representation of json metadata / schema / data dictionary
        """
        try:
            
            strFinalList = []
    
            lineCount = 0
            for row in myList:
                my_list = []
                my_list = row
                strFinalList.append(my_list)
                lineCount += 1
    
            count = 0
            for row in strFinalList:
                if count > 0:
                    if row[2] != "1" and row[2] != "0":
                        strPath = self.get_path(row[7], strFinalList)
                        #print(strPath)
                        row[8] = strPath + "|" + row[1]
                        #print(row[8])
    
                count = count + 1

            return strFinalList
        
        except Exception as e:
            print(e)
            logging.error(traceback.format_exc())
            # Logs the error appropriately.
            return None
        

    def fn_createmetadatalist(self):

        """ This routine is intended to create a 
        list object represents metadata
        from the pre-defined SQL script. Flexible to connect in 
        different database instacne based on the "Connection" attribute 
        of the configuration object utilizing DSN based connectivity.
        :param:None
        :return: List object represents metadata / data dictionary / schema object
        """        
        try:

            # For JsonLogMsg - commented as logger object defined in _init_
            # objLog = LoggingController.LogObject()
            # logger = objLog.get_logger()
            objSchemaPath = ConfigController.Configuration()
            #schema_path = objSchemaPath.getConfiguration("Path|schema_path")
            ip = objSchemaPath.getConfiguration("Connection|ip")
            port = objSchemaPath.getConfiguration("Connection|port")
            SERVICE_NAME = objSchemaPath.getConfiguration("Connection|SID")
            DB_ORA_USER = os.environ['DB_ORA_USER']
            DB_ORA_PWD = os.environ['DB_ORA_PWD']
    
            #dsnTns = cx_Oracle.makedsn(ip, port, SERVICE_NAME)
            #connection = cx_Oracle.connect(usr, pwd, dsnTns)
            connection = DB_ORA_USER + "/" + DB_ORA_PWD + "@" + ip + ":" + port + "/" + SERVICE_NAME

            #print(dsnTns)
            #print(connection)

            # For JsonLogMsg
            cf = currentframe()
            module = cf.f_code.co_name
            filename = cf.f_code.co_filename
            clas = self.__class__
            log_message = "Metadata list creation is started"
            self.logger.info(self.objLog.fn_ProduceJsonLogMessage(None, None, log_message, module, cf.f_lineno, filename, clas))
    
            sqlQuery = """SELECT TRIM(A.ENTITY_NAME) AS ENTITY_NAME,
              TRIM(UPPER(A.DOMAINTYPE)) AS DOMAINTYPE,
              TRIM(A.JSON_LEVEL) AS JSON_LEVEL,
              TRIM(A.DBTABLE) AS DBTABLE,
              TRIM(A.ATTRIBUTE_NAME) AS ATTRIBUTE_NAME,
              TRIM(A.TABLEFIELDS) AS TABLEFIELDS,
              TRIM(A.PARENT) AS PARENT,
              TRIM(A.JSON_PATH) AS JSON_PATH,
              A.ROOTENTRY,
              TRIM(DECODE(A.RI_NODE,NULL,NULL,A.ATTRIBUTE_NAME)) AS SRC_JSONTAG,
              TRIM(B.CURRENT_IND)                                AS IS_ACTIVE,
              TRIM(B.TABLE_NAME)                                 AS RI_DBTABLE,
              TRIM(B.COLUMN_NAME)                                AS RI_TABLEFIELDS,
              TRIM(A.KEY_FIELDS) AS KEY_FIELDS,
              TRIM(DECODE(C.KEY_MOVEUP_FEILDS,NULL,'NO',C.KEY_MOVEUP_FEILDS)) AS KEY_MOVEUP_FEILDS,
              A.ENTITY_ID,
              TRIM(A.ENTITY_TRANSFER) as ENTITY_TRANSFER 
            FROM
              (SELECT FIELD_ID,
                ENTITY_NAME,
                DOMAIN_NAME AS DOMAINTYPE,
                NODE_LEVEL  AS JSON_LEVEL,
                TABLE_NAME  AS DBTABLE,
                (ATTRIBUTE_NAME
                || '~'
                || LOGICAL_DATATYPE
                || '~'
                || DECODE(PARENT_COLUMN,NULL,'NO',PARENT_COLUMN)
                || '~'
                || TRIM(IS_KEY_GEN)
                || '~'
                || DECODE(FEILD_FOR_KEY,NULL,'NO',FEILD_FOR_KEY)
                || '~'
                || DECODE(KEY_TRANSFER,NULL,'NO',KEY_TRANSFER)
                || '~'
                || TRIM(IS_KEY_MOVEUP) 
                || '~'
                || DECODE(FIELD_FOR_ENTITY_TRANSFER,NULL,'NO',FIELD_FOR_ENTITY_TRANSFER)) AS ATTRIBUTE_NAME,
                COLUMN_NAME             AS TABLEFIELDS,
                PARENT_NODE             AS PARENT,
                NODE_PATH               AS JSON_PATH,
                ROOT_FLAG               AS ROOTENTRY,
                RI_NODE,
                DECODE(KEY_FIELDS,NULL,'NO',KEY_FIELDS) AS KEY_FIELDS, 
                DECODE(ENTITY_TRANSFER,NULL,'NO',ENTITY_TRANSFER) AS ENTITY_TRANSFER,
                CURRENT_IND,
                FIELD_ID AS ENTITY_ID 
              FROM T_EVENT_MAP
              ) A,
              (SELECT FIELD_ID,
                TABLE_NAME,
                COLUMN_NAME,
                CURRENT_IND
              FROM
                T_EVENT_MAP
              ) B,
              (SELECT KEYMOVE.FIELD_ID,
                (MAINMV.TABLE_NAME
                || '~'
                || KEYMOVE.UPD_COL
                || '~'
                || MAINMV.ATTRIBUTE_NAME
                || '~'
                || MAINMV.COLUMN_NAME) AS KEY_MOVEUP_FEILDS
              FROM
                (SELECT FIELD_ID,
                  KEY_MOVEUP_FEILDS,
                  SUBSTR(KEY_MOVEUP_FEILDS, 1, Instr(KEY_MOVEUP_FEILDS, '|', -1, 1) -1) UPD_COL,
                  SUBSTR(KEY_MOVEUP_FEILDS, Instr(KEY_MOVEUP_FEILDS, '|',    -1, 1) +1) UPD_REF_FIELD_ID
                FROM 
                T_EVENT_MAP
                WHERE KEY_MOVEUP_FEILDS IS NOT NULL
                ) KEYMOVE ,
                (SELECT FIELD_ID,
                TABLE_NAME,
                ATTRIBUTE_NAME,
                COLUMN_NAME 
                FROM 
                T_EVENT_MAP
                ) MAINMV
              WHERE KEYMOVE.UPD_REF_FIELD_ID = MAINMV.FIELD_ID
              ) C
            WHERE A.RI_NODE = B.FIELD_ID(+)
            AND A.FIELD_ID  = C.FIELD_ID(+) 
            AND A.CURRENT_IND = 'Y'
            ORDER BY A.FIELD_ID"""
            #AND A.DOMAINTYPE = 'fund'
            #print(sql_query)

            connect = cx_Oracle.connect(connection)
            #cursor = connect.cursor()
            #cursor.execute(sqlQuery)
            #dframe = pd.DataFrame(cursor.fetchall())
            #records = cursor.fetchall()
            #cursor.close()
            #connect.close()
    
            #dframe = pd.read_sql(sqlQuery, con=connection)
            dframe = pd.read_sql_query(sqlQuery, connect)
            #print(dframe)
            connect.close()
    
            finalList = []
            for index, row in dframe.iterrows():

                if row["ROOTENTRY"] == 1:
                    myList = list()
                    myList.append(str(row["DOMAINTYPE"]))
                    #myList.append(str(row["OBJECT"]))
                    myList.append(str(row["ENTITY_NAME"]))
                    myList.append(str(row["JSON_LEVEL"]))
                    myList.append(str(row["DBTABLE"]) + "_array")
                    #myList.append(str(row["DBTABLE"]))

                    strFields = ""
                    strTableFields = ""
                    strRIJsonTAG = ""
                    strRITable = ""
                    strRIFieldName = ""
                    strTableName = ""
                    #newFrame = dframe.loc[dframe["ENTITY_NAME"] == row["ENTITY_NAME"]]

                    #newFrame = dframe.loc[(dframe["ENTITY_NAME"] == row["ENTITY_NAME"]) & (dframe["PARENT"] == Rowparent) & (dframe["DOMAINTYPE"] == row["DOMAINTYPE"]) & (dframe["DBTABLE"] == row["DBTABLE"])]
                    newFrame = dframe.loc[(dframe["ENTITY_NAME"] == row["ENTITY_NAME"]) & (dframe["PARENT"] == row["PARENT"]) & (dframe["DOMAINTYPE"] == row["DOMAINTYPE"]) & (dframe["DBTABLE"] == row["DBTABLE"])]
                    for index1, row1 in newFrame.iterrows():
                        strTableName = str(row1["DBTABLE"])
                        strFields = strFields + str(row1["ATTRIBUTE_NAME"]) + ","
                        strTableFields = strTableFields + str(row1["TABLEFIELDS"]) + ","
                        if row1["SRC_JSONTAG"] != None :
                            strRIJsonTAG =  str(row1["SRC_JSONTAG"])
                            strRITable = str(row1["RI_DBTABLE"])
                            strRIFieldName = str(row1["RI_TABLEFIELDS"])

                    myList.append(strTableName)
                    myList.append(strFields[:-1])
                    myList.append(strTableFields[:-1])
                    myList.append(str(row["PARENT"]))
                    myList.append(str(row["JSON_PATH"]))
                    myList.append(strRIJsonTAG)
                    myList.append(str(row["IS_ACTIVE"]))
                    myList.append(strRITable)
                    myList.append(strRIFieldName)
                    myList.append(str(row["KEY_FIELDS"]))
                    myList.append(str(row["KEY_MOVEUP_FEILDS"]))
                    myList.append(str(row["ENTITY_ID"]))
                    myList.append(str(row["ENTITY_TRANSFER"]))

                    finalList.append(myList)
    
            #print(finalList)
            finalMetaDatalist = self.fn_createlist(finalList)
            #print(finalMetaDatalist)

            # logger.info("Successfully MetaData has been created")
            # For JsonLogMsg
            log_message = "Metadata list creation is completed"
            self.logger.info(self.objLog.fn_ProduceJsonLogMessage(None, None, log_message, module, cf.f_lineno, filename, clas))

            return finalMetaDatalist

        except Exception as e:
            print(e)
            logging.error(traceback.format_exc())
            # Logs the error appropriately.
