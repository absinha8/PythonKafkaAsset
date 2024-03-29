import ConfigController
import AuditTrailController
import cx_Oracle
import traceback
import logging
import os
from datetime import datetime
#from subprocess import Popen, PIPE

class DBConnection:
    # Self Declaration
    def __init__(self):
        #print("welcome to DB-Connection class")
        objSchemaPath = ConfigController.Configuration()
        self.ip = objSchemaPath.getConfiguration("Connection|ip")
        self.port = objSchemaPath.getConfiguration("Connection|port")
        self.SERVICE_NAME = objSchemaPath.getConfiguration("Connection|SID")
        self.BASE_SQL_DIR = objSchemaPath.getConfiguration("SQL|sql_path")
        self.DB_ORA_USER = os.environ['DB_ORA_USER']
        self.DB_ORA_PWD = os.environ['DB_ORA_PWD']
        nt_ora_conn = self.DB_ORA_USER + "/" + self.DB_ORA_PWD + "@" + self.ip + ":" + self.port + "/" + self.SERVICE_NAME
        self.conn = nt_ora_conn

    def fn_format_json_timestamp(self, json_timestamp, type):
        if json_timestamp.find('.') > -1:
            ip_timestamp = datetime.strptime(json_timestamp, "%Y-%m-%dT%H:%M:%S.%fZ")

            if type == 'DATE':
                op_timestamp = ip_timestamp.strftime("%Y-%m-%d")
            else:
                op_timestamp = ip_timestamp.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

            return op_timestamp
        else:
            ip_timestamp = datetime.strptime(json_timestamp, "%Y-%m-%dT%H:%M:%SZ")

            if type == 'DATE':
                op_timestamp = ip_timestamp.strftime("%Y-%m-%d")
            else:
                op_timestamp = ip_timestamp.strftime("%Y-%m-%d %H:%M:%S")

            return op_timestamp



    # function that takes the sqlCommand and connectString and returns the queryReslut and errorMessage (if any)
    #def fn_del_run_sql_script(self, filePath):
    #    #try:
    #    conn_str = self.conn
    #    sqlplus = Popen(['sqlplus', '-S', conn_str], stdin=PIPE, stdout=PIPE, stderr=PIPE)
    #    sqlplus.stdin.write('@' + filePath)
    #    return sqlplus.communicate()

    # function that takes the sqlCommand and connectString and returns the query result and errorMessage (if any)
    def fn_run_sql_script(self, eventId, domainName, filePath, msg_offset, msg_partition):
        objAuditController = AuditTrailController.AuditTrail()
        #dbreturn = 'F'
        safeDir = self.BASE_SQL_DIR
        if os.path.commonprefix([filePath, safeDir]) != safeDir:
            print("Bad request")
            dbreturn = 'E'
        else:
            dbINConn = self.create_connection(self.conn)
            dbINConn.autocommit = False
            cursor = dbINConn.cursor()
            Sqlfl = open(filePath)
            full_sql = Sqlfl.read()
            #full_sql_new = full_sql[:-1]
            sql_commands = full_sql.split('|')
            InsCnt = len(sql_commands)
            len_error = 0
            #Loop start
            for sql_command in sql_commands:
                try:
                    cursor.execute(sql_command)
                except Exception as e:
                    print(e)
                    print("Error sql :" + sql_command)
                    len_error = len(str(e))
                    #logging.error(traceback.format_exc())
                    End_st_f0 = "Data Insertion Fail. Please check the Log file" + '\n' + "Error : " + str(e)
                    End_tab_f0 = 'R'
                    objAuditController.fn_AuditTrailInsertProcess(eventId, domainName, End_st_f0, End_tab_f0, '0', msg_offset, msg_partition)
                    # Logs the error appropriately.
                    break

            print(len_error)
            if (len_error) > 0:
                dbINConn.rollback
                dbreturn = 'E'
            else:
                dbINConn.commit()
                dbreturn = str(InsCnt - 1)

            self.close_connection(dbINConn, cursor)

        return dbreturn


    # SQL Query addition in .sql file
    def fn_sql_file_append(self, fileObj, str_query):
        try:
            print(str_query)
            fileObj.write(str_query)
        except Exception as e:
            print(e)
            logging.error(traceback.format_exc())
            # Logs the error appropriately.

        return None

    # Create column value based on oracle data type for Insert query
    def fn_col_val_gen(self, coloumn_value, coloumn_datatype):

        """ This function is intended to create column value based on column data type
        :param:col_val: column value
        :param:col_datatype: Data type of the column(Oracle)
        :return: string object
        """
        date_format = 'YYYY-MM-DD'
        Timestamp_Format = "RRRR-MM-DD HH24:MI:SS.FF"
        coloumn_value_final = ''

        if coloumn_value == "NULL":
            coloumn_value_final = 'NULL'
        elif coloumn_value == "None":
            coloumn_value_final = 'NULL'
        elif coloumn_value == "Null":
            coloumn_value_final = 'NULL'
        elif coloumn_value == "null":
            coloumn_value_final = 'NULL'
        elif str.upper(coloumn_value) == str("NULL"):
            coloumn_value_final = 'NULL'
        elif coloumn_value == '':
            coloumn_value_final = 'NULL'
        elif coloumn_value == "":
            coloumn_value_final = 'NULL'
        elif coloumn_value == ' ':
            coloumn_value_final = 'NULL'
        elif coloumn_value == " ":
            coloumn_value_final = 'NULL'
        elif len(coloumn_value) == 0:
            coloumn_value_final = 'NULL'
        else:
            if coloumn_datatype == "VARCHAR2":
                coloumn_value_final = "q'[" + coloumn_value + "]'"
            elif coloumn_datatype == "CHAR":
                if str.upper(coloumn_value) == "TRUE" or coloumn_value == "T" or str.upper(coloumn_value) == "YES" or coloumn_value == "Y":
                    Col_val = "Y"
                elif str.upper(coloumn_value) == "FALSE" or coloumn_value == "F" or str.upper(coloumn_value) == "NO" or coloumn_value == "N":
                    Col_val = "N"
                else:
                    Col_val = coloumn_value
                coloumn_value_final = "q'[" + Col_val + "]'"
            elif coloumn_datatype == "CLOB":
                coloumn_value_final = "q'[" + coloumn_value + "]'"
            elif coloumn_datatype == "NUMBER":
                #print(coloumn_value)
                if str(coloumn_value).find("e") > -1 or str(coloumn_value).find("E") > -1:
                    col_val = "{:.2f}".format(float(coloumn_value))
                    coloumn_value_final = "TO_NUMBER('" + str(col_val) + "')"
                else:
                    coloumn_value_final = "TO_NUMBER('" + coloumn_value + "')"
            elif coloumn_datatype == "DATE":
                coloumn_value_final = "TO_DATE('" + self.fn_format_json_timestamp(coloumn_value, 'DATE') + "', '" + date_format + "')"
            elif coloumn_datatype == "TIMESTAMP":
                coloumn_value_final = "TO_TIMESTAMP('" + self.fn_format_json_timestamp(coloumn_value, 'TIMESTAMP') + "', '" + Timestamp_Format + "')"
            else:
                print("Wrong column Data Type mentioned in metadata.")

        return coloumn_value_final

    # Create Insert statement for adding in .sql file
    def fn_str_insert_records(self, tableName, insertStr, strtCollist, fileObj):
        """ This routine is intended to
        insert records in the database table
        :param tableName: target table name
        :param insertStr: values to be inserted (as comma separated string input) to the target table_name
        :param strtCollist: Column List of the target database table.
        :return:None
        """
        try:
            ins_header = 'BEGIN'
            ins_footer1 = 'EXCEPTION WHEN DUP_VAL_ON_INDEX THEN NULL;'
            ins_footer2 = 'END;'

            val = ""
            ins_flag = 'F'

            val_cnt = int(int(insertStr.count("|")) - 1)
            val_chk = insertStr.split("|")
            for i in range(len(val_chk)):
                if i < val_cnt:
                    if len(str(val_chk[i])) == 0:
                        ins_flag = "T"
                    elif str(val_chk[i]).strip() == "None":
                        ins_flag = "T"
                    elif str(val_chk[i]) == "None ":
                        ins_flag = "T"
                    elif str(val_chk[i]) == "NULL":
                        ins_flag = "T"
                    else:
                        ins_flag = "F"
                        break

            #print(ins_flag)
            if ins_flag == "F":
                str_insertVal = insertStr.split("|")
                for i in range(len(str_insertVal)):
                    if len(str(str_insertVal[i])) == 0:
                        val = val + "NULL,"
                    elif str(str_insertVal[i]) == "None":
                        val = val + "NULL,"
                    elif str(str_insertVal[i]) == "None ":
                        val = val + "NULL,"
                    elif str(str_insertVal[i]).strip() == "None":
                        val = val + "NULL,"
                    elif str(str_insertVal[i]) == "NULL":
                        val = val + "NULL,"
                    else:
                        val = val + str_insertVal[i] + ","

                val = val[:-1]
                # modifying insert string as a parameterised query
                #str_insertIn = "INSERT INTO " + tableName + "(" + strtCollist + ")" + " VALUES (" + val + ")" + ';\n'
                str_insertIn = "INSERT INTO {} ({}) VALUES ({}) ".format(tableName, strtCollist, val) + ';\n'

                str_insertInto = '\n' + ins_header + '\n' + str_insertIn + ins_footer1 + '\n' + ins_footer2 + '|\n'

                self.fn_sql_file_append(fileObj, str_insertInto)
            else:
                print("All values fetched as NULL for the table " + tableName)

        except Exception as e:
            print(e)
            logging.error(traceback.format_exc())
            # Logs the error appropriately.

        return None

    def fn_str_update_records(self, eventId, updkeyval, parentObj, upd_fields, updfileObj):
        """ This routine is intended to
        insert records in the database table
        :param tableName: target table name
        :param insertStr: values to be inserted (as comma separated string input) to the target table_name
        :param strtCollist: Column List of the target database table.
        :return:None
        """
        try:
            upd_Table_Nm = str(upd_fields.split("~")[0])
            upd_col_Nm = str(upd_fields.split("~")[1])
            parent_attr = str(upd_fields.split("~")[2])
            parent_col = str(upd_fields.split("~")[3])

            if parent_attr in parentObj:
                parent_attr_val = str(parentObj[parent_attr])

                #str_where = " WHERE BN_EVENT_ID = " + "'" + eventId + "'" + " AND " + parent_col + " = " + "'" + parent_attr_val + "'"
                #str_update = "UPDATE " + upd_Table_Nm + " SET " + upd_col_Nm + " = " + updkeyval + str_where + "|\n"

                # modifying update string as a parameterised query
                str_update = "UPDATE {} SET {} = {} WHERE BN_EVENT_ID = '{}' and {} = '{}'".format(upd_Table_Nm, upd_col_Nm, updkeyval, eventId, parent_col, parent_attr_val) + "|\n"

                self.fn_sql_file_append(updfileObj, str_update)

        except Exception as e:
            print(e)
            logging.error(traceback.format_exc())
            # Logs the error appropriately.

        return None

    # Create Connection
    def create_connection(self, connStr):
        
        """ This routine is intended to create a database connection 
        to the Oracle database specified by the connection string
        :param connstr: connection string
        :return: None
        """
        try:
            conn = cx_Oracle.connect(connStr)
            return conn
        
        except cx_Oracle.Error as e:
            print(e)
            #return conn

    # Close open Connection
    def close_connection(self, conn, cur):

        """ This routine is intended to close a database connection
            specified by the conn
        :param conn: connection string
        :param cur: cursor object
        :return: None
        """
        try:
            cur.close()
            conn.close()
        except conn.Error as e:
            print(e)

        return None

    def fn_db_insert(self, strInsert):

        """ This routine is intended to insert records into database table
        by executing all the queries run at a time
        :param:connStr: connection string given
        :param:strInsert: given insert string
        :return: None
        """
        try:
            dbConn = self.create_connection(self.conn)
            cur = dbConn.cursor()
            cur.execute(strInsert)
            dbConn.commit()
            self.close_connection(dbConn, cur)

        except Exception as e:
            print(e)
            logging.error(traceback.format_exc())
            # Logs the error appropriately.

        return None

    def fn_fetch_record(self, strSelect):

        """ This routine is intended to insert records into database table
        by executing all the queries run at a time
        :param:connStr: connection string given
        :param:strInsert: given insert string
        :return: None
        """
        try:
            connect = self.create_connection(self.conn)
            cursor = connect.cursor()
            cursor.execute(strSelect)
            records = cursor.fetchall()
            self.close_connection(connect, cursor)
            return records

        except Exception as e:
            print(e)
            logging.error(traceback.format_exc())
            # Logs the error appropriately.
            return None
