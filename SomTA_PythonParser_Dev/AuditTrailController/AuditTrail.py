import ConfigController
import DBConnectionController
import traceback
import logging
import datetime

class AuditTrail:

    def __init__(self):
        #print("welcome to AuditTrail class")
        objConfig = ConfigController.Configuration()
        self.Aud_Tab = objConfig.getConfiguration("Audit|Aud_Tab")
        self.Processing_Unit = objConfig.getConfiguration("Audit|Processing_Unit")
        self.objDBConnetionAd = DBConnectionController.DBConnection()
        self.Topic_Nm = objConfig.getConfiguration("Consumer|topic")

    def insert_records(self, tableName, insertStr, insertCol):

        """This routine is intended to
        insert records in the database table
        :param connStr: the Connection object
        :param tableName: target table names
        :param insertStr: values to be inserted to the target table_name
        :return: None
        """

        try:
            val = []
            insertVal = insertStr.split(",")

            for i in range(len(insertVal)):
                if i == 6:
                    val.append("TO_NUMBER('" + str(insertVal[i]) + "')")
                else:
                    val.append("'" + str(insertVal[i]) + "'")

            val1 = ",".join(val)
            # modifying insert string as a parameterised query
            insertInto = "INSERT INTO {} ({}) VALUES ({}) ".format(tableName, insertCol, val1)
            #insertInto = "INSERT INTO " + " " + tableName + " " + "(" + insertCol + ")" + " " + "VALUES (" + ",".join(
            #    val) + ")"
            print(insertInto)
            self.objDBConnetionAd.fn_db_insert(insertInto)

        except Exception as e:
            print(e)
            logging.error(traceback.format_exc())
            # Logs the error appropriately.

        return None

    def join(self, lst, s):
        """
        join a list object with a given operator
        :param: None
        :return: List object
        """
        return s.join(lst)

    # Audit_Trail Insert Routine for AuditTrailController
    def fn_AuditTrailInsertProcess(self, eventId, domainName, status, tab, insCnt, msg_offset, msg_partition):

        """This routine is intended to
        insert records into database table. Target table name is
        given through the argument "tab".
        :param connObj: connection string given
        :param tab: source table name / Audit Trail
        :param jsonDicObj: given JSON object
        :return: None
        :Advancement: None as of now 28 Jan 2019
        """
        try:

            # extract elemenatry outer attributes of json from given directory structure
            eventId = str(eventId)
            domainName = str(domainName)

            job_id = self.Processing_Unit + '_' + domainName

            if tab == 'S':
                proc_status = "Process Started"
            else:
                if tab == 'R':
                    proc_status = "Process Rejected"
                else:
                    if tab == 'F':
                        proc_status = "Process Finished"
                    else:
                        proc_status = "Finished Abnormally"

            result = str(status)
            topic_nm = str(self.Topic_Nm)

            # create insert list for audit trail table :NTDM_AUDIT.DS_AUDIT_TAB
            # immutable list will be changed for 'dup' argument based on whether duplicate event
            insertList = [eventId, job_id, topic_nm, domainName, proc_status, result, insCnt, msg_partition, msg_offset]
            insertColumnList = ['EVNT_ID', 'JOB_ID', 'TOPIC_NM', 'DOMAIN', 'STAT', 'RSLT', 'REC_PROS', 'PARTITION_NM', 'OFFSET']
            # print(insertList)
            insertStr = self.join(insertList, ',')
            # print(insertStr)
            insertColStr = self.join(insertColumnList, ',')
            # print(insertColStr)
            self.insert_records(self.Aud_Tab, insertStr, insertColStr)


        except Exception as e:
            print(e)
            logging.error(traceback.format_exc())
            # Logs the error appropriately.

        return None
