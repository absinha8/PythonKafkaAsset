import time
import MetadataController
import DataController
import LoggingController
import ValidationController
import ConfigController
import ProducerController
import ConsumerController
import RIValidationController
import AuditTrailController
import ExceptionController
import pandas as pd
from datetime import date
import copy

# For JsonLogMsg
import inspect
from inspect import currentframe, getframeinfo


def main():

    objLog = LoggingController.LogObject()
    logger = objLog.get_logger()

    try:
        objConfig = ConfigController.Configuration()
        #topic_for_consume = 'FinalTesting'
        #topic_for_produce = 'demo_fund_event'
        #topic_for_sup_produce = 'demo_fund_event'
        topic_for_consume = objConfig.getConfiguration("Consumer|topic")
        topic_for_produce = objConfig.getConfiguration("Producer|topic")
        topic_for_sup_produce = objConfig.getConfiguration("Producer|Suspense_topic")
        #topic_for_consume = str(sys.argv[1])
        #topic_for_produce = str(sys.argv[2])
        #topic_for_sup_produce = str(sys.argv[3])

        # For JsonLogMsg
        cf = currentframe()
        module = cf.f_code.co_name
        filename = cf.f_code.co_filename

        localtime = time.asctime(time.localtime(time.time()))
        print("program starts executing at:"+localtime)

        #logger.info("program starts executing at:"+localtime)
        # For JsonLogMsg
        log_message = "Program started executing"
        logger.info(objLog.fn_ProduceJsonLogMessage(None, None, log_message, module, cf.f_lineno, filename, None))

        objMetadata = MetadataController.MetadataList()
        final_list_all = objMetadata.fn_createmetadatalist()
        objExcepMetaData = ExceptionController.ExceptionController()
        excep_list_all = objExcepMetaData.fn_createexceptionlist()

        if len(final_list_all) > 0:
            #print(final_list_all)
            objConsumer = ConsumerController.ConsumeData()
            consumer = objConsumer.fn_consume(topic_for_consume)

            for message in consumer:
                print("Final iteration starts executing at:" + time.asctime(time.localtime(time.time())))

                # For JsonLogMsg
                log_message = "Final iteration started executing"
                logger.info(objLog.fn_ProduceJsonLogMessage(None, None, log_message, module, cf.f_lineno, filename, None))

                msg_offset = str(message.offset)
                msg_partition = str(message.partition)

                #print("Offset value : " + msg_offset)
                #print("Partition : " + msg_partition)
                obj = message.value
                objAuditController = AuditTrailController.AuditTrail()
                if isinstance(obj, dict):
                    #print(obj)
                    finalObj = copy.deepcopy(obj)
                    if 'eventId' in obj:
                        eventId = str(obj["eventId"])
                        print("Processing started for Event: " + eventId)

                        #logger.info("Processing started for Event: " + eventId)
                        # For JsonLogMsg
                        log_message = "Processing started for Event = " + eventId
                        logger.info(objLog.fn_ProduceJsonLogMessage(None, eventId, log_message, module, cf.f_lineno, filename, None))

                        if 'payload' in obj and 'domainName' in obj:
                            #entityId = str(obj["entityId"])
                            entityId = str("NA")
                            domainName = str.upper(str(obj["domainName"]))

                            objDataController = DataController.DataGeneration()
                            objValidation = ValidationController.Validation()
                            objRIValidation = RIValidationController.RIController()
                            objProducer = ProducerController.ProducerDS()

                            #====================== CReate Domain Specific Metadata =================================================
                            # For JsonLogMsg
                            log_message = "Domain specific metadata creation is started"
                            logger.info(objLog.fn_ProduceJsonLogMessage(domainName, eventId, log_message, module, cf.f_lineno, filename, None))

                            df_Metadata = pd.DataFrame(final_list_all)
                            df_filtered_Metadata = df_Metadata.loc[df_Metadata[0] == domainName]
                            final_list = df_filtered_Metadata.values.tolist()
                            #print("hahahaha")
                            #print(final_list)

                            # For JsonLogMsg
                            log_message = "Domain specific metadata creation is completed"
                            logger.info(objLog.fn_ProduceJsonLogMessage(domainName, eventId, log_message, module, cf.f_lineno, filename, None))
                            # ====================== CReate Domain Specific Eroor Exception Metadata =================================================
                            df_Excep_Metadata = pd.DataFrame(excep_list_all)
                            df_Excep_filtered_Metadata = df_Excep_Metadata.loc[df_Excep_Metadata[0] == domainName]
                            exception_list = df_Excep_filtered_Metadata.values.tolist()
                            #print(exception_list)

                            # Delete SQL File creation
                            # ======================================================================================================
                            sql_path = objConfig.getConfiguration("SQL|sql_path")
                            today = str(date.today())
                            strTime = str(time.time())
                            strTm = strTime[:-3]
                            del_filePath = str(sql_path) + "Del_" + today + "-" + strTm + ".sql"
                            #print(del_filePath)

                            #========================================================================================================
                            # Event processing code start
                            #************************************************************************
                            #Audit Entry
                            biggnning_st = "In Progress"
                            End_st = "Successfully Finished"
                            biggnning_tab = 'S'
                            End_tab = 'F'

                            objAuditController.fn_AuditTrailInsertProcess(eventId, domainName, biggnning_st, biggnning_tab, '0', msg_offset, msg_partition)

                            # For JsonLogMsg
                            log_message = "Audit table is inserted with status = " + biggnning_st
                            logger.info(objLog.fn_ProduceJsonLogMessage(domainName, eventId, log_message, module, cf.f_lineno, filename, None))

                            val_flg = objValidation.fn_validation_process(obj, msg_offset, msg_partition)
                            #val_flg = 'T'
                            print("Validation = " + val_flg)
                            val_flg,excep_type = val_flg.split("|")
                            #excep_type = str(val_flg.split("|")[1])
                            print(val_flg)
                            if val_flg == 'T':
                                #print(final_list)
                                val_ri_flg = objRIValidation.fn_RI_validation_process(obj, final_list, eventId, domainName)
                                #val_ri_flg = 'F'
                                print("RI Validation" + val_ri_flg)
                                if val_ri_flg != 'T':
                                    f_0 = objDataController.fn_generatedata(obj, final_list, eventId, entityId, domainName, msg_offset, msg_partition)
                                    if f_0 > 0:
                                        #del obj1["payload"]
                                        f_k = objProducer.fn_producer(obj, topic_for_produce)
                                        if f_k == 1:
                                            # For JsonLogMsg
                                            log_message = "Event info for Datastage is published in Topic = " + topic_for_produce
                                            logger.info(objLog.fn_ProduceJsonLogMessage(domainName, eventId, log_message, module, cf.f_lineno, filename, None))

                                            objAuditController.fn_AuditTrailInsertProcess(eventId, domainName, End_st, End_tab, str(f_0), msg_offset, msg_partition)
                                            # For JsonLogMsg
                                            log_message = "Audit table is inserted with status = " + End_st
                                            logger.info(objLog.fn_ProduceJsonLogMessage(domainName, eventId, log_message, module, cf.f_lineno, filename, None))

                                            #objConsumer.fn_CommitMessage(consumer, message)
                                        else:
                                            # For JsonLogMsg
                                            log_message = "Event info for Datastage is not published in Topic = " + topic_for_produce
                                            logger.info(objLog.fn_ProduceJsonLogMessage(domainName, eventId, log_message, module, cf.f_lineno, filename, None))

                                            End_st_fk = "Not Finished - Kafka Server down. Please check the Log file"
                                            End_tab_fk = 'R'
                                            # kafka exception
                                            if len(exception_list) > 0:
                                                excep_type = 'KAFKA_ERR'
                                                print("calling kafka exception")
                                                #print finalObj
                                                objExcepMetaData.fn_exception_process(finalObj, excep_type, domainName, exception_list)
                                            else:
                                                print("Kafka server down. Please check with system administrator.")
                                                #logger.error("Kafka server down. Please check with system administrator.")
                                                # For JsonLogMsg
                                                log_message = "Kafka server is down. Please check with system administrator."
                                                logger.error(objLog.fn_ProduceJsonLogMessage(domainName, eventId, log_message, module, cf.f_lineno, filename, None))

                                            objDataController.fn_del_sql_exec(del_filePath, final_list, eventId, domainName, msg_offset, msg_partition)
                                            objAuditController.fn_AuditTrailInsertProcess(eventId, domainName, End_st_fk, End_tab_fk, '0', msg_offset, msg_partition)
                                            # For JsonLogMsg
                                            log_message = "Audit table is inserted with status = " + End_st_fk
                                            logger.info(objLog.fn_ProduceJsonLogMessage(domainName, eventId, log_message, module, cf.f_lineno, filename, None))
                                    else:
                                        print("Not Finished - Data Insertion Fail. Please check the Log file")
                                        #End_st_f0 = "Not Finished - Data Insertion Fail. Please check the Log file"
                                        #End_tab_f0 = 'R'
                                        # objAuditController.fn_AuditTrailInsertProcess(eventId, domainName, End_st_f0, End_tab_f0, '0', msg_offset, msg_partition)

                                        # data insertion fail
                                        if len(exception_list) > 0:
                                            excep_type = 'DB_INSERT_ERR'
                                            objExcepMetaData.fn_exception_process(finalObj, excep_type, domainName,exception_list)
                                        else:
                                            print("Data insertion fail. Please check the log file for details.")
                                            #logger.error("Data insertion fail. Please check log file for details. ")
                                            # For JsonLogMsg
                                            log_message = "Data insertion failed"
                                            logger.error(objLog.fn_ProduceJsonLogMessage(domainName, eventId, log_message, module, cf.f_lineno, filename, None))
                                else:
                                    print("RI Validation Fail")
                                    #logger.error("RI Validation Fail")
                                    # For JsonLogMsg
                                    log_message = "RI Validation failed"
                                    logger.error(objLog.fn_ProduceJsonLogMessage(domainName, eventId, log_message, module, cf.f_lineno, filename, None))

                                    #objConsumer.fn_CommitMessage(consumer, message)
                                    f_sup = objProducer.fn_producer_sup(obj, topic_for_sup_produce)
                                    if f_sup == 1:
                                        print("Event published in suspense topic - " + topic_for_sup_produce)
                                        # For JsonLogMsg
                                        log_message = "Event is published in Suspense Topic = " + topic_for_sup_produce
                                        logger.info(objLog.fn_ProduceJsonLogMessage(domainName, eventId, log_message, module, cf.f_lineno, filename, None))
                                    else:
                                        print("Event not published in suspense topic - " + topic_for_sup_produce)
                                        # For JsonLogMsg
                                        log_message = "Event is not published in Suspense Topic = " + topic_for_sup_produce
                                        logger.error(objLog.fn_ProduceJsonLogMessage(domainName, eventId, log_message, module, cf.f_lineno, filename, None))

                                        End_st_fs = "Not Finished - Event not publish successfully in suspense topic. Please check the Log file"
                                        End_tab_fs = 'R'
                                        #objDataController.fn_del_sql_exec(del_filePath, final_list, eventId)
                                        objAuditController.fn_AuditTrailInsertProcess(eventId, domainName, End_st_fs, End_tab_fs, '0', msg_offset, msg_partition)
                                        # For JsonLogMsg
                                        log_message = "Audit table is inserted with status = " + End_st_fs
                                        logger.info(objLog.fn_ProduceJsonLogMessage(domainName, eventId, log_message, module, cf.f_lineno, filename, None))
                            else:
                                print("Validation Fail")
                                #logger.error("Validation Fail")
                                # For JsonLogMsg
                                log_message = "Validation Failed"
                                logger.error(objLog.fn_ProduceJsonLogMessage(domainName, eventId, log_message, module, cf.f_lineno, filename, None))

                                #objConsumer.fn_CommitMessage(consumer, message)
                                if len(exception_list) > 0:
                                    objExcepMetaData.fn_exception_process(finalObj, excep_type, domainName, exception_list)
                                else:
                                    print("Exception Metadata not found for this domain. Please check Exception table.")
                                    #logger.error("Exception Metadata not found for this domain. Please check Exception table.")
                                    # For JsonLogMsg
                                    log_message = "Exception Metadata not found for this domain. Please check Exception table."
                                    logger.error(objLog.fn_ProduceJsonLogMessage(domainName, eventId, log_message, module, cf.f_lineno, filename, None))
                                
                                if excep_type == "DEFAULT":
                                    End_st_fs = "Validation Fail. Please check the Log file"
                                    End_tab_fs = 'R'
                                    objAuditController.fn_AuditTrailInsertProcess(eventId, domainName, End_st_fs, End_tab_fs, '0', msg_offset, msg_partition)
                                    # For JsonLogMsg
                                    log_message = "Audit table is inserted with status = " + End_st_fs
                                    logger.info(objLog.fn_ProduceJsonLogMessage(domainName, eventId, log_message, module, cf.f_lineno, filename, None))
                            print("Event processing finish executing at:" + time.asctime(time.localtime(time.time())))
                            #logger.info("Event processing finish executing at:" + time.asctime(time.localtime(time.time())))
                            # For JsonLogMsg
                            log_message = "Event processing finished executing"
                            logger.info(objLog.fn_ProduceJsonLogMessage(domainName, eventId, log_message, module, cf.f_lineno, filename, None))
                        else:
                            print("JSON message can not process without PAYLOAD-payload and DOMAIN_NAME-domainName TAG.")
                            #logger.error("JSON message can not process without PAYLOAD-payload and DOMAIN_NAME-domainName TAG.")
                            # For JsonLogMsg
                            log_message = "JSON message cannot be processed without PAYLOAD and DOMAIN_NAME"
                            logger.error(objLog.fn_ProduceJsonLogMessage(None, eventId, log_message, module, cf.f_lineno, filename, None))

                            End_st_fs = "JSON message can not proceess without PAYLOAD-payload and DOMAIN_NAME-domainName TAG."
                            End_tab_fs = 'R'
                            objAuditController.fn_AuditTrailInsertProcess(eventId, 'DEFAULT-DOMAIN', End_st_fs, End_tab_fs,'0', msg_offset, msg_partition)
                            # For JsonLogMsg
                            log_message = "Audit table is inserted with status = " + End_st_fs
                            logger.info(objLog.fn_ProduceJsonLogMessage(None, eventId, log_message, module, cf.f_lineno, filename, None))
                    else:
                        print("JSON message can not process without EVENT ID.")
                        #logger.error("JSON message can not process without EVENT ID.")
                        # For JsonLogMsg
                        log_message = "JSON message cannot be processed without EVENT ID"
                        logger.error(objLog.fn_ProduceJsonLogMessage(None, None, log_message, module, cf.f_lineno, filename, None))

                        End_st_fs = "JSON message can not process without EVENT ID-eventId TAG."
                        End_tab_fs = 'R'
                        objAuditController.fn_AuditTrailInsertProcess('-99', 'DEFAULT-DOMAIN', End_st_fs, End_tab_fs, '0' , msg_offset, msg_partition)
                        # For JsonLogMsg
                        log_message = "Audit table is inserted with status = " + End_st_fs
                        logger.info(objLog.fn_ProduceJsonLogMessage(None, None, log_message, module, cf.f_lineno, filename, None))
                else:
                    print("Event processing has been denied due to incorrect JSON format.")
                    #logger.error("Event processing has been denied due to incorrect JSON format.")
                    # For JsonLogMsg
                    log_message = "Event processing is denied due to incorrect JSON format"
                    logger.error(objLog.fn_ProduceJsonLogMessage(None, None, log_message, module, cf.f_lineno, filename, None))

                    End_st_fs = "JSON event processing has been denied due to incorrect JSON format.Please check the Log file"
                    End_tab_fs = 'R'
                    objAuditController.fn_AuditTrailInsertProcess('-99', 'DEFAULT-DOMAIN', End_st_fs, End_tab_fs, '0', msg_offset, msg_partition)
                    # For JsonLogMsg
                    log_message = "Audit table is inserted with status = " + End_st_fs
                    logger.info(objLog.fn_ProduceJsonLogMessage(None, None, log_message, module, cf.f_lineno, filename, None))
            consumer.close()
        else:
            print("Empty MetaData Found.Please create Metadata before processing.")
            # logger.error("Empty MetaData Found.Please create Metadata before processing.")
            # For JsonLogMsg
            log_message = "Empty MetaData Found. Please create Metadata before processing."
            logger.error(objLog.fn_ProduceJsonLogMessage(None, None, log_message, module, cf.f_lineno, filename, None))

    except Exception as e:
        print(e)
        logger.error(e)


if __name__ == '__main__':
    main()

