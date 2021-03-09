
# ------------------------------- System defined Packages -------------------------------

class GlobalVariablesPath:
    def __init__(self):
        self.LocalTrainingPaths = {"RawBatchFiles"      : "Temp_Files/Training/RawBatchFiles/",
                                   "GoodRawBatchFiles"  : "Temp_Files/Training/GoodRawBatchFiles/",
                                   "BadRawBatchFiles"   : "Temp_Files/Training/BadRawBatchFiles/",
                                   "DataTransform_GoodRawBatchFiles": "Temp_Files/Training/DataTransform_GoodRawBatchFiles/",
                                   "SingleFile"         : "Temp_Files/Training/SingleFile/",
                                   "SchemaValidations"  : "Temp_Files/Training/Schema_Validations/"
                                   }
        self.LocalPredictingPaths = {"RawBatchFiles"    : "Temp_Files/Predicting/RawBatchFiles/",
                                     "GoodRawBatchFiles": "Temp_Files/Predicting/GoodRawBatchFiles/",
                                     "BadRawBatchFiles" : "Temp_Files/Predicting/BadRawBatchFiles/",
                                     "DataTransform_GoodRawBatchFiles": "Temp_Files/Predicting/DataTransform_GoodRawBatchFiles/",
                                     "SingleFile"       : "Temp_Files/Predicting/SingleFile/",
                                     "SchemaValidations": "Temp_Files/Predicting/Schema_Validations/"
                                     }

        self.CloudTrainingPaths = {"RawBatchFiles"      : "Training/RawBatchFiles/",
                                   "GoodRawBatchFiles"  : "Training/GoodRawBatchFiles/",
                                   "BadRawBatchFiles"   : "Training/BadRawBatchFiles/",
                                   "DataTransform_GoodRawBatchFiles": "Training/DataTransform_GoodRawBatchFiles/",
                                   "SingleFile"         : "Training/SingleFile/",
                                   "SchemaValidations"  : "Training/Schema_Validations/"
                                   }
        self.CloudPredictingPaths = {"RawBatchFiles"    : "Predicting/RawBatchFiles/",
                                     "GoodRawBatchFiles": "Predicting/GoodRawBatchFiles/",
                                     "BadRawBatchFiles" : "Predicting/BadRawBatchFiles/",
                                     "DataTransform_GoodRawBatchFiles": "Predicting/DataTransform_GoodRawBatchFiles/",
                                     "SingleFile"       : "Predicting/SingleFile/",
                                     "SchemaValidations": "Predicting/Schema_Validations/"
                                     }
        self.AwsVariables = {"s3"                   : "s3",
                             "Bucket_Name"          : "waferfaultdetection",
                             "aws_access_key_id"    : "AKIASAN2O2GWSUXUKI5X",
                             "aws_secret_access_key": "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",
                             "region_name"          : "us-east-1"
                             }
        self.MdbVariables = {"userId"                   : "gowtham136",
                             "pwd"                      : "user136",
                             "cluster"                  : "cluster0",
                             "DbName"                   : "WaferFaultDetection",
                             "SchemaTraining_FileName"  : "schema_training.json",
                             "SchemaPredicting_FileName": "schema_prediction.json",
                             "Logs_FileName"            : "Logs"
                             }

        self.Training_Logs = {"Path"                        : "Log_Files/Training/",
                              "Log_Transactions_FileName"   : "Logs_Train_of_AllTransactions.csv",
                              "Folder_FileName"             : "Logs_Train_of_Folder.csv",
                              "Downloading_FileName"        : "Logs_Train_of_Downloading.csv",
                              "Validation_FileName"         : "Logs_Train_of_Validated.csv",
                              "MissingValues_FileName"      : "Logs_Train_of_MissingValues.csv",
                              "Transform_FileName"          : "Logs_Train_of_Transform.csv",
                              "Merging_FileName"            : "Logs_Train_of_Merging.csv",
                              "Uploading_FileName"          : "Logs_Train_of_Uploading.csv",
                              "Preprocess_FileName"         : "Logs_Train_of_Preprocess.csv",
                              "Log_Tracks"                  : {"Task"        : "",
                                                               "Task_Type"   : "",
                                                               "Date_Time"   : "",
                                                               "File_Name"   : "",
                                                               "File_From"   : "",
                                                               "File_To"     : "",
                                                               "Log_Message" : "",
                                                               "Action_Taken": ""
                                                               }
                              }

        self.Predicting_Logs = {"Path"                      : "Log_Files/Predicting/",
                                "Log_Transactions_FileName" : "Logs_Predict_of_AllTransactions.csv",
                                "Folder_FileName"           : "Logs_Predict_of_Folder.csv",
                                "Downloading_FileName"      : "Logs_Predict_of_Downloading.csv",
                                "Validation_FileName"       : "Logs_Predict_of_Validated.csv",
                                "MissingValues_FileName"    : "Logs_Predict_of_MissingValues.csv",
                                "Transform_FileName"        : "Logs_Predict_of_Transform.csv",
                                "Merging_FileName"          : "Logs_Predict_of_Merging.csv",
                                "Uploading_FileName"        : "Logs_Predict_of_Uploading.csv",
                                "Preprocess_FileName"       : "Logs_Predict_of_Preprocess.csv",
                                "Log_Tracks"                : {"Task"       : "",
                                                               "Task_Type"  : "",
                                                               "Date_Time"  : "",
                                                               "File_Name"  : "",
                                                               "File_From"  : "",
                                                               "File_To"    : "",
                                                               "Log_Message": "",
                                                               "Action_Taken": ""
                                                               }
                                }

