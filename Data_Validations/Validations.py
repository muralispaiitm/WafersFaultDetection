
# Importing Required Libraries
# -------------------------------- User defined Packages --------------------------------
from Logs.Logs import Logs_History

# ------------------------------- System defined Packages -------------------------------
from glob import glob
import os
import json
import shutil
import pandas as pd
import re
from datetime import datetime



class Validating:
    def __init__(self):
        self.LH = Logs_History()

        # ******************************************************************************
        # Validating "Training" or "Predicting" Batch Files
        # ******************************************************************************
        # Validating Batch Raw Files from local path "Temp_Files/Training/RawBatchFiles"
        # ------------------------------------------------------------------------------------
        # 1. Validate "wafer" Name
        # 2. Validate "Date" Length
        # 3. Validate "Time" Length
        # 4. Validate "Number of Columns"
        # 5. Validate "Names of Columns"
        # 6. Validate "Data types of Columns"
        # 7. Validate "Missing values of Columns"
        # ------------------------------------------------------------------------------------

    def validating_RawBatchFiles(self, LocalPaths, Schema_File_Name, Logs):

        # Logs_Df = pd.DataFrame(columns=list(Logs["Log_Tracks"].keys()))  # Storing all Log histories
        Logs_List = list()

        GoodCount = 0
        BadCount = 0

        try:
            # Logs_List.append(["Task", "Task_Type", "Date_Time", "File_Name", "File_From", "File_To", "Log_Message", "Action_Taken"])

            # %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
            # -------------------- Loading "Schema File" and "RawBatchFiles" ---------------------
            # %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
            # ............................ Load Schema validation file ...........................
            # Training Path: "Temp_Files/Training/Schema_Validations/"
            # Predicting Path: "Temp_Files/Predicting/Schema_Validations/"

            Logs_List.append(["Loading", "Schema File Loading", datetime.now(), "", "", "","Started", ""])

            Schema_FilePath = LocalPaths["SchemaValidations"] + Schema_File_Name
            Json_File = open(Schema_FilePath)
            Schema_File = json.load(Json_File)

            Logs_List.append(["Loading", "Schema File Loading", datetime.now(), Schema_File_Name, LocalPaths["SchemaValidations"], "Project", "Success", ""])
            Logs_List.append(["Loading", "Schema File Loading", datetime.now(), "", "", "", "Completed", ""])
            # ....................................................................................

            # ................................ Load RawBatchFiles ................................
            # Training Path: "Temp_Files/Training/RawBatchFiles/"
            # Predicting Path: "Temp_Files/Predicting/RawBatchFiles/"

            Logs_List.append(["Loading", "RawBatchFiles Loading", datetime.now(), "", "", "", "Started", ""])

            Local_CsvFiles = glob(os.path.join(LocalPaths["RawBatchFiles"], '*.csv'))
            Local_CsvFiles = [file.replace("\\", "/") for file in Local_CsvFiles]
            Logs_List.append(["Loading", "RawBatchFiles Loading", datetime.now(), Local_CsvFiles, "", LocalPaths["RawBatchFiles"], "Project", "Success", ""])

            Logs_List.append(["Loading", "RawBatchFiles Loading", datetime.now(), "", "", "", "Completed", ""])
            # ....................................................................................
            # ====================================================================================

            # Loop extracts one-by-one file from RawBatchFiles
            Logs_List.append(["Validation", "File Names and Columns Validations", datetime.now(), "", "", "", "Started", ""])
            for file in Local_CsvFiles:
                fileName = os.path.split(file)[1]

                # %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
                # ------------------------------ File Name Validations -------------------------------
                # %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
                fileNameList = fileName.split(".csv")[0].split("_")

                Logs_List.append(["Validation", "Names Validations", datetime.now(), "", "", "", "Started", ""])

                # Validation 1 ............................. "wafer" Validation  .............................
                if Schema_File["SampleFileName"].split("_")[0].lower() == fileNameList[0].lower():
                    Logs_List.append(["Validation", "'wafer' validation", datetime.now(), fileName, file, "Project", "Success", ""])

                    # Validation 2 ............................. "Date" Validation .............................
                    DateDigits = (len(re.compile('\d').findall(fileNameList[1])) == Schema_File["LengthOfDateStampInFile"])
                    DateLengh = (len(fileNameList[1]) == Schema_File["LengthOfDateStampInFile"])
                    if DateDigits and DateLengh:
                        Logs_List.append(["Validation", "'Date' validation", datetime.now(), fileName, file, "Project", "Success", ""])
                        # Validation 3 ............................. "Time" Validation .............................
                        TimeDigits = (len(re.compile('\d').findall(fileNameList[2])) == Schema_File["LengthOfTimeStampInFile"])
                        TimeLength = (len(fileNameList[2]) == Schema_File["LengthOfTimeStampInFile"])
                        if TimeDigits and TimeLength:
                            Logs_List.append(["Validation", "'Time' validation", datetime.now(), fileName, file, "Project", "Success",""])
                            Logs_List.append(["Validation", "Name Validations", datetime.now(), "", "", "", "Completed", ""])

                            # %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
                            # -------------------------------- Column Validations --------------------------------
                            # %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

                            Logs_List.append(["Validation", "Column Validations", datetime.now(), "", "", "", "Started", ""])

                            df = pd.read_csv(file)

                            # Validation 4 ............................. "ColumnsNumber" Validation .............................
                            if len(df.columns) == Schema_File["NumberofColumns"]:
                                Logs_List.append(["Validation", "'Column Number' validation", datetime.now(), fileName, file, "Project", "Success", ""])

                                # ColumnsNames List
                                schema_ColumnNames = list(Schema_File["ColName"].keys())
                                file_ColumnNames = list(df.columns)
                                # ColumnsDtypes List
                                schema_ColumnDTypes = list(Schema_File["ColName"].values())
                                file_ColumnDTypes = list(df.dtypes)
                                for i in range(1, Schema_File["NumberofColumns"]):
                                    # Validation 5 ............................. "Columns_Names" Validation .............................
                                    if schema_ColumnNames[i] == file_ColumnNames[i]:
                                        Logs_List.append(["Validation", "'Column Names' validation", datetime.now(), fileName, file,"Project", "Success", ""])

                                        # Validation 6 ............................."Columns_DataTypes" Validation .............................
                                        if (schema_ColumnDTypes[i] == str(file_ColumnDTypes[i])) or ("int64" == str(file_ColumnDTypes[i])):
                                            Logs_List.append(["Validation", "'Column DTypes' validation", datetime.now(), fileName, file,"Project", "Success", ""])

                                            # Validation 7 ............................."MissingValues" Validation .............................
                                            # Condition: If Missing values are less than or equal to 20%, then it is valid column.
                                            n_missingValues = df[file_ColumnNames[i]].isnull().sum()
                                            n_totalValues = len(df)
                                            if  n_missingValues != n_totalValues:
                                                Logs_List.append(["Validation", "'Missing values' validation", datetime.now(), fileName, file, "Project", "Success", ""])
                                                if i == Schema_File["NumberofColumns"] - 1:
                                                    GoodCount += 1
                                                    _ = shutil.move(file, LocalPaths["GoodRawBatchFiles"] + fileName)
                                                    Logs_List.append(["Validation", "Column Validations", datetime.now(), fileName, f"{LocalPaths['RawBatchFiles']}", f"{LocalPaths['GoodRawBatchFiles']}", "Completed", "File moved to GoodRawBatchFiles Folder"])
                                            else:
                                                BadCount += 1
                                                _ = shutil.move(file, LocalPaths["BadRawBatchFiles"] + fileName)
                                                Logs_List.append(["Validation", "'Missing values' validation", datetime.now(), fileName, f"{LocalPaths['RawBatchFiles']}", f"{LocalPaths['BadRawBatchFiles']}", f"Failed! {file_ColumnNames[i]} -> {n_missingValues}/{n_totalValues}", "File moved to BadRawBatchFiles Folder"])
                                                break
                                        else:
                                            BadCount += 1
                                            _ = shutil.move(file, LocalPaths["BadRawBatchFiles"] + fileName)
                                            Logs_List.append(["Validation", "'Column DTypes' validation", datetime.now(), fileName, f"{LocalPaths['RawBatchFiles']}", f"{LocalPaths['BadRawBatchFiles']}", f"Failed! {file_ColumnNames[i]} -> {str(file_ColumnDTypes[i])}", "File moved to BadRawBatchFiles Folder"])
                                            break
                                    else:
                                        BadCount += 1
                                        _ = shutil.move(file, LocalPaths["BadRawBatchFiles"] + fileName)
                                        Logs_List.append(["Validation", "'Column Names' validation", datetime.now(), fileName, f"{LocalPaths['RawBatchFiles']}", f"{LocalPaths['BadRawBatchFiles']}", f"Failed! {file_ColumnNames[i]}", "File moved to BadRawBatchFiles Folder"])
                                        break
                            else:
                                BadCount += 1
                                _ = shutil.move(file, LocalPaths["BadRawBatchFiles"] + fileName)
                                Logs_List.append(["Validation", "'Column Number' validation", datetime.now(), fileName, f"{LocalPaths['RawBatchFiles']}", f"{LocalPaths['BadRawBatchFiles']}", f"Failed! {len(df.columns)}/{Schema_File['NumberofColumns']}", "File moved to BadRawBatchFiles Folder"])
                        else:
                            BadCount += 1
                            _ = shutil.move(file, LocalPaths["BadRawBatchFiles"] + fileName)
                            Logs_List.append(["Validation", "'Time' validation", datetime.now(), fileName, f"{LocalPaths['RawBatchFiles']}", f"{LocalPaths['BadRawBatchFiles']}", f"Failed! {fileNameList[2]}","File moved to BadRawBatchFiles Folder"])
                    else:
                        BadCount += 1
                        _ = shutil.move(file, LocalPaths["BadRawBatchFiles"] + fileName)
                        Logs_List.append(["Validation", "'Date' validation", datetime.now(), fileName, f"{LocalPaths['RawBatchFiles']}", f"{LocalPaths['BadRawBatchFiles']}", f"Failed! {fileNameList[1]}", "File moved to BadRawBatchFiles Folder"])
                else:
                    BadCount += 1
                    _ = shutil.move(file, LocalPaths["BadRawBatchFiles"] + fileName)
                    Logs_List.append(["Validation", "'wafer' validation", datetime.now(), fileName, f"{LocalPaths['RawBatchFiles']}", f"{LocalPaths['BadRawBatchFiles']}", f"Failed! {fileNameList[0]}", "File moved to BadRawBatchFiles Folder"])

            Logs_List.append(["Validation", "File Names and Columns Validations", datetime.now(), "AllFiles", f"{LocalPaths['RawBatchFiles']}", f"{LocalPaths['GoodRawBatchFiles']}", f"Completed! GoodFiles:{GoodCount}, BadFiles:{BadCount}", "File moved to GoodRawBatchFiles Folder"])

            Message = "Validation Successful"

        except Exception as e:
            Logs_List.append(["Loading/Validation", "System Error", datetime.now(), "", "", "", f"Error! {e}", ""])
            Message = "Validation Failed!"

        # Sending Log history to Logs function
        # ----------------------------------------------------------------------------------------
        self.LH.storeLogs(Logs_List, Logs, FileName=Logs["Validation_FileName"])
        # ----------------------------------------------------------------------------------------

        return Message

        # ....................................................................................
    # ....................................................................................

