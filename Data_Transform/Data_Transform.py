
# Importing Required Libraries
# -------------------------------- User defined Packages --------------------------------
from Logs.Logs import Logs_History

# ------------------------------- System defined Packages -------------------------------
from glob import glob
import os
from datetime import datetime
import pandas as pd
import json

class Transform:
    # Transforming Data of Files taken from local path "Temp_Files/Training/GoodRawBatchFiles"
    # ------------------------------------------------------------------------------------
    # 1. Rename "Unnamed 0" column to "Wafer"
    # 2. Store missing values data into a csv file
    # 3. Imputing Missing values with "mean()"
    # 4. Convert columns data types from "int64" to "float64"
    # ------------------------------------------------------------------------------------

    def __init__(self):
        self.LH = Logs_History()

    def transformData(self, LocalPaths, Schema_File_Name, Logs):

        # Logs_List.append(["Task", "Task_Type", "Date_Time", "File_Name", "File_From", "File_To", "Log_Message", "Action_Taken"])

        Logs_List = list()
        fileNameList = list()
        try:
            # %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
            # -------------------- Loading "Schema File" and "RawBatchFiles" ---------------------
            # %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
            # ............................ Load Schema validation file ...........................
            # Training Path: "Temp_Files/Training/Schema_Validations/"
            # Predicting Path: "Temp_Files/Predicting/Schema_Validations/"

            Logs_List.append(["Loading", "Schema File Loading", datetime.now(), "", "", "", "Started", ""])

            Schema_FilePath = LocalPaths["SchemaValidations"] + Schema_File_Name
            Json_File = open(Schema_FilePath)
            Schema_File = json.load(Json_File)
            schema_ColumnNames = list(Schema_File["ColName"].keys())

            Missing_Df = pd.DataFrame(columns=schema_ColumnNames)

            Logs_List.append(["Loading", "Schema File Loading", datetime.now(), Schema_File_Name, LocalPaths["SchemaValidations"],"Project", "Completed", ""])

            # ................................ Load GoodRawBatchFiles ................................
            Logs_List.append(["Loading", "GoodRawBatchFiles", datetime.now(), "", "", "", "Started", ""])
            # Training Path: "Temp_Files/Training/GoodRawBatchFiles/"
            # Predicting Path: "Temp_Files/Predicting/GoodRawBatchFiles/"

            Local_CsvFiles = glob(os.path.join(LocalPaths["GoodRawBatchFiles"], '*.csv'))

            Logs_List.append(["Transform", "GoodRawBatchFiles", datetime.now(), "", "", "", "Started", ""])

            Logs_List.append(["Transform", "DataTypes, MissingValues", datetime.now(), "", "", "", "Started", ""])
            for file in Local_CsvFiles:

                file = file.replace("\\", "/")
                fileName = os.path.split(file)[1]
                fileNameList.append(fileName)
                df = pd.read_csv(file)

                df.rename(columns={"Unnamed: 0": schema_ColumnNames[0]}, inplace=True)
                Logs_List.append(["Transform", "Rename Unnamed_column to 'Wafer'", datetime.now(), fileName,"", "", "Completed! Renamed column", ""])

                Logs_List.append(["Transform", "DataTypes/MissingValues", datetime.now(), fileName, LocalPaths["GoodRawBatchFiles"], "","Started", ""])

                # -------------------------------- Store missing values into DataFrame -------------------------------
                Missing_Df = pd.concat([Missing_Df, pd.DataFrame(df.isnull().sum()).T])

                converted_cols = list()
                imputed_cols = list()
                for col in schema_ColumnNames:
                    # --------------------------- Imputing Missing values with "mean()" ----------------------------
                    if df[col].isnull().sum() > 0:
                        df[col].fillna(df[col].mean(), inplace=True)
                        imputed_cols.append(col)

                    # -------------------- Convert columns data types from "int64" to "float64" --------------------
                    if df[col].dtype == "int64":
                        df[col] = df[col].astype(float)
                        converted_cols.append(col)

                df.to_csv(LocalPaths["DataTransform_GoodRawBatchFiles"] + fileName, index=False)

                Logs_List.append(["Transform", "Converted DataTypes", datetime.now(), fileName, LocalPaths["GoodRawBatchFiles"], "","Completed", f"{converted_cols} columns are converted to 'float64'"])
                Logs_List.append(["Transform", "Imputed Missing Columns", datetime.now(), fileName, LocalPaths["GoodRawBatchFiles"], "","Completed", f"{imputed_cols} columns are converted to 'float64'"])
            Logs_List.append(["Transform", "Imputed MissingValues", datetime.now(), "", "", "", "Completed", ""])

            # Storing Missing values data frame into CSV
            # -------------------------------------------------------------------------------------------
            Missing_Df.index = fileNameList
            Missing_Df.to_csv(Logs["Path"] + Logs["MissingValues_FileName"])

            Logs_List.append(["Transform", "Storing MissingValues", datetime.now(), "All Files", "DataFrame", f"{Logs['Path']}","Completed", f"Stored in : {Logs['Path'] + Logs['MissingValues_FileName']}"])

            Message = "Transform Successful"
        except Exception as e:
            Logs_List.append(["Transform", "System Error", datetime.now(), "GoodRawBatchFiles", f"{LocalPaths['GoodRawBatchFiles']}", f"{LocalPaths['GoodRawBatchFiles']}", f"Failed! {e}", "Stoped"])
            Message = "Transform Failed!"

        # Storing Log history to Logs function
        # ----------------------------------------------------------------------------------------
        Logs_History().storeLogs(Logs_List, Logs, FileName=Logs["Transform_FileName"])
        # ----------------------------------------------------------------------------------------

        return Message
