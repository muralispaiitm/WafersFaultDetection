# Importing Required Libraries
# -------------------------------- User defined Packages --------------------------------
from Logs.Logs import Logs_History

# ------------------------------- System defined Packages -------------------------------
from glob import glob
import os
from datetime import datetime
import pandas as pd

class Merge:
    # Generate Single File from Multiple Files "Temp_Files/Training/DataTransform_GoodRawBatchFiles"
    # ------------------------------------------------------------------------------------
    # 1. Combining all csv files into single csv file
    # ------------------------------------------------------------------------------------

    def __init__(self):
        pass

    def mergeFiles(self, LocalPaths, Logs):

        Logs_List = list()
        try:
            # ................................ Load DataTransform_GoodRawBatchFiles ................................
            # Training Path: "Temp_Files/Training/DataTransform_GoodRawBatchFiles/"
            # Predicting Path: "Temp_Files/Predicting/DataTransform_GoodRawBatchFiles/"

            Logs_List.append(["Loading", "DataTransform_GoodRawBatchFiles Loading", datetime.now(), "", "", "", "Started", ""])

            Local_CsvFiles = glob(os.path.join(LocalPaths["DataTransform_GoodRawBatchFiles"], '*.csv'))

            Logs_List.append(["Loading", "DataTransform_GoodRawBatchFiles Loading", datetime.now(), "", f"{LocalPaths['DataTransform_GoodRawBatchFiles']}", "", "Completed", ""])

            Logs_List.append(["Merging", "Merging all to one file", datetime.now(), "", "", "", "Started", ""])

            single_df = ""
            filesList = []
            for file in Local_CsvFiles:
                file = file.replace("\\", "/")
                fileName = os.path.split(file)[1]
                filesList.append(fileName)
                df = pd.read_csv(file)

                if len(single_df)==0:
                    single_df = pd.DataFrame(columns=df.columns)

                single_df = pd.concat([single_df, df])
            Logs_List.append(["Merging", "Merging all to one file", datetime.now(), f"{filesList}", f"{LocalPaths['DataTransform_GoodRawBatchFiles']}", "Project", "Completed", ""])

            single_df.to_csv(LocalPaths["SingleFile"] + "SingleFile.csv", index=False)

            Logs_List.append(["Merging", "Saving singleFile", datetime.now(), "SingleFile.csv", "Dataframe", f"{LocalPaths['SingleFile']}", "Completed", ""])

            Message = "Merge Successful"

            # ....................................................................................
        except Exception as e:
            Logs_List.append(["Merging", "System Error!", datetime.now(), "DataTransform_GoodRawBatchFiles", "", "", f"Failed! {e}", "Stoped"])
            Message = "Merge Failed!"

        # Storing Log history to Logs function
        # ----------------------------------------------------------------------------------------
        Logs_History().storeLogs(Logs_List, Logs, FileName=Logs["Merging_FileName"])

        return Message