
# Importing Required Libraries
# -------------------------------- User defined Packages --------------------------------
from Global_Variables.Global_Variables import GlobalVariablesPath

# ------------------------------- System defined Packages -------------------------------
import pandas as pd
import os


class Logs_History:
    def __init__(self):
        self.GVP = GlobalVariablesPath()
        self.Training_Logs = self.GVP.Training_Logs

    # Creating Log dataframe with the Log History ----------------------------------------
    def createDataFrame_From_List(self, Logs_List, Logs, FileName):

        KeyNames = list(Logs["Log_Tracks"].keys())

        if FileName in os.listdir(Logs["Path"]):
            Logs_Df = pd.read_csv(Logs["Path"] + FileName)
        else:
            Logs_Df = pd.DataFrame(columns=KeyNames)

        # Loop Extracts one-by-one log value from Logs_List
        for log in Logs_List:
            Dict = dict()
            for i in range(len(KeyNames)):
                Dict[KeyNames[i]] = log[i]
            Logs_Df = Logs_Df.append(Dict, ignore_index=True)

        Logs_Df.to_csv(Logs["Path"] + FileName, index=False)
        return Logs_Df
    # -------------------------------------------------------------------------------------

    # Concatenating "Old Dataframe" and "New Dataframe" -----------------------------------
    def concatLogs_into_OldDf(self, New_Df, Logs):
        if Logs["Log_Transactions_FileName"] not in os.listdir(Logs["Path"]):
            pd.DataFrame(columns=New_Df.columns).to_csv(Logs["Path"] + Logs["Log_Transactions_FileName"], index=False)
        Old_Df = pd.read_csv(Logs["Path"] + Logs["Log_Transactions_FileName"])
        Old_Df = pd.concat([Old_Df, New_Df])
        Old_Df.to_csv(Logs["Path"] + Logs["Log_Transactions_FileName"], index=False)
    # -------------------------------------------------------------------------------------

    # Main Funciton =======================================================================
    # Main: Storing Logs "separately" and "one file" --------------------------------------
    def storeLogs(self, Logs_List, Logs, FileName):
        New_Df = self.createDataFrame_From_List(Logs_List, Logs, FileName)
        self.concatLogs_into_OldDf(New_Df, Logs)
        # return []
    # -------------------------------------------------------------------------------------
