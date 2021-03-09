
# Importing Required Libraries
# -------------------------------- User defined Packages --------------------------------
from Logs.Logs import Logs_History

# ------------------------------- System defined Packages -------------------------------
from glob import glob
import os
from datetime import datetime
import pandas as pd
import json
from sklearn.preprocessing import MinMaxScaler



class Preprocess:
    # Preprocessing Data of Files from local path "Temp_Files/Training/InputFile"
    # ------------------------------------------------------------------------------------
    # 1. FeatureSelections : Remove columns with zero_std_deviation and Removing Unnecessary columns
    # 2. Split "Predictors (Input_Features)" and "Response (Output_Feature)"
    # 3. Scaling Data of "Predictors"
    # ------------------------------------------------------------------------------------

    def __init__(self):
        pass
        # self.LH = Logs_History()
        # self.MMScaler = MinMaxScaler()

    def preprocess_Data(self, LocalPaths, Logs):

        Logs_List = list()

        try:
            # Loading SingleFile from LocalPaths['SingleFile']
            Logs_List.append(["Preprocess", "Loading singleFile", datetime.now(), "SingleFile.csv", "", "", "Started", ""])
            data = pd.read_csv(LocalPaths['SingleFile'] + "SingleFile.csv")
            Logs_List.append(["Preprocess", "Loading singleFile", datetime.now(), "SingleFile.csv", {LocalPaths['SingleFile']}, "Project: DataFrame", "Completed", ""])

            # ------------------------------- 1. FeatureSelections  ------------------------------
            Logs_List.append(["Preprocess", "Feature Selection", datetime.now(), "SingleFile.csv", "", "", "Started", ""])
            # Remove columns with zero standard deviation
            data_desc = data.describe().T
            data = data[list(data_desc[data_desc['std'] > 0].index)]
            RemovedColumns = list(data_desc[data_desc['std'] == 0].index)
            Logs_List.append(["Preprocess", "Feature Selection", datetime.now(), "SingleFile.csv", "", "", "Completed", f"Zero Standard deviation columns {RemovedColumns} are removed"])
            # Remove columns with high correlations among predictors and low correlations with response
            # ------------------------------------------------------------------------------------

            # ------------------------ 2 Split "Predictors" and "Response"  ----------------------
            Logs_List.append(["Preprocess", "Splitting 'Predictors' and 'Response'", datetime.now(), "DF: data with required columns", "", "", "Started", ""])
            X = data.iloc[:, :-1]
            y = data.iloc[:, -1]
            Logs_List.append(["Preprocess", "Splitting 'Predictors' and 'Response'", datetime.now(), "DF: data with required columns", "", "", "Completed","X:Predictors, y:Response"])
            # ------------------------------------------------------------------------------------

            # -------------------------- 3 Scaling Data of "Predictors" --------------------------
            Logs_List.append(["Preprocess", "Feature Scaling", datetime.now(),"X:Predictors", "", "", "Started", ""])

            X_Scale = pd.DataFrame(MinMaxScaler().fit_transform(X), columns=X.columns)
            Logs_List.append(["Preprocess", "Feature Scaling", datetime.now(), "X:Predictors", "", "", "Completed", "Using MinMaxScalar"])
            # ------------------------------------------------------------------------------------

            # Storing Scaling dataframe into SingleFileFolder
            pd.concat([X_Scale, y], axis=1).to_csv(LocalPaths['SingleFile'] + "SingleFile_Scale.csv", index=False)
            Logs_List.append(["Preprocess", "Storing Scaling file", datetime.now(), "SingleFile_Scale.csv", "Df: [X, y]", LocalPaths['SingleFile'], "Completed","Concat X, y and stored as CSV file"])

        except Exception as e:
            Logs_List.append(["Preprocess", "System Error", datetime.now(), "SingleFile.csv", "", "", f"Failed! {e}", "Stopped"])

        # Storing Log history
        Logs_History().storeLogs(Logs_List, Logs, FileName=Logs["Preprocess_FileName"])

        return X_Scale, y