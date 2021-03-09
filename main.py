# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
# ---------------------------------- IMPORTING LIBRARIES --------------------------------
# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

# ------------------------------- System defined Packages -------------------------------
from flask import Flask, request, render_template
from flask import Response
import os
from flask_cors import CORS, cross_origin
import flask_monitoringdashboard as dashboard
import json
from datetime import datetime

# -------------------------------- User defined Packages --------------------------------
from Logs.Logs import Logs_History

from Global_Variables.Global_Variables import GlobalVariablesPath
from Data_Load.Load_Data import AWS
from Data_Load.Load_Data import MongoDB
from Data_Load.Load_Data import Local

from Data_Validations.Validations import Validating
from Data_Transform.Data_Transform import Transform
from Data_Merge_Files.Merge_Files import Merge
from Data_Preprocessing.Preprocessing import Preprocess


app = Flask(__name__)

# Home Page ------------------------------------------------------------------------------------------------
# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
@app.route("/", methods=['GET'])
def home():
    return render_template('index.html')

# Training the Raw Batch Files -----------------------------------------------------------------------------
# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
@app.route("/train", methods=['GET'])
def trainRouteClient():

    global Training_Logs, LocalTrainingPaths
    LH = Logs_History()

    Logs_List = list()

    try:
        # %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
        # -------------------------------------- Step 1 --------------------------------------
        # ----------------------------------- Downloading ------------------------------------
        # %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

        # ["Task", "Task_Type", "Date_Time", "File_Name", "File_From", "File_To", "Log_Message", "Action_Taken"]

        # -------------------------------- Loading Global Variables ---------------------------------

        Logs_List.append(["Loading_Variables : Main.py", "01 : Loading Variables", datetime.now(), "", "", "", "Started", ""])

        # Loading "Training_Files Paths" , "AWS variables path" and "MongoDB variables path"
        GVP = GlobalVariablesPath()

        LocalTrainingPaths = GVP.LocalTrainingPaths
        CloudTrainingPaths = GVP.CloudTrainingPaths

        awsVariables = GVP.AwsVariables
        MdbVariables = GVP.MdbVariables

        Training_Logs = GVP.Training_Logs

        Logs_List.append(["Loading_Variables : Main.py", "02 : Loading Variables", datetime.now(), "", "User defined package: GlobalVariablesPath()", "", "Completed", ""])
        # ------------------------------------------------------------------------------------


        # --------------------- Creating Folder Structure in Local Drive ---------------------

        Logs_List.append(["Folder : Main.py", "01 : Remove & Create Folder in Local", datetime.now(), "", "User defined package: Local()", "", "Started", ""])

        try:
            local = Local()
            LogMessage = local.removeDir("Temp_Files")                       # Removing Temporary Directories
            Logs_List.append(LogMessage)

            # local.createDir("Temp_Files")
            logMessageList = local.createDirectories(LocalTrainingPaths)         # Creating Temporary Directories
            Logs_List = Logs_List + logMessageList

            #local.removeDir("Log_Files")  # Removing Temporary Directories
            logMessageList = local.createDirectories({list(Training_Logs.keys())[0] : list(Training_Logs.values())[0]})
            Logs_List = Logs_List + logMessageList

            Logs_List.append(["Folder : Main.py", "02 : Creating folder in Local", datetime.now(), "", "", "", "Completed", ""])

        except Exception as e:
            Logs_List.append(["Folder : Main.py", "03 : System Error", datetime.now(), "", "User defined package: Local()", "", f"Failed! {e}", ""])

        # Storing Log information
        # ------------------------------------------------------------------------------------
        LH.storeLogs(Logs_List, Training_Logs, FileName=Training_Logs["Folder_FileName"])


        # --------- Downloading "Training Files" and "Schema Files" from AWS to Local --------
        Logs_List = list()

        Logs_List.append(["Downloading : Main.py", "03 : Downloading Training Files", datetime.now(), "", "", "", "Started", ""])

        aws = AWS()
        MySession = aws.Create_Session()

        try:
            # Loading Training Batch Files
            Logs_List.append(["Downloading : Main.py", "04 : Downloading Training Files", datetime.now(), "RawBatchTrainingFiles", "", "", "Started", ""])
            aws.DownloadAllFiles(awsVariables["Bucket_Name"], CloudTrainingPaths["RawBatchFiles"], LocalTrainingPaths["RawBatchFiles"], MySession)

            Logs_List.append(["Downloading : Main.py", "05 : Downloading Training Files", datetime.now(), "RawBatchTrainingFiles", "AWS", "Local", "Completed", f"Storing file into {LocalTrainingPaths['RawBatchFiles']}"])
        except Exception as e:
            Logs_List.append(["Downloading : Main.py", "06 : System Error", datetime.now(), "RawBatchTrainingFiles", "AWS", "Local", f"Failed! {e}", "Stopped"])

        # Loading Schema File
        Logs_List.append(["Downloading : Main.py", "07 : Downloading Schema File", datetime.now(), "", "", "", "Started", ""])

        Cloud_FilePath = CloudTrainingPaths["SchemaValidations"] + MdbVariables["SchemaTraining_FileName"]
        Local_FilePath = LocalTrainingPaths["SchemaValidations"] + MdbVariables["SchemaTraining_FileName"]
        try:
            aws.Download_File(awsVariables["Bucket_Name"], Cloud_FilePath, Local_FilePath, MySession)

            Logs_List.append(["Downloading : Main.py", "08 : Downloading Schema File", datetime.now(), "SchemaTrainingFile", f"{Cloud_FilePath}", f"{Local_FilePath}", "Completed", f"Storing file into {Local_FilePath}"])
        except Exception as e:
            Logs_List.append(["Downloading : Main.py", "09 : Ststem Error", datetime.now(), "SchemaTrainingFile", f"{Cloud_FilePath}", f"{Local_FilePath}", f"Failed! {e}", ""])

        # Storing Log information
        # ------------------------------------------------------------------------------------
        LH.storeLogs(Logs_List, Training_Logs, FileName=Training_Logs["Downloading_FileName"])

        # %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
        # -------------------------------------- Step 2 --------------------------------------
        # ----------------------------------- Validating -------------------------------------
        # %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
        Logs_List = list()
        Logs_List.append(["Validation : Main.py", "01 : Calling the validation function 'validating_RawBatchFiles()'", datetime.now(), "Function Call", "Main.py", "Validation.py", "Started", ""])
        validate = Validating()
        Message = validate.validating_RawBatchFiles(LocalTrainingPaths, MdbVariables["SchemaTraining_FileName"], Training_Logs)

        Logs_List.append(["Validation : Main.py", "02 : Returned from validation function 'validating_RawBatchFiles'", datetime.now(), "Returned from called Function", "Validation.py", "Main.py", Message, ""])

        # Storing Log information
        # ------------------------------------------------------------------------------------
        LH.storeLogs(Logs_List, Training_Logs, FileName=Training_Logs["Validation_FileName"])

        # %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
        # -------------------------------------- Step 3 --------------------------------------
        # ---------------------------------- Transforming ------------------------------------
        # %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

        # Transforming Data of Files taken from local path "Temp_Files/Training/GoodRawBatchFiles"
        # ------------------------------------------------------------------------------------
        Logs_List = list()

        Logs_List.append(["Transforming : Main.py", "01 : Calling the Transforming function 'Transforming_GoodRawBatchFiles()'", datetime.now(), "Function Call", "Main.py", "Data_Transform.py", "Started", ""])
        transform = Transform()
        Message = transform.transformData(LocalTrainingPaths, MdbVariables["SchemaTraining_FileName"],Training_Logs)

        Logs_List.append(["Transforming : Main.py", "02 : Returned from Transforming function 'Transforming_GoodRawBatchFiles()'", datetime.now(), "Returned from called Function", "Data_Transform.py", "Main.py", Message, ""])

        # Storing Log information
        # ------------------------------------------------------------------------------------
        LH.storeLogs(Logs_List, Training_Logs, FileName=Training_Logs["Transform_FileName"])

        # ------------------------------------------------------------------------------------

        # %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
        # -------------------------------------- Step 4 --------------------------------------
        # ---------------------------------- Merging Files -----------------------------------
        # %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

        # Generate Single File from Multiple Files "Temp_Files/Training/DataTransform_GoodRawBatchFiles"
        # ------------------------------------------------------------------------------------
        Logs_List = list()

        Logs_List.append(["Merging : Main.py", "01 : Calling the Merging function 'Creating Single_File from Transformed Files'", datetime.now(), "Function Call", "Main.py", "Merge_Files.py", "Started", ""])
        merge = Merge()
        Message = merge.mergeFiles(LocalTrainingPaths, Training_Logs)

        Logs_List.append(["Merging : Main.py", "02 : Returned from Merging function : 'Creating Single_File from Transformed Files'", datetime.now(), "Function Call", "Merge_Files.py", "Main.py", Message, ""])

        # Storing Log information
        # ------------------------------------------------------------------------------------
        LH.storeLogs(Logs_List, Training_Logs, FileName=Training_Logs["Transform_FileName"])

        # %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
        # -------------------------------------- Step 5 --------------------------------------
        # ------------------------------------ Uploading -------------------------------------
        # %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

        Logs_List = list()

        # --------------------- 1. Upload "GoodRawBatchFiles" from local to cloud AWS ---------------------
        try:
            Logs_List.append(["Uploading : Main.py", "01 : Uploading GoodRawBatchFiles", datetime.now(), "GoodRawBatchFiles", "", "","Started", ""])
            aws.UploadAllFiles(LocalTrainingPaths["GoodRawBatchFiles"], awsVariables["Bucket_Name"], CloudTrainingPaths["GoodRawBatchFiles"], MySession)
            Logs_List.append(["Uploading : Main.py", "02 : Uploading GoodRawBatchFiles", datetime.now(), "GoodRawBatchFiles", f"{awsVariables['Bucket_Name']}/{LocalTrainingPaths['GoodRawBatchFiles']}", f"{awsVariables['Bucket_Name']}/{CloudTrainingPaths['GoodRawBatchFiles']}", "Completed", ""])

            # --------------------- 2. Upload "BadRawBatchFiles" from local to cloud AWS ---------------------
            Logs_List.append(["Uploading : Main.py", "03 : Uploading BadRawBatchFiles", datetime.now(), "BadRawBatchFiles", "", "", "Started", ""])
            aws.UploadAllFiles(LocalTrainingPaths["BadRawBatchFiles"], awsVariables["Bucket_Name"], CloudTrainingPaths["BadRawBatchFiles"], MySession)
            Logs_List.append(["Uploading : Main.py", "04 : Uploading BadRawBatchFiles", datetime.now(), "BadRawBatchFiles", f"{awsVariables['Bucket_Name']}/{LocalTrainingPaths['BadRawBatchFiles']}", f"{awsVariables['Bucket_Name']}/{CloudTrainingPaths['BadRawBatchFiles']}", "Completed", ""])

            # --------------------- 3. Upload "DataTransform_GoodRawBatchFiles" from local to cloud AWS ---------------------
            Logs_List.append(["Uploading : Main.py", "05 : Uploading DataTransform_GoodRawBatchFiles", datetime.now(), "DataTransform_GoodRawBatchFiles", "", "", "Started", ""])
            aws.UploadAllFiles(LocalTrainingPaths["DataTransform_GoodRawBatchFiles"], awsVariables["Bucket_Name"], CloudTrainingPaths["DataTransform_GoodRawBatchFiles"], MySession)
            Logs_List.append(["Uploading : Main.py", "06 : Uploading DataTransform_GoodRawBatchFiles", datetime.now(), "DataTransform_GoodRawBatchFiles", f"{awsVariables['Bucket_Name']}/{LocalTrainingPaths['DataTransform_GoodRawBatchFiles']}", f"{awsVariables['Bucket_Name']}/{CloudTrainingPaths['DataTransform_GoodRawBatchFiles']}", "Completed", ""])

            # --------------------- 4. Upload "SingleFile" from local to cloud AWS ---------------------
            Logs_List.append(["Uploading : Main.py", "07 : Uploading SingleFile", datetime.now(), "SingleFile", "", "", "Started", ""])
            aws.UploadAllFiles(LocalTrainingPaths["SingleFile"], awsVariables["Bucket_Name"],CloudTrainingPaths["SingleFile"], MySession)
            Logs_List.append(["Uploading : Main.py", "08 : Uploading SingleFile", datetime.now(), "SingleFile", f"{awsVariables['Bucket_Name']}/{LocalTrainingPaths['SingleFile']}", f"{awsVariables['Bucket_Name']}/{CloudTrainingPaths['SingleFile']}", "Completed", ""])
        except Exception as e:
            Logs_List.append(["Uploading : Main.py", "09 : System Error!", datetime.now(), "UploadingError", "Local", "AWS", f"Failed! {e}", "Stoped"])

        # Storing Log information
        # ------------------------------------------------------------------------------------
        LH.storeLogs(Logs_List, Training_Logs, FileName=Training_Logs["Uploading_FileName"])

        # %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
        # -------------------------------------- Step 6 --------------------------------------
        # ---------------------------------- Preprocessing -----------------------------------
        # %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
        Logs_List = list()
        PP = Preprocess()
        try:
            Logs_List.append(["Preprocessing : Main.py", "01 : Preprocessing SingleFile", datetime.now(), "", "", "", "Started",""])
            PP.preprocess_Data(LocalTrainingPaths, Training_Logs)
            Logs_List.append(["Preprocessing : Main.py", "02 : Preprocessing SingleFile", datetime.now(), "SingleFile.csv", f"{LocalTrainingPaths['SingleFile']}", "", "Completed", ""])

        except Exception as e:
            Logs_List.append(["Preprocessing : Main.py", "03 : System Error!", datetime.now(), "SingleFile.csv", f"{LocalTrainingPaths['SingleFile']}", "", f"Failed! {e}", "Stopped"])

        # Storing Log information
        # ------------------------------------------------------------------------------------
        LH.storeLogs(Logs_List, Training_Logs, FileName=Training_Logs["Uploading_FileName"])

        # %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
        # -------------------------------------- Step 7 --------------------------------------
        # ---------------------------------- Model Training ----------------------------------
        # %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

        # ----------------------------------- Clustering -------------------------------------
        # 1.KMeans clustering using elbow
        # ------------------------------------------------------------------------------------

        # ----------------------------- Finding the Best Model -------------------------------
        # 1. XGBoost
        # 2. Random Forest
        # ------------------------------------------------------------------------------------

        # %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
        # -------------------------------------- Step 8 --------------------------------------
        # -------------------------------------- Predict -------------------------------------
        # %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

        # --------------------------------- Predict Response ---------------------------------
        # 1. Predict Response
        # 2. Store Response in Predict_Path
        # ------------------------------------------------------------------------------------
    except Exception as e:
        Error_Message = e
        Logs_List.append(["Preprocessing", "System Error", datetime.now(), "SingleFile.csv", f"{LocalTrainingPaths['SingleFile']}", "", f"Failed! {e}", "Stopped"])
        LH.storeLogs(Logs_List, Training_Logs, FileName=Training_Logs["Uploading_FileName"])

if __name__ == "__main__":
    app.run(debug=True, port=8001)