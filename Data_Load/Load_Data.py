# Import required libraries
from pymongo import MongoClient
import json
import boto3
import pandas as pd
import os
import shutil
from Global_Variables.Global_Variables import GlobalVariablesPath
import glob
from datetime import datetime

'''
from Global_Variables.Global_Variables import Paths
Path = Paths().
TP = Path.TrainingPaths()
PP = Path.PredictingPaths()
'''

# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
# ============================================= MONGO DB ==============================================
# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
class MongoDB:
    def __init__(self):
        # Importing MongoDB Variables paths
        self.paths = GlobalVariablesPath()
        self.mdbVar = self.paths.MdbVariables

        self.userId = self.mdbVar["userId"]
        self.pwd = self.mdbVar["pwd"]
        self.cluster = self.mdbVar["cluster"]
        self.DbURL = f"mongodb+srv://{self.userId}:{self.pwd}@{self.cluster}.heyil.mongodb.net/<dbname>?retryWrites=true&w=majority"

    # ======================= MongoDB Variables =======================
    # Need to modify

    # ======================= Data Base CONNECTING and CLOSING =======================
    def MDB_Connection_Open(self):
        client = MongoClient(self.DbURL)
        return client

    def MDB_Connection_Close(self, client):
        client.close()

    # ======================= Data Base =======================
    def List_Of_DB_Names(self, client):
        DB_Names = client.list_database_names()
        return DB_Names

    def Check_DB_Exists(self, client, DB_Name):
        if DB_Name in client.list_database_names():
            return True
        else:
            return False

    def Get_DataBase(self, client, DB_Name):
        if self.Check_DB_Exists(client, DB_Name):
            db = client[DB_Name]
            return db
        else:
            return False

    def Create_DataBase(self, client, DB_Name):
        if self.Check_DB_Exists(client, DB_Name):
            db = self.Get_DataBase(client, DB_Name)
            return db
        else:
            db = client[DB_Name]
            return db

    def Drop_DataBase(self, client, DB_Name):
        List_of_Collections = self.List_Of_Collections_From_DB(client, DB_Name)
        for collection in List_of_Collections:
            coll = self.Get_Collection(client, DB_Name, collection)
            coll.drop()

    # ======================= Collections =======================
    def List_Of_Collections_From_DB(self, client, DB_Name):
        if self.Check_DB_Exists(client, DB_Name):
            db = client[DB_Name]
            collectionNames = db.list_collection_names()
            return collectionNames
        else:
            return False

    def Check_Collection_Exists(self, client, DB_Name, CollectionName):
        db = self.Get_DataBase(client, DB_Name)
        if type(db) != type(False):
            if CollectionName in db.list_collection_names():
                return True
            else:
                return False
        else:
            return False

    def Insert_Collection(self, client, Local_JsonFilePath, DB_Name, Collection_Name):
        json_file = open(Local_JsonFilePath)
        FileCollection = json.load(json_file)

        if self.Check_Collection_Exists(client, DB_Name, Collection_Name):
            collection = self.Get_Collection(client, DB_Name, Collection_Name)
            collection.insert_one(FileCollection)

    def Drop_Collection(self, client, DB_Name, Collection_Name):
        collection = self.Get_Collection(client, DB_Name, Collection_Name)
        collection.drop()

    # ======================= Extract Records =======================
    def Get_Collection(self, client, DB_Name, Collection_Name):
        if (self.Check_DB_Exists(client, DB_Name)) & (self.Check_Collection_Exists(client, DB_Name, Collection_Name)):
            db = client[DB_Name]
            collection = db[Collection_Name]
            return collection
        else:
            return False

    def Get_Records_From_Collection_As_List(self, client, DB_Name, Collection_Name):
        collection = self.Get_Collection(client, DB_Name, Collection_Name)
        if type(collection) != type(False):
            Collection_In_List = list(collection.find())
            return Collection_In_List
        else:
            return False

    def Get_Records_From_Collection_As_DataFrame(self, client, DB_Name, Collection_Name):
        Collection_In_List = self.Get_Records_From_Collection_As_List(client, DB_Name, Collection_Name)
        if type(Collection_In_List) != type(False):
            Df = pd.DataFrame(Collection_In_List)
            return Df
        else:
            return False

    def Check_Record_Exists(self, client, DB_Name, Collection_Name, Record):
        collection = self.Get_Collection(client, DB_Name, Collection_Name)
        N_Records = collection.find(Record)
        if N_Records.count() > 0:
            return True
        else:
            return False

    # ======================= Insert Records =======================
    def Insert_Record(self, client, DB_Name, CollectionName, Record):
        collection = self.Get_Collection(client, DB_Name, CollectionName)
        collection.insert_one(Record)

    def Insert_Records_From_Df_Into_Collection(self, client, DB_Name, CollectionName, Df):
        if "_id" in Df.columns.to_list():
            Df = Df.drop(columns=["_id"], axis=1)
        records = json.loads(Df.T.to_json()).values()

        collection = self.Get_Collection(client, DB_Name, CollectionName)

        N_Inserted_Records = 0
        N_Uninserted_Records = 0
        for record in records:
            if self.Check_Record_Exists(client, DB_Name, CollectionName, record):
                N_Uninserted_Records += 1
            else:
                collection.insert_one(record)
                N_Inserted_Records += 1
        return {"Inserted_Records": N_Inserted_Records, "Uninserted_Records": N_Uninserted_Records}

# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
# ================================================ AWS ================================================
# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
class AWS:
    # ======================= Initialization =======================
    def __init__(self):
        # Importing AWS variables paths
        self.paths = GlobalVariablesPath()
        self.awsVar = self.paths.AwsVariables

        self.s3 = self.awsVar["s3"]
        self.aws_access_key_id = self.awsVar["aws_access_key_id"]
        self.aws_secret_access_key = self.awsVar["aws_secret_access_key"]
        self.region_name = self.awsVar["region_name"]

    # ========================== Session Creation ======================
    def Create_Session(self):
        MySession = boto3.Session(aws_access_key_id=self.aws_access_key_id,
                                  aws_secret_access_key=self.aws_secret_access_key,
                                  region_name=self.region_name)
        return MySession

    # ========================== Client Connection ======================
    def Connect_Client(self, MySession="None"):
        if MySession == "None":
            MySession = self.Create_Session()
        client = MySession.client(service_name=self.s3)
        return client

    def Connect_Resource(self, MySession="None"):
        if MySession == "None":
            MySession = self.Create_Session()
        resource = MySession.resource(service_name=self.s3)
        return resource

    # ========================== Bucket ======================
    def List_of_Buckets(self, MySession="None"):
        client = self.Connect_Client(MySession)
        bucket_response = client.list_buckets()
        buckets = bucket_response["Buckets"]
        BucketNames = [bucket['Name'] for bucket in buckets]
        return BucketNames

    # ========================== Folders In a Bucket ======================
    def List_of_Folders_In_Bucket(self, BucketName, MySession="None"):
        resource = self.Connect_Resource(MySession)  # Connect Resource
        Bucket_Objects_Summary = resource.Bucket(BucketName)  # Connect to a Bucket
        Files_Path = [FilePath.key for FilePath in Bucket_Objects_Summary.objects.all()]
        Folder_Names = list(set([File_Path.split(File_Path.split('/')[-1])[0] for File_Path in Files_Path]))
        return Folder_Names

    def Delete_Folder_In_Bucket(self, BucketName, Folder_Name, MySession="None"):
        LOFIF = self.List_of_Files_In_Folder(BucketName, Folder_Name, MySession)
        for Cloud_FileName in LOFIF:
            self.Delete_File(BucketName, Cloud_FileName, MySession)

    # ========================== Files in a Folder ======================
    def List_of_Files_In_Folder(self, BucketName, FolderName, MySession="None"):
        resource = self.Connect_Resource(MySession)  # Connect Resource
        Bucket_Objects_Summary = resource.Bucket(BucketName)  # Connect to a Bucket
        Files_Path = [FilePath.key for FilePath in Bucket_Objects_Summary.objects.all()]
        Required_Files_Path = [File_Path for File_Path in Files_Path if
                               File_Path.split(File_Path.split('/')[-1])[0] == FolderName]
        return Required_Files_Path

    def ReadCsv_As_DataFrame(self, BucketName, File_Path, MySession="None"):
        client = self.Connect_Client(MySession)
        obj = client.get_object(Bucket=BucketName, Key=File_Path)
        data = pd.read_csv(obj['Body'])
        return data

    def Upload_File(self, Local_FileName, BucketName, Cloud_FileName, MySession="None"):
        client = self.Connect_Client(MySession)
        client.upload_file(Local_FileName, BucketName, Cloud_FileName)

    def UploadAllFiles(self, Local_FilesPath, BucketName, Cloud_FilesPath, MySession="None"):
        Local_CsvFiles = glob.glob(os.path.join(Local_FilesPath, '*.csv'))

        client = self.Connect_Client(MySession)

        for LocalFilePath in Local_CsvFiles:
            (_, FileName) = os.path.split(LocalFilePath)
            CloudFilePath = Cloud_FilesPath + FileName
            client.upload_file(LocalFilePath, BucketName, CloudFilePath)

    def Download_File(self, BucketName, Cloud_FilePath, Local_FilePath, MySession="None"):
        client = self.Connect_Client(MySession)
        client.download_file(BucketName, Cloud_FilePath, Local_FilePath)

    def DownloadAllFiles(self, BucketName, Cloud_FilesPath, Local_FilesPath, MySession="None"):
        Cloud_CsvFiles = self.List_of_Files_In_Folder(BucketName, Cloud_FilesPath, MySession)

        client = self.Connect_Client(MySession)

        for CloudFilePath in Cloud_CsvFiles:
            (_, FileName) = os.path.split(CloudFilePath)
            LocalFilePath = Local_FilesPath + FileName
            client.download_file(BucketName, CloudFilePath, LocalFilePath)

    def Delete_File(self, BucketName, Cloud_FileName, MySession="None"):
        client = self.Connect_Client(MySession)
        client.delete_object(Bucket=BucketName, Key=Cloud_FileName)

    # *************************** NEED TO BE MODIFIED ***************************
    # def Insert_Data(self, DB, Collection, Data):

# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
# ============================================== LOCAL ================================================
# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
class Local:
    def __init__(self):
        pass

    # Sub functions which can be used in another function
    # --------------------------------------------------------
    def findDir(self, DirName):
        True_False = os.path.isdir(DirName)
        return True_False

    def createDir(self, DirName):
        logMessage = ""
        if not self.findDir(DirName):
            os.mkdir(DirName)
            logMessage = ["Folder : Load_Data.py", "01 : Creating Folder", datetime.now(), f"{DirName}", "User defined package: Local().createDir()", "", "Completed folder Creation", f"{DirName} is created successfully"]
        return logMessage

    def removeDir(self, DirName):
        Logs_List = list()
        Logs_List.append(["Folder : Load_Data.py", "01 : Removing folder in Local", datetime.now(), "", "User defined package: Local()", "", "Started", ""])

        if self.findDir(DirName):
            shutil.rmtree(DirName)
            LogMessage = ["Folder : Load_Data.py", "01 : Removing folder in Local", datetime.now(), f"Folder : '{DirName}'", "Local", "", "Completed", f"'{DirName}' is Removed Successfully"]
        else:
            LogMessage = ["Folder : Load_Data.py", "02 : Removing folder in Local", datetime.now(), f"Folder : '{DirName}'", "Local", "", "Completed", f"'{DirName}' is Removed Failed"]
        return LogMessage

    # Splits Path into list of directory names for using in createDirectories function
    # Example : "Training/RawBatchFiles/" --> ['Training', 'RawBatchFiles', '']
    # ---------------------------------------------------
    def pathSplit(self, path):
        path_Split = os.path.split(path)
        if path_Split[0] != "":
            pathDirList = self.pathSplit(path_Split[0]) + [path_Split[1]]
        else:
            pathDirList = [path_Split[1]]
        return pathDirList


    # Main functions
    # ---------------------------------------------------
    def createDirectories(self, Dict):

        Logs_List = list()

        # It creates folder and subfolder from the given Dictionary of Paths
        Org_Dir = os.getcwd()

        # Loop for creating Training directory in "Temp_Files"
        # ---------------------------------------
        Logs_List.append(["Folder : Load_Data.py", "01 : Creating Batch folders in Local", datetime.now(), "", "User defined package: Local()", "", "Started", ""])

        for path in list(Dict.values()):
            pathList = self.pathSplit(path)

            # Loop for creating sub-directories in Training Directory
            # ---------------------------------------
            for DirName in pathList:
                if DirName != "":
                    logMessage = self.createDir(DirName)
                    if logMessage != "":
                        Logs_List.append(logMessage)   # Appending Log Messages

                    os.chdir(os.path.join(os.getcwd(), DirName))

            # Change directory back to "Temp_Files" directory
            os.chdir(Org_Dir)
            # ---------------------------------------

        # Change back to Originial Directory
        os.chdir(Org_Dir)

        Logs_List.append(["Folder : Load_Data.py", "02 : Creating Batch folders in Local", datetime.now(), "", "User defined package: Local()", "", "Completed", ""])

        return Logs_List

        # ---------------------------------------
