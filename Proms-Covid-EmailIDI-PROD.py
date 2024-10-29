# Databricks notebook source
# MAGIC %md
# MAGIC ##### Importing Voltage Libraries

# COMMAND ----------

# MAGIC %scala
# MAGIC import com.wba.idi20.voltage.VoltageHelper
# MAGIC import com.wba.idi20.voltage.VoltageEncryption
# MAGIC val ve = new VoltageEncryption("dapadbscope","VoltageKeyManagerUrl","voltagePolicyServerUrl","VoltageKeyVaultSecretScopeName","VoltageKeyVaultSecretName")
# MAGIC spark.udf.register("decryptFpeUDF", ve.decryptFpeUDF)
# MAGIC spark.udf.register("encryptFpeUDF", ve.encryptFpeUDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Defining Functions

# COMMAND ----------

def delete_files_not_ending_with_csv(directory_path):
    try:
        # Get a list of all files in the directory
        files = dbutils.fs.ls(directory_path)

        # Filter files that do not end with ".csv"
        files_to_delete = [file.path for file in files if not file.name.endswith(".csv")]

        # Delete the files
        for file_path in files_to_delete:
            dbutils.fs.rm(file_path)
            print(f"Deleted: {file_path}")
        
        print("Deletion completed successfully.")
    except Exception as e:
        print(f"Error occurred: {e}")

def delete_files_not_ending_with_dat(directory_path):
    try:
        # Get a list of all files in the directory
        files = dbutils.fs.ls(directory_path)

        # Filter files that do not end with ".dat"
        files_to_delete = [file.path for file in files if not file.name.endswith(".dat")]

        # Delete the files
        for file_path in files_to_delete:
            dbutils.fs.rm(file_path)
            print(f"Deleted: {file_path}")
        
        print("Deletion completed successfully.")
    except Exception as e:
        print(f"Error occurred: {e}")

def rename_dat_file_in_folder(path, custom_file_name):
    try:
        # Step 1: Use dbutils.fs.ls() command to list the files in the directory
        files = dbutils.fs.ls(os.path.join(path))
        
        # Step 2: Loop through the files and find the one that ends with ".dat"
        dat_file_path = None
        for file_info in files:
            if file_info.name.endswith(".csv"):
                dat_file_path = os.path.join(path, file_info.name)
                break
        
        # Step 3: Rename the file with the custom name
        if dat_file_path:
            new_file_path = os.path.join(path, custom_file_name)
            dbutils.fs.mv(dat_file_path, new_file_path)
            print(f"File renamed successfully. New file name: {custom_file_name}")
        else:
            print("No '.dat' file found in the specified folder.")
    except Exception as e:
        print(f"An error occurred: {str(e)}")

def encrypt_data(formatted_text):
   response = requests.post(url_encrypt, formatted_text)
   return(response.status_code)
 
def decrypt_data(formatted_text):
   response = requests.post(url_decrypt, formatted_text)
   return(response.status_code)
 
def get_csv_files(directory_path):
  """recursively list path of all parquet files in path directory """
  parquet_files_with_path = []
  
  files_to_treat = dbutils.fs.ls(directory_path)
  while files_to_treat:
    path = files_to_treat.pop(0).path
    if path.endswith('/'):
      files_to_treat += dbutils.fs.ls(path)
    elif path.endswith('.csv'):
      parquet_files_with_path.append(path)
      
  return parquet_files_with_path

def move_file_dat_delete_original(path_in,path_out,in_file_name,out_file_name):
  if os.path.exists('/dbfs'+path_in):
    dbutils.fs.cp(path_in+in_file_name,path_out+out_file_name)
    dbutils.fs.rm(path_in,True)
  else:
      print("path does not exist")

# COMMAND ----------

mount_point = '/mnt/prtout_semantic_clinicaltrial-phi'
inputFolderName = '/cohort-browser/pfizer/raw-contains-idi-dataset/'
outputFolderName = '/cohort-browser/pfizer/output-contains-idi-dataset/'
voltageConfig = '/mnt/prtout_semantic_clinicaltrial-phi/cohort-browser/pde-config-schema/config_email_idi.json'
finalDestination = '/cohort-browser/hotfix-encryption-processing/pfizer/C4591064/'
archiveInputFolderName = '/cohort-browser/pfizer/raw-contains-idi-dataset/archive/'
inputStorage = "dlxprodsemprtoutsa01"
outputStorage = "dlxprodsemprtoutsa01"
outputContainer = 'clinicaltrial-phi'
container = 'clinicaltrial-phi'
url_encrypt = 'https://dapprodpde-voltage-encrypt-decrypt-svc.azurewebsites.net/v0/encryption/fle/encrypt/123'
url_decrypt = 'https://dapprodpde-voltage-encrypt-decrypt-svc.azurewebsites.net/v0/encryption/fle/decrypt/123'
filepath = voltageConfig

# COMMAND ----------

# List all .dat files
dat_files = dbutils.fs.ls(mount_point + inputFolderName)

# Filter files based on ConsentA and ConsentB
dat_files_a = [file for file in dat_files if "ConsentA" in file.name and file.name.endswith(".dat")]
dat_files_b = [file for file in dat_files if "ConsentB" in file.name and file.name.endswith(".dat")]

# Extracting file names from FileInfo objects
file_names_a = [file.name for file in dat_files_a]
file_names_b = [file.name for file in dat_files_b]

print(len(dat_files_a), file_names_a, len(dat_files_b), file_names_b)

# COMMAND ----------

# Process files with ConsentA if the length is 1
if len(dat_files_a) == 1:
    inputFileName = dat_files_a[0].name
    print("Processing ConsentA file:", inputFileName)
    originalInputFileName = inputFileName
    print("Beginning processing ConsentA file:", inputFileName)
    
    from pyspark.sql.functions import col
    
    df_original_csv=spark.read.option("header",True).option("quote", "").option("delimiter","\u0001").csv(mount_point+inputFolderName+inputFileName);
    print("Total records in original encrypted file:",df_original_csv.count())
    display(df_original_csv)

    df_original_csv.filter(col('EMAIL')!='null').createOrReplaceTempView("Finalview")
    # Execute the SQL query and capture the result in a new DataFrame
    df_decrypted_IDI = spark.sql("""
    SELECT PATIENT_ID,PROTOCOL_ID,PATIENT_FIRST_NAME,PATIENT_LAST_NAME,DOB,PREFERRED_METHOD,/*EMAIL,*/ decryptFpeUDF(8000,EMAIL) as EMAIL, PHONE_SMS,LOCATION_NAME,LOCATION_ID,PATIENT_LANGUAGE FROM Finalview
    """)

    # Show the result DataFrame
    display(df_decrypted_IDI)
    df_decrypted_IDI.count()

    import os
    df_decrypted_IDI.repartition(1).write.option('header',True).option('delimiter','\u0001').mode('overwrite').csv(mount_point+outputFolderName)
    
    delete_files_not_ending_with_csv(mount_point+outputFolderName)
    rename_dat_file_in_folder(mount_point+outputFolderName, inputFileName.rsplit('.dat', 1)[0] + '_IDI_DEC.dat')
    print("Starting Decryption/Encryption Process")
      
    import requests, json
    inputFileName = inputFileName.rsplit('.dat', 1)[0] + '_IDI_DEC.dat'
    inputFolder = outputFolderName
    outputFolder = outputFolderName
    outputFileName = inputFileName.rsplit('.dat', 1)[0] + '_DNA_ENC.dat'
    message_body = dbutils.fs.head(filepath)
    print("message",message_body)
    message_dict = json.loads(message_body)
    message_dict["storageAccount"] = inputStorage
    message_dict["outputStorageAccount"] = outputStorage
    message_dict["outputContainer"] = outputContainer
    message_dict["container"] = container
    message_dict["inputFolder"]=inputFolder
    message_dict["inputFile"]=inputFileName
    message_dict["outputFolder"]=outputFolder
    message_dict["outputFile"]=outputFileName
    formatted_text = json.dumps(message_dict)

    print(formatted_text)
    encrypt_data(formatted_text)
    import time
    time.sleep(60)
    print("Ending  Decryption/Encryption Process")
    
    df_output=spark.read.option("header",True).option("quote", "").option("delimiter","\u0001").csv(mount_point+outputFolderName+outputFileName);
    print(mount_point+outputFolderName+outputFileName, "Total records in original encrypted file:",df_output.count())
    display(df_output)
    
    print("Cleaning Files not ending with .dat")
    
    delete_files_not_ending_with_dat(mount_point+outputFolderName) 
elif len(dat_files_a) != 1:
    print("Error: Multiple files found for ConsentA. Expected only one file.")

# COMMAND ----------

if len(dat_files_a) == 1:
  print("Moving Files to cohort browser destination")
    
  move_file_dat_delete_original(mount_point+outputFolderName, mount_point+finalDestination,outputFileName, outputFileName.rsplit('_IDI_DEC_DNA_ENC.dat', 1)[0] + '.dat')

  time.sleep(60)

  print("Archiving Original Files")

  dbutils.fs.mv(mount_point+inputFolderName+originalInputFileName, mount_point+archiveInputFolderName+originalInputFileName.rsplit('.dat', 1)[0] + '_archive.dat')
elif len(dat_files_a) != 1:
    print("Error: Multiple files found for ConsentA. Expected only one file.")

# COMMAND ----------

# Process files with ConsentA if the length is 1
if len(dat_files_b) == 1:
    inputFileName = dat_files_b[0].name
    print("Processing ConsentB file:", inputFileName)
    originalInputFileName = inputFileName
    print("Beginning processing ConsentB file:", inputFileName)
    
    from pyspark.sql.functions import col
    
    df_original_csv=spark.read.option("header",True).option("quote", "").option("delimiter","\u0001").csv(mount_point+inputFolderName+inputFileName);
    print("Total records in original encrypted file:",df_original_csv.count())
    display(df_original_csv)

    df_original_csv.filter(col('EMAIL')!='null').createOrReplaceTempView("Finalview")
    # Execute the SQL query and capture the result in a new DataFrame
    df_decrypted_IDI = spark.sql("""
    SELECT PATIENT_ID,PROTOCOL_ID,PATIENT_FIRST_NAME,PATIENT_LAST_NAME,DOB,PREFERRED_METHOD,/*EMAIL,*/ decryptFpeUDF(8000,EMAIL) as EMAIL, PHONE_SMS,LOCATION_NAME,LOCATION_ID,PATIENT_LANGUAGE FROM Finalview
    """)

    # Show the result DataFrame
    display(df_decrypted_IDI)
    df_decrypted_IDI.count()

    import os
    df_decrypted_IDI.repartition(1).write.option('header',True).option('delimiter','\u0001').mode('overwrite').csv(mount_point+outputFolderName)
    
    delete_files_not_ending_with_csv(mount_point+outputFolderName)
    rename_dat_file_in_folder(mount_point+outputFolderName, inputFileName.rsplit('.dat', 1)[0] + '_IDI_DEC.dat')
    print("Starting Decryption/Encryption Process")
      
    import requests, json
    inputFileName = inputFileName.rsplit('.dat', 1)[0] + '_IDI_DEC.dat'
    inputFolder = outputFolderName
    outputFolder = outputFolderName
    outputFileName = inputFileName.rsplit('.dat', 1)[0] + '_DNA_ENC.dat'
    message_body = dbutils.fs.head(filepath)
    print("message",message_body)
    message_dict = json.loads(message_body)
    message_dict["storageAccount"] = inputStorage
    message_dict["outputStorageAccount"] = outputStorage
    message_dict["outputContainer"] = outputContainer
    message_dict["container"] = container
    message_dict["inputFolder"]=inputFolder
    message_dict["inputFile"]=inputFileName
    message_dict["outputFolder"]=outputFolder
    message_dict["outputFile"]=outputFileName
    formatted_text = json.dumps(message_dict)

    print(formatted_text)
    encrypt_data(formatted_text)
    import time
    time.sleep(60)
    print("Ending  Decryption/Encryption Process")
    
    df_output=spark.read.option("header",True).option("quote", "").option("delimiter","\u0001").csv(mount_point+outputFolderName+outputFileName);
    print(mount_point+outputFolderName+outputFileName, "Total records in original encrypted file:",df_output.count())
    display(df_output)
    
    print("Cleaning Files not ending with .dat")
    
    delete_files_not_ending_with_dat(mount_point+outputFolderName) 
elif len(dat_files_b) != 1:
    print("Error: Multiple files found for ConsentB. Expected only one file.")

# COMMAND ----------

if len(dat_files_b) == 1:
  print("Moving Files to cohort browser destination")
    
  move_file_dat_delete_original(mount_point+outputFolderName, mount_point+finalDestination,outputFileName, outputFileName.rsplit('_IDI_DEC_DNA_ENC.dat', 1)[0] + '.dat')

  time.sleep(60)

  print("Archiving Original Files")

  dbutils.fs.mv(mount_point+inputFolderName+originalInputFileName, mount_point+archiveInputFolderName+originalInputFileName.rsplit('.dat', 1)[0] + '_archive.dat')
elif len(dat_files_b) != 1:
    print("Error: Multiple files found for ConsentA. Expected only one file.")
