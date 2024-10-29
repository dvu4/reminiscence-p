// Databricks notebook source
import com.wba.idi20.voltage.VoltageHelper
import com.wba.idi20.voltage.VoltageEncryption
import org.apache.spark.sql.functions.{coalesce, col, current_date, current_timestamp, date_add, date_format, expr, lit, month, row_number, to_date, to_timestamp, udf, when, _}

// COMMAND ----------

val ve = new VoltageEncryption("dtrans-secrets","IDIVoltageKeyManagerUrl","IDIVoltagePolicyUrl","IDIVoltageKeyVaultSecretScopeName","IDIVoltageKeyVaultSecretName")

// COMMAND ----------

// MAGIC %python
// MAGIC dbutils.widgets.removeAll()
// MAGIC # Changed per environment
// MAGIC dbutils.widgets.text("env", "prod")
// MAGIC env = dbutils.widgets.get("env")

// COMMAND ----------

// MAGIC %python
// MAGIC from pyspark.sql.functions import col, when, first
// MAGIC from pyspark.sql.functions import * 
// MAGIC from datetime import date,timedelta
// MAGIC from pyspark.sql.types import IntegerType,DecimalType, DoubleType, StructType, StructField
// MAGIC from pyspark.sql.window import Window
// MAGIC import os
// MAGIC from pyspark.sql.functions import sum as spark_sum, col
// MAGIC from pyspark.sql.window import Window
// MAGIC from pyspark.sql.functions import col, row_number

// COMMAND ----------

// MAGIC %python
// MAGIC start_date = str(date.today() - timedelta(days=1))
// MAGIC end_date = str(date.today())

// COMMAND ----------

// MAGIC %python
// MAGIC # Transaction
// MAGIC df_transactions = spark.read.load("abfss://curated@prodretailmchpadls01.dfs.core.windows.net/retail/transaction/POS_TRANSACTION").filter((col("RECORDTYP") == 1100) | (col("RECORDTYP") == 1000)).filter((col("RFN").isNotNull()) & (trim(col("RFN")) != "")).withColumn("SALES_TXN_DT", to_timestamp("SALES_TXN_DT", "yyyyMMdd")).withColumn('UPC_NBR', col('UPC_NBR').cast('long')).withColumn("LINE_ITEM_SEQ_NBR", col('LINE_ITEM_SEQ_NBR').cast(IntegerType())).withColumn("UNIT_QTY", col('UNIT_QTY').cast(IntegerType())).withColumn("ITEM_UNIT_PRICE_DLRS",  col('ITEM_UNIT_PRICE_DLRS').cast(DoubleType())).filter(col("SALES_TXN_DT").between(start_date, end_date)).select("SALES_TXN_DT", "LINE_ITEM_SEQ_NBR", "STORE_NBR", "UPC_NBR", "ITEM_UNIT_PRICE_DLRS", "RFN", "UNIT_QTY", "ARTICLE_NBR", "loyalty_id").distinct()
// MAGIC
// MAGIC # Product master (unique combination of UPC, ARTICLE_NBR &  opStudy)
// MAGIC df_master = spark.read.load("abfss://curated@prodretailmchpadls01.dfs.core.windows.net/retail/masterdata/PRODUCT_MASTER/").withColumn('opStudy', substring(col('NODE'), 9, 3)).withColumn('UPC_NBR', col('UPC').cast('long')).withColumn("opStudy", col('opStudy').cast(IntegerType())).select("ARTICLE_NBR", "UPC_NBR", "opStudy").distinct()
// MAGIC
// MAGIC # For sku
// MAGIC df_sku = spark.read.load("abfss://curated@prodretailmchpadls01.dfs.core.windows.net/retail/subscription/STORE_ASSORTMENT_DIGITAL").filter(col('SKU_ID').isNotNull()).filter(col("STATUS_CODE") == "Activated").withColumn("WIC_NBR", col('WIC_NBR').cast(IntegerType())).select("ARTICLE_NBR", "SKU_ID", "WIC_NBR","UPC_WALG").distinct()
// MAGIC
// MAGIC # For PROD_CAT_NBR
// MAGIC df_PROD_UPC= spark.read.load("abfss://merchscmprod@prodretailmchpadls01.dfs.core.windows.net/delta/cooked/DIM_PROD_UPC").filter(col('UPC_NBR').isNotNull()).filter(col('PROD_CAT_NBR').isNotNull()).select("UPC_NBR", "PROD_CAT_NAME","PROD_CAT_NBR").distinct()
// MAGIC
// MAGIC # Store number mapping
// MAGIC df_store = spark.read.load("abfss://curated@prodretailmchpadls01.dfs.core.windows.net/retail/xref/DIM_XREF_SITE").withColumn("ZLOC_NUM", col('ZLOC_NUM').cast(IntegerType())).select("ZWERKS", "ZLOC_NUM").distinct()
// MAGIC
// MAGIC # UPC number & WIC mapping
// MAGIC df_XREF = spark.read.load("abfss://curated@prodretailmchpadls01.dfs.core.windows.net/retail/xref/DIM_XREF_ARTICLE").withColumn('UPC_NBR', col('ZUPC').cast('long')).withColumn("WIC_NBR", col('ZWIC').cast(IntegerType())).select(col("ZARTICLE").alias("ARTICLE_NBR"), "ZUPCWOCHECK","UPC_NBR", "WIC_NBR").distinct()
// MAGIC

// COMMAND ----------

// MAGIC %md
// MAGIC # Transaction Table:-

// COMMAND ----------

// MAGIC %python
// MAGIC df_transactions_loyalty = df_transactions.filter((col("RECORDTYP") == 1000) & (trim(col("Loyalty_id")) != ""))
// MAGIC df_transactions = df_transactions.filter(col("RECORDTYP") == 1100).filter(col("UNIT_QTY") >= 0).drop("Loyalty_id")

// COMMAND ----------

// MAGIC %python
// MAGIC result_df = df_transactions.alias("tr") \
// MAGIC             .join(df_transactions_loyalty.alias("ly"), on=['RFN', 'STORE_NBR', 'SALES_TXN_DT'], how='inner') \
// MAGIC             .select(col("tr.SALES_TXN_DT"), \
// MAGIC                     col("tr.ITEM_UNIT_PRICE_DLRS"), \
// MAGIC                     col("tr.RFN"), \
// MAGIC                     col("tr.UNIT_QTY"), \
// MAGIC                     col("tr.UPC_NBR"), \
// MAGIC                     col("tr.STORE_NBR"), \
// MAGIC                     col("tr.ARTICLE_NBR"), \
// MAGIC                     col("tr.LINE_ITEM_SEQ_NBR"), \
// MAGIC                     col("ly.loyalty_id").alias("LOYALTY_ID")).dropDuplicates()
// MAGIC
// MAGIC # Join the DataFrames on the common column ZWERKS
// MAGIC joined_df = result_df.join(df_store, result_df.STORE_NBR == df_store.ZWERKS, "left_outer")
// MAGIC
// MAGIC # Select ZLOC_NUM column and rename it to store_nbr
// MAGIC result_df = joined_df.select(result_df["*"], df_store.ZLOC_NUM.cast(IntegerType()).alias("store_nbrInt")).drop("STORE_NBR")
// MAGIC             
// MAGIC df_transactions = result_df.filter(
// MAGIC     col("SALES_TXN_DT").isNotNull() &
// MAGIC     col("ITEM_UNIT_PRICE_DLRS").isNotNull() &
// MAGIC     col("RFN").isNotNull() &
// MAGIC     col("UNIT_QTY").isNotNull() &
// MAGIC     col("UPC_NBR").isNotNull() &
// MAGIC     col("store_nbrInt").isNotNull() &
// MAGIC     col("ARTICLE_NBR").isNotNull() &
// MAGIC     col("LINE_ITEM_SEQ_NBR").isNotNull() &
// MAGIC     col("LOYALTY_ID").isNotNull()
// MAGIC ).withColumnRenamed('store_nbrInt', 'STORE_NBR')

// COMMAND ----------

// MAGIC %md
// MAGIC ### Aggregation of QTY

// COMMAND ----------

// MAGIC %python
// MAGIC selected_cols = ["RFN", "UPC_NBR", "SALES_TXN_DT", "LINE_ITEM_SEQ_NBR", "STORE_NBR"]
// MAGIC filtered_df = df_transactions.dropDuplicates(selected_cols)

// COMMAND ----------

// MAGIC %python
// MAGIC aggregated_df = filtered_df.groupBy("RFN", "UPC_NBR", "SALES_TXN_DT", "ITEM_UNIT_PRICE_DLRS", "STORE_NBR") \
// MAGIC     .agg(spark_sum("UNIT_QTY").alias("QTY"))
// MAGIC
// MAGIC # Then, join the aggregated data back to the original DataFrame to get other columns
// MAGIC df_transactions = filtered_df.drop("UNIT_QTY","LINE_ITEM_SEQ_NBR").join(aggregated_df, ["RFN", "UPC_NBR", "SALES_TXN_DT", "ITEM_UNIT_PRICE_DLRS", "STORE_NBR"]).distinct()

// COMMAND ----------

// MAGIC %md
// MAGIC ### Map LoyaltyID with eComm_ID

// COMMAND ----------

// MAGIC %python
// MAGIC df_consumer_src_mid = spark.read.load("abfss://customer-phi@dlxprodcrtmstrdatsa00.dfs.core.windows.net/consumer_src_mid").filter(col("dna_end_dttm").cast("date") == "9999-12-31")

// COMMAND ----------

// MAGIC %python
// MAGIC # 1. Filter cust_src_cd = "LR" and get mid & cust_src_id as LR_ID
// MAGIC lr_df = df_consumer_src_mid.filter(col("cust_src_cd") == "LR") \
// MAGIC                            .select("mid", "cust_src_id") \
// MAGIC                            .withColumnRenamed("cust_src_id", "LR_ID")
// MAGIC
// MAGIC # 2. Filter cust_src_cd = "EC", for same mid get cust_src_id, and name that column as eComm_ID
// MAGIC #    Also, ensure to select the latest cust_src_id based on dna_update_dttm
// MAGIC window_spec = Window.partitionBy("mid").orderBy(desc("dna_update_dttm"))
// MAGIC
// MAGIC ec_df = df_consumer_src_mid.filter(col("cust_src_cd") == "EC") \
// MAGIC                            .withColumn("rn", row_number().over(window_spec)) \
// MAGIC                            .filter(col("rn") == 1) \
// MAGIC                            .select("mid", "cust_src_id") \
// MAGIC                            .withColumnRenamed("cust_src_id", "eComm_ID")
// MAGIC
// MAGIC # 3. Join LR and EC DataFrames and select unique LR_ID with unique eComm_ID
// MAGIC joined_df = lr_df.join(ec_df, "mid", "left")
// MAGIC
// MAGIC # 4. Remove records which we couldn't find eComm_ID
// MAGIC filtered_joined_df = joined_df.filter(col('eComm_ID').isNotNull())

// COMMAND ----------

// MAGIC %python
// MAGIC joined_df = df_transactions.join(filtered_joined_df, df_transactions.LOYALTY_ID == filtered_joined_df.LR_ID, "left_outer")
// MAGIC _df = joined_df.withColumn("eComm_IDMatch", when(joined_df.LOYALTY_ID == joined_df.LR_ID, joined_df.eComm_ID).otherwise(None))

// COMMAND ----------

// MAGIC %python
// MAGIC df_transactions = _df.filter(col("eComm_IDMatch").isNotNull()).drop("eComm_ID","LR_ID","mid").withColumnRenamed("eComm_IDMatch", "ECOMM_ID")

// COMMAND ----------

// MAGIC %md
// MAGIC # Product Table:-

// COMMAND ----------

// MAGIC %python
// MAGIC df = df_master.alias("ma").join(df_XREF.alias("xref"), on=['ARTICLE_NBR', 'UPC_NBR'], how='inner') \
// MAGIC                           .join(df_PROD_UPC.alias("upc"), col("xref.ZUPCWOCHECK") == col("upc.UPC_NBR"), "inner") \
// MAGIC                           .select(col("ma.ARTICLE_NBR"), \
// MAGIC                                   col("ma.UPC_NBR"), \
// MAGIC                                   col("ma.opStudy"), \
// MAGIC                                   col("xref.WIC_NBR"), \
// MAGIC                                   col("xref.ZUPCWOCHECK"), \
// MAGIC                                   col("upc.PROD_CAT_NBR"), \
// MAGIC                                   col("upc.PROD_CAT_NAME") \
// MAGIC                           ).dropDuplicates()

// COMMAND ----------

// MAGIC %python
// MAGIC # df = df.alias("ma").join(df_sku.alias("sku"), on=['WIC_NBR', 'ARTICLE_NBR'], how='inner') \
// MAGIC #                           .select("ma.*", "sku.SKU_ID", "sku.UPC_WALG").dropDuplicates()

// COMMAND ----------

// MAGIC %md
// MAGIC # Product Table - Transaction Table

// COMMAND ----------

// MAGIC %python
// MAGIC df_daily = df_transactions.join(df, on=['ARTICLE_NBR', 'UPC_NBR'], how='inner').distinct()

// COMMAND ----------

// MAGIC %md
// MAGIC # Get SKU

// COMMAND ----------

// MAGIC %python
// MAGIC df_daily = df_daily.alias("ma").join(df_sku.alias("sku"), on=['WIC_NBR', 'ARTICLE_NBR'], how='inner') \
// MAGIC                           .select("ma.*", "sku.SKU_ID", "sku.UPC_WALG").dropDuplicates()

// COMMAND ----------

// MAGIC %md
// MAGIC # Filter Out Security concerns

// COMMAND ----------

// MAGIC %python
// MAGIC df_ccpa = spark.read.load("abfss://customer-pii@dlxprodcrtmstrdatsa01.dfs.core.windows.net/ccpa_data").filter(col("srccode") == 'LR')
// MAGIC
// MAGIC # Exclusion of all transactions in Washington State 
// MAGIC df_dim_location_store = spark.read.load("abfss://location-pii@dlxprodcrtmstrdatsa01.dfs.core.windows.net/dim_location_store").filter(col("edw_rec_end_dt") == '9999-12-31').filter(~(lower(col("store_str_city")).like("%washington%"))).filter(~(col("store_str_state_cd") == 'WA'))
// MAGIC
// MAGIC # Exclusion of all customers that are residents of Washington state, as set within their customer profiles
// MAGIC df_customer_address = spark.read.load("abfss://customer-pii@dlxprodcrtmstrdatsa01.dfs.core.windows.net/customer_address").filter(col("edw_rec_end_dt") == '9999-12-31').filter(col("src_sys_cd") == 'LR').filter(~(lower(col("state_cd")) == 'wa'))

// COMMAND ----------

// MAGIC %python
// MAGIC df_daily_transactions = df_daily.filter(~(col("opStudy").isin(74, 174, 214, 37, 212, 15))).filter(~((col("PROD_CAT_NAME").like('%PRENATAL%')) | \
// MAGIC                                           (col("PROD_CAT_NBR").isin(1007, 907907, 908908, 73015, 73016, 74001, 74002, 74003, 74004, 74005, 74006, 74007, 74008, 74009, 74010, 74011, 74012, 74013, 74014, 74015, 74016, 74017, 74018, 74019, 174001, 174003, 174006, 174007, 174008, 174010, 174013, 174016, 174017, 192011, 212001, 212002, 212003, 212004, 212005, 212006, 212007, 212008, 212010, 212011, 212012, 212013, 212014, 212068, 212069, 214001, 214002, 214003, 214004, 214005, 214006, 214007,35013, 35021, 129006, 136002, 46001, 46017, 46015, 213012, 50022, 3045, 153008, 85006, 3115, 57002, 2054, 165004, 197122, 2090, 138006, 19119, 173130, 117010, 90067, 25006, 9001, 70010, 138022, 90007, 176028, 82033, 3110, 39012, 199199, 4092, 14008, 104108, 902902, 145008, 205010, 133007))))

// COMMAND ----------

// MAGIC %python
// MAGIC df_daily_transactions = df_daily_transactions.alias("pt").join(df_dim_location_store.alias("loc"), col("pt.STORE_NBR") == col("loc.store_nbr"), "inner") \
// MAGIC                         .join(df_customer_address.alias("cad"), col("pt.LOYALTY_ID") == col("cad.cust_src_id"), "inner") \
// MAGIC                         .select("pt.*").dropDuplicates()

// COMMAND ----------

// MAGIC %python
// MAGIC final_df = df_daily_transactions.alias("pos").join(
// MAGIC     df_ccpa.alias("ccpa"), (col("pos.LOYALTY_ID") == col("ccpa.srcid")),
// MAGIC     "left_anti"  # This will keep only the records in df_daily_transactions that are not in df_ccpa
// MAGIC ).filter((col("LOYALTY_ID").isNotNull()) & (col("LOYALTY_ID") != ""))

// COMMAND ----------

// %python
// # # Check Security requirements; excluded or not

// # List of ops_dept_nbr and prod_cat_nbr to filter
// ops_dept_nbr_list = [74, 174, 214, 37, 212, 15]
// prod_cat_nbr_list = [1007, 907907, 908908, 73015, 73016, 74001, 74002, 74003, 74004, 74005, 74006, 74007, 74008, 74009, 74010, 74011, 74012, 74013, 74014, 74015, 74016, 74017, 74018, 74019, 174001, 174003, 174006, 174007, 174008, 174010, 174013, 174016, 174017, 192011, 212001, 212002, 212003, 212004, 212005, 212006, 212007, 212008, 212010, 212011, 212012, 212013, 212014, 212068, 212069, 214001, 214002, 214003, 214004, 214005, 214006, 214007, 35013, 35021, 129006, 136002, 46001, 46017, 46015, 213012, 50022, 3045, 153008, 85006, 3115, 57002, 2054, 165004, 197122, 2090, 138006, 19119, 173130, 117010, 90067, 25006, 9001, 70010, 138022, 90007, 176028, 82033, 3110, 39012, 199199, 4092, 14008, 104108, 902902, 145008, 205010, 133007]

// # Count records where ops_dept_nbr is in the list
// count_ops_dept_nbr = final_df.filter(col("opStudy").isin(ops_dept_nbr_list)).count()
// print(f"Count of records where ops_dept_nbr is in the list: {count_ops_dept_nbr}")

// # Count records where prod_cat_nbr is in the list
// count_prod_cat_nbr = final_df.filter(col("PROD_CAT_NBR").isin(prod_cat_nbr_list)).count()
// print(f"Count of records where prod_cat_nbr is in the list: {count_prod_cat_nbr}")

// # Count records where lower(store_str_city) contains 'washington'
// # count_store_str_city_washington = final_df.filter(lower(col("store_str_city")).like("%washington%")).count()
// # print(f"Count of records where store_str_city contains 'washington': {count_store_str_city_washington}")


// COMMAND ----------

// MAGIC %python
// MAGIC final_df = final_df.select(
// MAGIC     col("ECOMM_ID").alias("user_id"),
// MAGIC     col("RFN").alias("event_id"),
// MAGIC     col("QTY").alias("event_item_quantity"),
// MAGIC     col("SKU_ID").alias("event_item_id"),
// MAGIC     col("ITEM_UNIT_PRICE_DLRS").alias("event_item_price"),
// MAGIC     col("SALES_TXN_DT").alias("event_timestamp"),
// MAGIC     col("STORE_NBR").alias("store_id")
// MAGIC ).dropDuplicates()

// COMMAND ----------

// MAGIC %python
// MAGIC # final_df.display()

// COMMAND ----------

// MAGIC %python
// MAGIC # revenue_df = final_df.withColumn("total", col("event_item_quantity") * col("event_item_price"))

// COMMAND ----------

// %python
// revenue_df.groupBy('event_timestamp') \
//                     .agg({'*': 'count', 'total': 'sum'}) \
//                     .withColumnRenamed('count(1)', 'count') \
//                     .withColumnRenamed('sum(total)', 'total_sum').display()

// COMMAND ----------

// MAGIC %python
// MAGIC final_df.createOrReplaceTempView("final_df")

// COMMAND ----------

// MAGIC %md
// MAGIC # Voltage Encryption

// COMMAND ----------

val encryptedDataDF = spark.sql("SELECT * FROM final_df")
.withColumn("user_id_Encrypted", ve.encryptUDF(lit("1"), col("user_id")))

display(encryptedDataDF)

// COMMAND ----------

encryptedDataDF.createOrReplaceTempView("encryptedDataDF")

// COMMAND ----------

// MAGIC %md
// MAGIC # Base64 Encoding

// COMMAND ----------

// MAGIC %python
// MAGIC from typing import Set
// MAGIC from pyspark.sql.functions import base64, col
// MAGIC from pyspark.sql import DataFrame

// COMMAND ----------

// MAGIC %python
// MAGIC def encode_base64(df: DataFrame, columns: list) -> DataFrame:
// MAGIC     """
// MAGIC     Encode specified columns in a DataFrame using Base64 encoding.
// MAGIC     
// MAGIC     Parameters:
// MAGIC         df (DataFrame): Input DataFrame.
// MAGIC         columns (list): List of column names to be encoded.
// MAGIC     
// MAGIC     Returns:
// MAGIC         DataFrame: DataFrame with specified columns encoded using Base64.
// MAGIC     """
// MAGIC     for column in columns:
// MAGIC         df = df.withColumn(column, base64(col(column).cast("string")))
// MAGIC     return df

// COMMAND ----------

// MAGIC %python
// MAGIC encryptedDataDF = spark.sql("SELECT * FROM encryptedDataDF")
// MAGIC
// MAGIC # Specify columns to encode using Base64
// MAGIC columns_to_encode = ["user_id_Encrypted"]
// MAGIC
// MAGIC final_df_encode = encode_base64(encryptedDataDF, columns_to_encode)
// MAGIC

// COMMAND ----------

// MAGIC %python
// MAGIC #final_df_encode.display()

// COMMAND ----------

// MAGIC %python
// MAGIC # Drop the user_id column
// MAGIC final_df_without_user_id = final_df_encode.drop("user_id")
// MAGIC
// MAGIC # Rename user_id_Encrypted to user_id
// MAGIC final_df_renamed = final_df_without_user_id.withColumnRenamed("user_id_Encrypted", "user_id").dropDuplicates()

// COMMAND ----------

// %python
// final_df_renamed.display()

// COMMAND ----------

// %python
// final_df_renamed.groupBy("event_timestamp") \
//                      .agg(countDistinct(col("event_id"))) \
//                      .orderBy("event_timestamp").display()

// COMMAND ----------

// %python
// final_df_renamed.groupBy('event_timestamp').count().display()

// COMMAND ----------

// MAGIC %md
// MAGIC # Saving csv file

// COMMAND ----------

// MAGIC %python
// MAGIC # Setting Azure path where csv is exported
// MAGIC current_date = date.today().strftime("%Y%m%d")
// MAGIC _out = 'Walgreens_49240_offline_sales'
// MAGIC _csvfile = f'{_out}_{current_date}'

// COMMAND ----------

// MAGIC %python
// MAGIC # Archive existing files 
// MAGIC existing_folder_path = "abfss://ds-tdtrans-output@proddseus2tdtranssa01.dfs.core.windows.net/tmp_tables/latest/"
// MAGIC archive_folder_path = "abfss://ds-tdtrans-output@proddseus2tdtranssa01.dfs.core.windows.net/tmp_tables/archive/"
// MAGIC fileList = dbutils.fs.ls(f"{existing_folder_path}")
// MAGIC currentfile_names = [_file.name for _file in fileList]
// MAGIC files =  [f"{_out}"]
// MAGIC files_to_archive = [s for s in currentfile_names if any(sub in s for sub in files)]
// MAGIC
// MAGIC
// MAGIC for item in files_to_archive:
// MAGIC     existing_file_path = os.path.join(existing_folder_path, item)
// MAGIC     # Move existing file to the archive folder
// MAGIC     archive_file_path = os.path.join(archive_folder_path, item)
// MAGIC     dbutils.fs.mv(existing_file_path, archive_file_path, recurse=True)

// COMMAND ----------

// MAGIC %python
// MAGIC # Write csv file to location
// MAGIC def write_table_to_file(table, container, storage_account, out_name = None, filetype = '.dat', delimiter = '|', audit_file = False, audit_prefix = '.ok', debug = True):
// MAGIC   if out_name is None:
// MAGIC       in_name = var_name(table)
// MAGIC       out_name = in_name
// MAGIC
// MAGIC   write_path = "abfss://" + container + "@" + storage_account + ".dfs.core.windows.net/" + out_name 
// MAGIC   table.coalesce(1).write.mode("overwrite").option("header", "true").option("sep", delimiter).format("com.databricks.spark.csv").save(write_path)
// MAGIC
// MAGIC   files = dbutils.fs.ls(write_path)
// MAGIC   output_file = [x for x in files if x.name.startswith("part-")]
// MAGIC
// MAGIC   dbutils.fs.mv(output_file[0].path, write_path + filetype)    
// MAGIC   
// MAGIC   if audit_file:
// MAGIC       # write the .ok file
// MAGIC       dbutils.fs.put(write_path + audit_prefix, str(table.count() + 1))
// MAGIC   
// MAGIC   dbutils.fs.rm(write_path + "/", recurse = True)
// MAGIC
// MAGIC   if debug:
// MAGIC       print(out_name + " has been written to " + write_path + filetype + ".")
// MAGIC   return()

// COMMAND ----------

// MAGIC %python
// MAGIC final_df_renamed.createOrReplaceTempView("final_df_renamed")

// COMMAND ----------

// MAGIC %python
// MAGIC # Writing csv file to loc example: abfss://ds-tdtrans-output@proddseus2tdtranssa01.dfs.core.windows.net/tmp_tables/protected_WAG_CNVR_sales_feed_criteo_14122023.csv
// MAGIC write_table_to_file(final_df_renamed, container = "ds-tdtrans-output", storage_account = "proddseus2tdtranssa01", out_name = f"tmp_tables/latest/{_csvfile}", filetype = '.csv', delimiter = '|', audit_file = False, audit_prefix = '.ok', debug = True)
// MAGIC
