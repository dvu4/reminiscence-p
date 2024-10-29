# Databricks notebook source
# DBTITLE 1,Parameters For Env & Reject Threshold
environment = dbutils.widgets.get("environment")
RejectThreshold= int(dbutils.widgets.get("RejectThreshold"))
strgPthNotation = dbutils.widgets.get("strgPthNotation")

# COMMAND ----------

# DBTITLE 1,Import Voltage Lib & UDF 
# MAGIC %scala
# MAGIC import com.wba.idi20.voltage.VoltageHelper
# MAGIC import com.wba.idi20.voltage.VoltageEncryption
# MAGIC val ve = new VoltageEncryption("dapadbscope","VoltageKeyManagerUrl","voltagePolicyServerUrl","VoltageKeyVaultSecretScopeName","VoltageKeyVaultSecretName")
# MAGIC spark.udf.register("decryptFpeUDF", ve.decryptFpeUDF)
# MAGIC spark.udf.register("encryptFpeUDF", ve.encryptFpeUDF)

# COMMAND ----------

# DBTITLE 1,ADLS Connection & Custom Functions
# MAGIC %run ./IQVIA_Custom_Functions

# COMMAND ----------

# DBTITLE 1,Transformation & Voltage Decryption Function
#Logic for transformation of all fields.
def Transformation(df):
  df.createOrReplaceTempView("Finalview")
  diagnosis_CodeRegex="[^0-9A-Za-z ./-]"
  crg="[^0-9A-Za-z. /-]"
  crgsubmitGRPID="[^0-9A-Za-z]"

  zipSupDfPath=f"abfss://rtl-iqvia-lookup@dlx{environment}semprtoutsa01.dfs.core.windows.net/in/zip_suppress.csv"
  
  zipSupDf= spark.read.option("header",True).csv(zipSupDfPath)
  zipSupDf.createOrReplaceTempView("zipSuppressView")
 

  TransformDF=spark.sql("select lpad(nvl(cast(ltrim(regexp_replace(relocate_fm_str_nbr,'"+crg+"',''))  as string), ''),5,'0')  as wag_original_store_id,\
  lpad(nvl(cast(ltrim(regexp_replace(str_nbr,'"+crg+"',''))  as string), ''), 5, 0) as wag_current_store_id,\
  case when rx_nbr is null then lpad(nvl(cast(ltrim(regexp_replace(rx_nbr,'"+crg+"','')) +'152' as bigint), ''), 15, '0') \
    else lpad(nvl(cast(ltrim(regexp_replace(decryptFpeUDF(3000,rx_nbr),'"+crg+"','')) +'152' as bigint), ''), 15, '0') \
  end as prescription_ref_no_402d2_test,\
  lpad(nvl(cast(ltrim(regexp_replace(rx_fill_nbr,'"+crg+"','')) as string), ''), 3, '0') as  wag_rx_fill_nbr,\
  lpad(nvl(date_format(fill_enter_dt, 'yyyyMMdd'), ''), 8, ' ') as date_authorized,\
  lpad(nvl(date_format(fill_enter_tm,'HH:mm:ss'), ''), 8, ' ') as time_authorized,\
  lpad(nvl(date_format(fill_sold_dt, 'yyyyMMdd'), ''), 8, ' ') as service_date_401d1,\
  lpad(nvl(date_format(fill_sold_tm, 'HH:mm:ss'), ''), 8, ' ') as wag_fill_sold_tm,\
  rpad(nvl(trim(cast(conv(regexp_replace(pat_id,'"+crg+"','') ,10,16) as string)), ''), 11, ' ') as de_identified_patient_code,\
  rpad(nvl(regexp_replace(fill_stat_cd,'"+crg+"',''), ''), 2, ' ') as wag_fill_stat_cd,\
  case when dspn_fill_nbr  = 0 then '00'\
    when (dspn_fill_nbr >= 1) and (dspn_fill_nbr <= 100) then lpad(cast(dspn_fill_nbr-1 as integer),2,'0')\
    when dspn_fill_nbr >100 then '99'\
    when dspn_fill_nbr is null then lpad('',2,'0')\
  end as fill_num_403d3,\
  case when fill_qty_dspn is null then lpad('',10,' ')\
    else concat(format_string('%07d',cast(fill_qty_dspn as integer)),lpad(substring(fill_qty_dspn, instr(fill_qty_dspn,'.')+1), 3, '0'))\
  end  as   quantity_dispensed_442e7,\
  case when fill_days_supply is null then lpad('',3,' ')\
    else lpad(regexp_replace(fill_days_supply,'"+crg+"',''),3,'0')\
  end  as   days_supply_405d5,\
  case when fill_type_cd is null then lpad('',1,' ')\
    else ltrim(regexp_replace(fill_type_cd,'"+crg+"',''))\
  end as    prescription_type_60149,\
  case when partial_fill_cd is null then lpad('',1,' ')\
    else ltrim(regexp_replace(partial_fill_cd,'"+crg+"',''))\
  end  as   partial_fill_cd,\
  lpad(nvl(date_format(fill_vrfy_dt, 'yyyyMMdd'), ''), 8, ' ') as wag_fill_vrfy_dt,\
  lpad(nvl(date_format(fill_vrfy_tm, 'HH:mm:ss'), ''), 8, ' ') as wag_fill_vrfy_tm,\
  lpad(nvl(date_format(fill_data_review_dt, 'yyyyMMdd'), ''), 8, ' ') as wag_fill_data_review_dt,\
  lpad(nvl(date_format(fill_data_review_tm, 'HH:mm:ss'), ''), 8, ' ') as wag_fill_data_review_tm,\
  lpad(nvl(date_format(filling_dt, 'yyyyMMdd'), ''), 8, ' ') as wag_filling_dt,\
  lpad(nvl(date_format(filling_tm, 'HH:mm:ss'), ''), 8, ' ') as wag_filling_tm,\
  lpad(nvl(date_format(fill_del_dt, 'yyyyMMdd'), ''), 8, ' ') as wag_fill_del_dt,\
  lpad(nvl(date_format(fill_del_tm, 'HH:mm:ss'), ''), 8, ' ') as wag_fill_del_tm,\
  rpad(nvl(ltrim(regexp_replace(return_plan_id,'"+crg+"','')), ''), 8, ' ') as plan_id_524fo,\
  lpad(nvl(ltrim(regexp_replace(cob_ind,'"+crg+"','')), ''), 1, ' ') as wag_cob_ind,\
  lpad(nvl(ltrim(regexp_replace(fill_adjud_cd,'"+crg+"','')), ''), 1, ' ') as trans_response_status_112an,\
  lpad(nvl(date_format(fill_adjud_dt, 'yyyyMMdd'), ''), 8, ' ') as wag_fill_adjud_dt,\
  lpad(nvl(date_format(fill_adjud_tm, 'HH:mm:ss'), ''), 8, ' ') as wag_fill_adjud_tm,\
  case when general_recipient_nbr is null then rpad('',21,' ')\
    else rpad(nvl(ltrim(regexp_replace(decryptFpeUDF(1,general_recipient_nbr),'"+crg+"','')), ''),21, ' ') \
  end as cardholder_id_302c2,\
  rpad(nvl(ltrim(regexp_replace(plan_group_nbr,'"+crgsubmitGRPID+"','')), ''),15, ' ') as submit_group_id,\
  case when plan_submit_cost_dlrs is null then lpad('',8,' ')\
  else concat(lpad((case when cast(plan_submit_cost_dlrs as integer) = 0 and cast(plan_submit_cost_dlrs as double) != 0 Then '' else  cast(plan_submit_cost_dlrs as integer) end ), 6,' '),lpad(cast(round(abs(plan_submit_cost_dlrs - cast(plan_submit_cost_dlrs as integer))*100) as integer), 2, '0'))\
  end  as   ingredient_cost_submit_409d9,\
  case when plan_submit_fee_dlrs is null then lpad('',8,' ')\
  else concat(lpad((case when cast(plan_submit_fee_dlrs as integer) = 0 and cast(plan_submit_fee_dlrs as double) != 0 Then '' else  cast(plan_submit_fee_dlrs as integer) end ), 6,' '),lpad(cast(round(abs(plan_submit_fee_dlrs - cast(plan_submit_fee_dlrs as integer))*100) as integer), 2, '0'))\
  end  as   dispensing_fee_submitted_412dc,\
  lpad(nvl(ltrim(regexp_replace(bin_nbr,'"+crg+"','')), ''),6, ' ') as bin_num_101a1,\
  rpad(nvl(ltrim(regexp_replace(reject_cd_1,'"+crg+"','')), ''),3, ' ') as reject_cd_1_511fb,\
  rpad(nvl(ltrim(regexp_replace(reject_cd_2,'"+crg+"','')), ''),3, ' ') as reject_cd_2_511fb,\
  rpad(nvl(ltrim(regexp_replace(reject_cd_3,'"+crg+"','')), ''),3, ' ') as reject_cd_3_511fb,\
  lpad(nvl(cast(ltrim(regexp_replace(max_reject_nbr,'"+crg+"',''))as string), ''),2, ' ') as reject_count_510fa,\
  lpad(nvl(date_format(rx_written_dt, 'yyyyMMdd'), ''), 8, ' ') as date_prescript_written_414de,\
  case when abs(fill_nbr_prescribed) >99 then '99'\
    when abs(fill_nbr_prescribed) <=99 then lpad(cast (abs(fill_nbr_prescribed) as integer),2,0)\
    when fill_nbr_prescribed is null then lpad('',2,0)\
  end  as num_refills_auth_415df,\
  case when rx_orig_qty is null then lpad('',10,'0')\
    else concat(format_string('%07d',cast(rx_orig_qty as integer)),lpad(substring(rx_orig_qty, instr(rx_orig_qty,'.')+1), 3, '0'))\
  end  as   orig_presc_quantity_446eb,\
  case when drug_non_sys_cd is null or drug_non_sys_cd  = 'N' then '1'\
    when drug_non_sys_cd  = 'C' then '2'\
    else ' '\
  end  as compound_cd_406d6,\
  case when rx_daw_cd = '' or rx_daw_cd is null then ' '\
    else ltrim(regexp_replace(rx_daw_cd,'"+crg+"',''))\
  end  as daw_prod_selection_cd_408d8,\
  rpad(nvl(ltrim(regexp_replace(prcs_ctrl_nbr,'"+crg+"','')), ''),10, ' ') as processor_ctrl_num_104a4,\
  case when fill_sold_dlrs is null then '0'\
    else '1' \
  end  as fill_sold_dlrs_ind,\
  case when origin_cd is null then '0'\
    else ltrim(regexp_replace(origin_cd,'"+crg+"',''))\
  end  as prescription_origin_code,\
  rpad(nvl(ltrim(regexp_replace(diagnosis_cd_1,'"+crg+"','')), ''),15, ' ') as diagnosis_code,\
  case when diagnosis_cd_qlfr is null or diagnosis_cd_qlfr  = '' then '00'\
    else regexp_replace(ltrim(diagnosis_cd_qlfr),'"+crg+"','')\
  end  as diagnosis_code_qualifier,\
  case when free_goods_ind = 'Y' then  lpad('', 8,0)\
    when free_goods_ind = 'N' and plan_copay_dlrs is null then lpad('',8,' ')\
    else concat(lpad((case when cast(plan_copay_dlrs as integer) = 0 and cast(plan_copay_dlrs as double) != 0 Then '' else  cast(plan_copay_dlrs as integer) end ), 6,' '),lpad(cast(round(abs(plan_copay_dlrs - cast(plan_copay_dlrs as integer))*100) as integer), 2, '0'))\
  end  as   patient_pay_amount,\
  lpad('', 8)  as plan_short_name,\
  lpad('', 15) as processor_name,\
  lpad('', 10) as carrier_id ,\
  case when benefit_stg_1_qlfr_cd is null then 'N' else 'Y' end  as medicare_part_d_flag,\
  lpad('', 1) as medical_benefit_claim_indicator,\
  lpad('', 8) as ingredient_cost_paid,\
  lpad('', 8) as dispensing_fee_paid,\
  lpad('', 2) as basis_of_ingredient_cost_submit,\
  case when basis_of_reimb_detrm is null then '00'\
    else rpad(trim(regexp_replace(basis_of_reimb_detrm,'"+crg+"','')), 2, ' ')\
  end  as basis_of_ingredient_cost_reimbursed,\
  case when cob_ind = 'Y' then '01'\
    else lpad('', 2, ' ')\
  end  as ims_coordination_of_benefits_counter,\
  case when free_goods_ind = 'Y' then  lpad('', 8,0)\
  when free_goods_ind = 'N' and fill_awp_cost_dlrs is null then lpad('',8,' ')\
    else concat(lpad((case when cast(fill_awp_cost_dlrs as integer) = 0 and cast(fill_awp_cost_dlrs as double) != 0 Then '' else  cast(fill_awp_cost_dlrs as integer) end ), 6,' '),lpad(cast(round(abs(fill_awp_cost_dlrs - cast(fill_awp_cost_dlrs as integer))*100) as integer), 2, '0'))\
  end  as   total_transaction_price_collected,\
  case when free_goods_ind = 'Y' then  lpad('', 8,0)\
  when free_goods_ind = 'N' and plan_return_copay_dlrs is null then lpad('',8,' ')\
    else concat(lpad((case when cast(plan_return_copay_dlrs as integer) = 0 and cast(plan_return_copay_dlrs as double) != 0 Then '' else  cast(plan_return_copay_dlrs as integer) end ), 6,' '),lpad(cast(round(abs(plan_return_copay_dlrs - cast(plan_return_copay_dlrs as integer))*100) as integer), 2, '0'))\
  end  as   amount_of_copay_coinsurance,\
  case when upper(fill_pay_method_cd) in ('C', 'O') then '1'\
    when substring(third_party_plan_id, 3, 3) = 'DPA'\
      or substring(third_party_plan_id, 3, 3) = 'MED'\
      or substring(third_party_plan_id, 4, 2) = 'PA'\
      or substring(third_party_plan_id, 3, 3) = 'PA ' \
      or substring(third_party_plan_id, 1, 3) = 'GAM'\
      or substring(third_party_plan_id, 1, 3) = 'MAM'\
      then '2'\
    else '3'\
  end as ims_payment_type,\
  case when fill_del_dt is null then 'D'\
    else 'R'\
  end as ims_claim_indicator,\
  rpad(nvl(ltrim(regexp_replace(third_party_plan_id,'"+crg+"','')), ''),9, ' ') as supplier_plan_code,\
  rpad(nvl(ltrim(regexp_replace(ntwrk_remb_return_id,'"+crg+"','')), ''),10, ' ') as returned_network_reimbursement_id_545_2f,\
  lpad(nvl(ltrim(regexp_replace(relation_cd_pit,'"+crg+"','')), ''),1, ' ') as patient_rel_to_ins_306_c6,\
  case when pat_first_name_pit is null then rpad('',12,' ')\
    else rpad(nvl(ltrim(regexp_replace(decryptFpeUDF(1,pat_first_name_pit),'"+crg+"','')), ''),12, ' ') \
  end as first_name_encrypted,\
  case when pat_first_name_pit is null then rpad('',15,' ')\
    else rpad(nvl(ltrim(regexp_replace(decryptFpeUDF(1,pat_last_name_pit),'"+crg+"','')), ''),15, ' ') \
  end as last_name_encrypted,\
  case when  pat_brth_dt_pit  is null or date_format(pat_brth_dt_pit, 'yyyyMMdd') ='' then rpad('',4,' ')\
    when cast(abs(datediff(pat_brth_dt_pit, CURRENT_DATE()))/365.25 as integer) >89 then '0000'\
    else cast(year(pat_brth_dt_pit) as varchar(4))\
  end  as brth_dt,\
  case when pat_gndr_cd_pit = 'M' then '1'\
    when pat_gndr_cd_pit = 'F' then '2'\
    else '3' \
  end  as patient_gender_305c5,\
  case when pat_address_line_pit is null then rpad('',30,' ')\
    else rpad(nvl(ltrim(regexp_replace(decryptFpeUDF(1,pat_address_line_pit),'"+crg+"','')), ''),30, ' ') \
  end as patient_street_address,\
  rpad(nvl(ltrim(regexp_replace(pat_state_cd_pit,'"+crg+"','')), ''),2, ' ') as patient_state_province_324_co,\
  case when trim(pat_zip_cd_5_pit) is null then lpad('', 5, ' ')\
    when trim(zip_3) is null then substring(decryptFpeUDF(1,pat_zip_cd_5_pit), 1, 3)||'00'\
	  else '00000'\
	end as patient_zip_325_cp,\
  case when ltrim(eid_cur) is null then rpad('', 20, ' ')\
    else rpad(ltrim(cast (eid_cur +'845' as bigint)),20, ' ')\
  end  as wag_e_id,\
  case when pat_zip_cd_5_pit is null then rpad(nvl(ltrim(regexp_replace((pat_zip_cd_5_pit),'"+crg+"','')), ''),5, ' ') \
    else rpad(nvl(ltrim(regexp_replace(decryptFpeUDF(1,pat_zip_cd_5_pit),'"+crg+"','')), ''),5, ' ') \
  end as patient_zip,\
  rpad(nvl(date_format(pat_brth_dt_pit, 'yyyyMMdd'), ''), 10, ' ') as patient_date_of_birth,\
  case when pbr_dea_nbr is null then rpad('', 9, ' ')\
    when pbr_state_cd_pit is null or upper(pbr_state_cd_pit) in ('WI', 'MN') then rpad('', 9, ' ')\
    else rpad (trim(regexp_replace(pbr_dea_nbr,'"+crg+"','')), 9, ' ')\
  end  as prescriber_dea,\
  lpad('12',2, ' ') as prescriber_id_qualifer,\
  rpad(nvl(ltrim(regexp_replace(pbr_last_name_pit,'"+crg+"','')), ''),15, ' ') as prescriber_last_name_427_dr,\
  rpad(nvl(ltrim(regexp_replace(pbr_npi_pit,'"+crg+"','')), ''),15, ' ') as prescriber_npi,\
  rpad(nvl(ltrim(regexp_replace(pbr_first_name_pit,'"+crg+"','')), ''),12, ' ') as prescriber_first_name_364_2j,\
  rpad(nvl(ltrim(regexp_replace(pbr_middle_initial_pit,'"+crg+"','')), ''),1, ' ') as prescriber_middle_initial,\
  rpad(nvl(ltrim(regexp_replace(pbr_city_pit,'"+crg+"','')), ''),20, ' ') as prescriber_city_366_2m,\
  rpad(nvl(ltrim(regexp_replace(pbr_state_cd_pit,'"+crg+"','')), ''),2, ' ') as prescriber_state_367_2n,\
  rpad(nvl(ltrim(regexp_replace(pbr_zip_cd_5_pit,'"+crg+"','')), ''),5, ' ') as prescriber_zip5,\
  case when (drug_non_sys_cd is null or drug_non_sys_cd ='')\
        and (ndc11_cur is not null or ndc11_cur!='')\
        then rpad(ltrim(regexp_replace(ndc11_cur,'"+crg+"','')), 19, ' ' ) \
    else rpad ('',19, ' ') \
  end  as product_service_id_407_d7,\
  case when (case when (drug_non_sys_cd is null or drug_non_sys_cd ='')\
        and (ndc11_cur is not null or ndc11_cur!='')\
        then rpad(ltrim(regexp_replace(ndc11_cur,'"+crg+"','')), 19, ' ' )\
    else rpad ('',19, ' ')\
  end ) is null then lpad('',2,' ')\
  when trim(case when (drug_non_sys_cd is null or drug_non_sys_cd ='')\
        and (ndc11_cur is not null or ndc11_cur!='')\
        then rpad(ltrim(regexp_replace(ndc11_cur,'"+crg+"','')), 19, ' ' )\
    else rpad ('',19, ' ')\
  end ) !='' then 'N '\
    else 'Z '\
  end as product_service_id_qual_436_e1,\
  rpad(nvl(ltrim(regexp_replace(pkg_sz_uom_cur,'"+crg+"','')), ''),2, ' ') as unit_of_measure_600_28,\
  rpad(nvl(ltrim(regexp_replace(prod_name_cur,'"+crg+"','')), ''),28, ' ') as dispensed_drug_name,\
  rpad(nvl(ltrim(regexp_replace(store_nabp_nbr_cur,'"+crg+"','')), 'PATIENT_ZIP'),7, ' ') as service_provider_id_201_b1,\
  case when ldb_store_type_cur is null then ' '\
    when ldb_store_type_cur = '04' then 'M'\
    else 'R'\
  end  as wag_mail_retail_ind,\
  rpad(nvl(ltrim(store_zip_cd_5), ''),5, ' ')  as pharmacy_zip_code,\
  rpad(nvl(ltrim(regexp_replace(store_npi_nbr_cur,'"+crg+"','')), ''),10, ' ') as store_npi_number,\
  lpad(nvl(ltrim(regexp_replace(benefit_stg_1_qlfr_cd,'"+crg+"','')), ''),2, ' ') as benefit_qualifier_1_394_mv,\
  lpad(nvl(ltrim(regexp_replace(benefit_stg_2_qlfr_cd,'"+crg+"','')), ''),2, ' ') as benefit_qualifier_2_394_mv,\
  lpad(nvl(ltrim(regexp_replace(benefit_stg_3_qlfr_cd,'"+crg+"','')), ''),2, ' ') as benefit_qualifier_3_394_mv,\
  lpad(nvl(ltrim(regexp_replace(benefit_stg_4_qlfr_cd,'"+crg+"','')), ''),2, ' ') as benefit_qualifier_4_394_mv,\
  case when fill_rtl_price_dlrs is null then lpad('',8,' ')\
  else concat(lpad((case when cast(fill_rtl_price_dlrs as integer) = 0 and cast(fill_rtl_price_dlrs as double) != 0 Then '' else  cast(fill_rtl_price_dlrs as integer) end ), 6,' '),lpad(cast(round(abs(fill_rtl_price_dlrs - cast(fill_rtl_price_dlrs as integer))*100) as integer), 2, '0'))\
  end  as   usual_and_customary_charge,\
  rpad(nvl(regexp_replace(return_plan_group_nbr,'"+crg+"',''), ''),15, ' ') as returned_group_id_301_c1,\
  free_goods_ind as free_goods,\
  lpad(nvl(cast(ltrim(regexp_replace(relocate_fm_str_nbr,'"+crg+"','')) as string), ''), 7, '0') as wag_original_store_id_7,\
  lpad(nvl(cast(ltrim(regexp_replace(str_nbr,'"+crg+"','')) as string), ''),7,'0') as wag_current_store_id_7\
  from Finalview \
    left join zipSuppressView on Finalview.pat_state_cd_pit = zipSuppressView.STATE_CD \
    and substring((case when pat_zip_cd_5_pit is null then rpad(nvl(ltrim(regexp_replace((pat_zip_cd_5_pit),'"+crg+"','')), ''),5, ' ') \
      else rpad(nvl(ltrim(regexp_replace(decryptFpeUDF(1,pat_zip_cd_5_pit),'"+crg+"','')), ''),5, ' ') \
    end), 1, 3) = zipSuppressView.zip_3 ")
  
  df=TransformDF.select((concat_ws("","wag_original_store_id","wag_current_store_id","prescription_ref_no_402d2_test","wag_rx_fill_nbr","date_authorized","time_authorized","service_date_401d1","wag_fill_sold_tm","de_identified_patient_code","wag_fill_stat_cd","fill_num_403d3","quantity_dispensed_442e7","days_supply_405d5","prescription_type_60149","partial_fill_cd","wag_fill_vrfy_dt","wag_fill_vrfy_tm","wag_fill_data_review_dt","wag_fill_data_review_tm","wag_filling_dt","wag_filling_tm","wag_fill_del_dt","wag_fill_del_tm","plan_id_524fo","wag_cob_ind","trans_response_status_112an","wag_fill_adjud_dt","wag_fill_adjud_tm","cardholder_id_302c2","submit_group_id","ingredient_cost_submit_409d9","dispensing_fee_submitted_412dc","bin_num_101a1","reject_cd_1_511fb","reject_cd_2_511fb","reject_cd_3_511fb","reject_count_510fa","date_prescript_written_414de","num_refills_auth_415df","orig_presc_quantity_446eb","compound_cd_406d6","daw_prod_selection_cd_408d8","processor_ctrl_num_104a4","fill_sold_dlrs_ind","prescription_origin_code","diagnosis_code","diagnosis_code_qualifier","patient_pay_amount","plan_short_name","processor_name","carrier_id","medicare_part_d_flag","medical_benefit_claim_indicator","ingredient_cost_paid","dispensing_fee_paid","basis_of_ingredient_cost_submit","basis_of_ingredient_cost_reimbursed","ims_coordination_of_benefits_counter","total_transaction_price_collected","amount_of_copay_coinsurance","ims_payment_type","ims_claim_indicator","supplier_plan_code","returned_network_reimbursement_id_545_2f","patient_rel_to_ins_306_c6","first_name_encrypted","last_name_encrypted","brth_dt","patient_gender_305c5","patient_street_address","patient_state_province_324_co","patient_zip_325_cp","wag_e_id","patient_zip","patient_date_of_birth","prescriber_dea","prescriber_id_qualifer","prescriber_last_name_427_dr","prescriber_npi","prescriber_first_name_364_2j","prescriber_middle_initial","prescriber_city_366_2m","prescriber_state_367_2n","prescriber_zip5","product_service_id_407_d7","product_service_id_qual_436_e1","unit_of_measure_600_28","dispensed_drug_name","service_provider_id_201_b1","wag_mail_retail_ind","pharmacy_zip_code","store_npi_number","benefit_qualifier_1_394_mv","benefit_qualifier_2_394_mv","benefit_qualifier_3_394_mv","benefit_qualifier_4_394_mv","usual_and_customary_charge","returned_group_id_301_c1","free_goods","wag_original_store_id_7","wag_current_store_id_7")).alias("fixedlength"))
  
  return df

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

spark.sql(f"use catalog dlx_{environment}")

path=spark.sql("select extract_landing_path from engineering_internal.dlx_iqvia_metadata where extract_name='IQVIA_SYMPHONY_master_transactions' and  extract_landing_flag='Y' and extract_prcsd_flag='N' and transformation_complition_flag='N' and deid_complition_flag='N' and transformed_file_deletion_flag='N' and master_straight_record_count>0")
pl=path.select(col("extract_landing_path").cast("string")).collect()
PathsList=[row["extract_landing_path"] for row in pl]

logger = getlogger('IQVIA Transformation')

for i in PathsList:
  vep=f"{strgPthNotation}Encrypt/"+(i.split("Encrypt/")[1])
  filename=((vep.split("Encrypt/")[1]).split(".dat")[0]).replace('_SYMPHONY','')  
  iqviaDf = spark.read.option("header",True).option('sep','\u0001').csv(vep)
  TotalRec=iqviaDf.count()

  drugList=f"abfss://rtl-iqvia-lookup@dlx{environment}semprtoutsa01.dfs.core.windows.net/in/rtl_drug_exclusion_list.csv"
  logger.info(f"Read Drug Exclusion List {drugList}")
  drugDf= spark.read.option("header",True).csv(drugList)  
  dl=drugDf.select(col("prod_name").cast("string")).collect()
  drugList=[row["prod_name"] for row in dl]  

  filterDF=iqviaDf.filter(~(col("prod_name_cur").rlike("|".join(drugList))) | (col("prod_name_cur").isNull()) | (col("prod_name_cur")==""))
  
  drugExRecCount=(iqviaDf.select("prod_name_cur").filter(expr("OR".join([f" prod_name_cur LIKE '%{word}%'" for word in drugList])))).count()		  
  logger.info(f"Total Records count is : {TotalRec}. Drug exclusion list record count: {drugExRecCount}")

  logger.info("updating the meta data table for drug exclusion records count")
  run_query_with_retry(spark.sql(f"update engineering_internal.dlx_iqvia_metadata set drugExclusion_record_count='{drugExRecCount}',last_update_dttm=current_timestamp() where extract_name='IQVIA_SYMPHONY_master_transactions' and  extract_landing_flag='Y' and extract_prcsd_flag='N' and transformation_complition_flag='N' and deid_complition_flag='N' and transformed_file_deletion_flag='N' and extract_landing_path='{i}'"))
  logger.info("updated meta data table sucessfully.")
    
  #invoking transformation function
  logger.info("Start Transformation.")
  df= Transformation(filterDF)
  df=df.withColumn("encode",encoding_udf("fixedlength"))

  #Filter the layout corrupt & valid records
  validLayoutDF=df.filter(col("encode")==708)
  layoutcorruptDF=df.filter((col("encode")>708)|(col("encode")<708))
  drpEncodeCuratedDF=validLayoutDF.drop("encode")

  #Counts of the total records, layout corrupt & valid records
  TotalRecordsCount=df.count()
  layoutrejectcount= layoutcorruptDF.count()
  validRecordsCount=validLayoutDF.count()

  # Generating Fixed length file as per check. If we have only valid layout records then write valid else both.
  logger.info(f"Total Records : {TotalRecordsCount}.Valid layout Reocrds : {validRecordsCount} , layout Corrupted records : {layoutrejectcount}")
  BadRecPer=(layoutrejectcount*100)/TotalRecordsCount
  if(BadRecPer>RejectThreshold):
    logger.info(f"Percentage of layout corrupt records is {BadRecPer} high. So rejecting the extract file {i}")
    dbutils.notebook.exit(f"Stop the process. Percentage of layout corrupt records is {BadRecPer} high")
  else:
    if(BadRecPer==0):					
      VaildlayoutRecPath=f"{strgPthNotation}Transformation/Transform_{filename}/"      
      VaildlayoutRecPathdat=f"abfss://iqvia-phi@dlx{environment}semprtoutsa01.dfs.core.windows.net/Transformation/Transform_{filename}.dat"
      logger.info(f"Percentage of layout crroupt records is {BadRecPer}. So writting valid records to {VaildlayoutRecPathdat}")
      drpEncodeCuratedDF.coalesce(1).write.mode("overwrite").format("text").option("header","True").save(VaildlayoutRecPath)          
      removePartFiles(VaildlayoutRecPath,".dat")
      logger.info(f"Valid layout records file generated sucessfully")

      logger.info(f"Updating Meta Data Table")   
      run_query_with_retry(spark.sql(f"update engineering_internal.dlx_iqvia_metadata set transformation_complition_flag='Y' ,transformed_file_landing_path='{VaildlayoutRecPathdat}' ,transformation_complition_dttm= current_timestamp(),last_update_dttm=current_timestamp() where extract_name='IQVIA_SYMPHONY_master_transactions' and  extract_landing_flag='Y' and extract_prcsd_flag='N' and transformation_complition_flag='N' and deid_complition_flag='N' and transformed_file_deletion_flag='N' and extract_landing_path='{i}'"))
    else:
      VaildlayoutRecPath=f"{strgPthNotation}Transformation/Transform_{filename}/"      
      VaildlayoutRecPathdat=f"abfss://iqvia-phi@dlx{environment}semprtoutsa01.dfs.core.windows.net/Transformation/Transform_{filename}.dat"
      logger.info(f"Percentage of bad records is {BadRecPer} low. So writting valid records to{VaildlayoutRecPathdat}")
      drpEncodeCuratedDF.coalesce(1).write.mode("overwrite").format("text").option("header","True").save(VaildlayoutRecPath)          
      removePartFiles(VaildlayoutRecPath,".dat")
      logger.info(f"Valid layout records file generated sucessfully")
      
      layoutRejPath=f"{strgPthNotation}Rejects/ims_layout_rejects_{filename}"      
      layoutRejPathADLS=f"abfss://iqvia-phi@dlx{environment}semprtoutsa01.dfs.core.windows.net/Rejects/ims_layout_rejects_{filename}"
      logger.info(f"Start writting {BadRecPer}% layout reject records to {layoutRejPathADLS}.dat")
      layoutcorruptDF.coalesce(1).write.mode("overwrite").format("csv").option("header","True").save(layoutRejPath)
      removePartFiles(layoutRejPath,".dat")
      logger.info(f"bad layout records file generated sucessfully")

      logger.info(f"Updating Meta Data Table")
      run_query_with_retry(spark.sql(f"update engineering_internal.dlx_iqvia_metadata set layout_reject_record_count ='{layoutrejectcount}',transformation_complition_flag='Y' ,transformed_file_landing_path='{VaildlayoutRecPathdat}' ,transformation_complition_dttm= current_timestamp(),last_update_dttm=current_timestamp() where extract_name='IQVIA_SYMPHONY_master_transactions' and  extract_landing_flag='Y' and extract_prcsd_flag='N' and transformation_complition_flag='N' and deid_complition_flag='N' and transformed_file_deletion_flag='N' and extract_landing_path='{i}'"))
