// Databricks notebook source
//A big df with 3 tables for the first spring and calendar dim from DW 

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import sqlContext.implicits._ 
import org.apache.spark.sql.functions.unix_timestamp

// COMMAND ----------

display(dbutils.fs.ls("/mnt/ifrs9"))

// COMMAND ----------

val S_CIF_CIFWASMR_H = sqlContext.read.format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .option("delimiter", "|")
  .load("dbfs:/mnt/ifrs9/20170519_S_CIF_CIFWASMR_H.dat")

val renamed_S_CIF_CIFWASMR_H = S_CIF_CIFWASMR_H.selectExpr("DATE_KEY as DATE_KEY_S_CIF_CIFWASMR_H", "QAXBRNUM_QA00","QAPCUSNM_QA00","QAXACSUF_QA00_1") 

val S_CIF_CIFWAS_MR_A0 = sqlContext.read.format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .option("delimiter", "|")
  .load("dbfs:/mnt/ifrs9/20170519_S_CIF_CIFWAS_MR_A0.dat")

val renamed_S_CIF_CIFWAS_MR_A0 = S_CIF_CIFWAS_MR_A0.selectExpr ("DATE_KEY as DATE_KEY_S_CIF_CIFWAS_MR_A0", "QAXBRNUM_QAA0","QAPCUSNM_QAA0","QAXACSUF_QAA0_1","QAPCISA9","QAPMATDT","QAPHOLMC","QAPHOLMR","QAXOPBNK","QAPBALNC" )

val S_WASP_WORFFL = sqlContext.read.format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .option("delimiter", "|")
  .load("dbfs:/mnt/ifrs9/20170519_S_WASP_WORFFL.dat")

val renamed_S_WASP_WORFFL = S_WASP_WORFFL.selectExpr ("DATE_KEY as DATE_KEY_S_WASP_WORFFL","WORFFL_BANK","WORFFL_BRANCH","WORFFL_BASE","WORFFL_SUFFIX","WORFFL_DOMICILED_BRANCH","WORFFL_ACCT_TYPE","WORFFL_REDUCTION_FREQ_CODE","WORFFL_NO_OF_DISHNR_THIS_MTH","WORFFL_ACCT_TYPE_1","WORFFL_HO_LIMIT_AMT","WORFFL_ACCT_BAL","WORFFL_ARREARS_SINCE_DATE","WORFFL_CORPORATE_NUMBER","WORFFL_ARREARS_COUNT_12_MTHS")

val cifs = renamed_S_CIF_CIFWASMR_H.join(renamed_S_CIF_CIFWAS_MR_A0, renamed_S_CIF_CIFWASMR_H.col("QAPCUSNM_QA00") === renamed_S_CIF_CIFWAS_MR_A0.col("QAPCUSNM_QAA0") && renamed_S_CIF_CIFWAS_MR_A0.col("QAXBRNUM_QAA0") ===renamed_S_CIF_CIFWASMR_H.col("QAXBRNUM_QA00")&& renamed_S_CIF_CIFWAS_MR_A0.col("QAXACSUF_QAA0_1")===renamed_S_CIF_CIFWASMR_H.col("QAXACSUF_QA00_1") && renamed_S_CIF_CIFWAS_MR_A0.col("DATE_KEY_S_CIF_CIFWAS_MR_A0")===renamed_S_CIF_CIFWASMR_H.col("DATE_KEY_S_CIF_CIFWASMR_H") )

val cif_wasp =  cifs.join(renamed_S_WASP_WORFFL,renamed_S_WASP_WORFFL.col("WORFFL_BRANCH")===cifs.col("QAXBRNUM_QAA0") && renamed_S_WASP_WORFFL.col("WORFFL_BASE")===cifs.col("QAPCUSNM_QAA0") && renamed_S_WASP_WORFFL.col("DATE_KEY_S_WASP_WORFFL") === cifs.col("DATE_KEY_S_CIF_CIFWAS_MR_A0"))

val date_dim = sqlContext.read.format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .load("dbfs:/mnt/ifrs9/Calendar_Day_Ref.csv")
//
val date_dim_eom = date_dim.selectExpr("DATE_KEY as DATE_KEY_dim", "month_key")

val cif_wasp_date = cif_wasp.join(date_dim_eom,cif_wasp.col("DATE_KEY_S_WASP_WORFFL")=== date_dim_eom.col("DATE_KEY_dim"))

//this is a df with all the three staging tables and 2 cols from our DW calendar dim 
//val tabletwo = sqlContext.sql("SELECT * FROM cif_wasp_date_table")
//display(tabletwo.select("*"))

// COMMAND ----------

display(tablefive)

// COMMAND ----------

//
val date_dim = sqlContext.read.format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .load("dbfs:/mnt/ifrs9/Calendar_Day_Ref.csv")

   //tabletwo.select(tabletwo("month_key").cast("String"))

//var tablethree = tabletwo.withColumn("PERIOD_ID", concat(substring(col("month_key"),7,4),substring(col("month_key"),4,2),substring(col("month_key"),1,2))) 
var tablethree = cif_wasp_date.withColumn("PERIOD_ID", concat(substring(col("month_key"),7,4),substring(col("month_key"),4,2),substring(col("month_key"),1,2))) 
var tablefour = tablethree.withColumn("SOURCE_SYSTEM_CODE", lit("CIF")) 
var tablefive = tablefour.withColumn("NZ_SOURCE_SYSTEM_CODE", lit("NZCIF"))
tablefive.createOrReplaceTempView("tablesix")
var tableseven = sqlContext.sql("SELECT QAXBRNUM_QA00 QAXBRNUM_QA00_t7,QAPCUSNM_QA00 QAPCUSNM_QA00_t7,QAXACSUF_QA00_1 QAXACSUF_QA00_1_t7,WORFFL_HO_LIMIT_AMT as lmt,WORFFL_ACCT_BAL as bal, case when WORFFL_HO_LIMIT_AMT < 0 then ABS(WORFFL_HO_LIMIT_AMT)+WORFFL_ACCT_BAL when WORFFL_ACCT_BAL < 0 then WORFFL_HO_LIMIT_AMT+ABS(WORFFL_ACCT_BAL) * -1 when WORFFL_ACCT_BAL < WORFFL_HO_LIMIT_AMT then WORFFL_ACCT_BAL - WORFFL_HO_LIMIT_AMT else WORFFL_ACCT_BAL + WORFFL_HO_LIMIT_AMT end as intermediate_NZ_TOTAL_ARREARS_AMOUNT FROM tablesix")



var tableeight = tablefive.join(tableseven, tablefive.col("QAXBRNUM_QA00")===tableseven.col("QAXBRNUM_QA00_t7") && tableseven.col("QAPCUSNM_QA00_t7")===tablefive.col("QAPCUSNM_QA00") && tableseven.col("QAXACSUF_QA00_1_t7")===tablefive.col("QAXACSUF_QA00_1") )


var tablenine = tableeight.drop(tableeight.col("lmt"))
var tableten = tablenine.drop(tableeight.col("bal"))
var tableevelen = tableten.withColumn("COUNTRY_CODE", lit("NZ")) 

val tabletwelve = tableevelen.withColumn("ACCOUNT_SUB_TYPE", when($"WORFFL_ACCT_TYPE"==="CHC", "T").otherwise(null))
val tablethirdteen = tabletwelve.withColumn("NZ_TOTAL_ARREARS_AMOUNT", when( $"intermediate_NZ_TOTAL_ARREARS_AMOUNT"<0,$"intermediate_NZ_TOTAL_ARREARS_AMOUNT" ).when($"WORFFL_ARREARS_SINCE_DATE"=!=0,$"intermediate_NZ_TOTAL_ARREARS_AMOUNT").otherwise(null))

//display(tablethirdteen.select("*"))
val tablefourteen = tablethirdteen.withColumn("LIMIT_REDUCTION_FREQUENCY",when( $"WORFFL_REDUCTION_FREQ_CODE" === "FT","04" ).when($"WORFFL_REDUCTION_FREQ_CODE"=== "MO","01").when($"WORFFL_REDUCTION_FREQ_CODE"=== "QT","03").when($"WORFFL_REDUCTION_FREQ_CODE"=== "BM","02").when($"WORFFL_REDUCTION_FREQ_CODE"=== "BA","06").when($"WORFFL_REDUCTION_FREQ_CODE"=== "AN","12").otherwise(null))

val tablefiveteen = tablefourteen.withColumn("MONTH_KEY",concat(substring(col("month_key"),7,4),substring(col("month_key"),4,2)))
display(tablefiveteen)

// COMMAND ----------


val TargetDF14=tablefiveteen.withColumn("FACILITY_ID",concat(lit("CIF"),lpad(trim($"QAXBRNUM_QA00"),4,"0"),lpad(trim($"QAPCUSNM_QA00"),7,"0"),lpad(trim($"QAXACSUF_QA00_1"),2,"0"))) 
.withColumn("ACCOUNT_BASE_NUMBER",lpad(trim($"QAPCUSNM_QA00"),7,"0"))
.withColumn("ACCOUNT_BRANCH_CODE",lpad(trim($"QAXBRNUM_QA00"),4,"0"))
.withColumn("CUSTOMER_ID",$"QAPCISA9".as[Int])
.withColumn("MATURITY_DATE",$"QAPMATDT")
.withColumn("MONTH_NUMBER_OF_DISHONOURS",$"WORFFL_NO_OF_DISHNR_THIS_MTH")
.withColumn("NZ_FACILITY_LIMIT_AMOUNT",$"QAPHOLMC" * -1)
.withColumn("ACCOUNT_SUFFIX",lpad(trim($"QAXACSUF_QA00_1"),2,"0")) 
.withColumn("RESP_INTERNAL_BUSINESS_UNIT_ID",when(isnull($"WORFFL_CORPORATE_NUMBER") || $"WORFFL_CORPORATE_NUMBER" === 501100 || $"WORFFL_CORPORATE_NUMBER" === "001100","1").when(($"WORFFL_CORPORATE_NUMBER">=0) && ($"WORFFL_CORPORATE_NUMBER"< 100000),"2").when(($"WORFFL_CORPORATE_NUMBER">=590000) && ($"WORFFL_CORPORATE_NUMBER"< 593000),"9").when(($"WORFFL_CORPORATE_NUMBER">=800000) && ($"WORFFL_CORPORATE_NUMBER"< 900000),"8").when($"WORFFL_CORPORATE_NUMBER">=900000,"1").otherwise("5"))
.withColumn("NZ_LIMIT_REDUCTION_AMOUNT",$"QAPHOLMR")
.withColumn("NZ_SOURCE_PRODUCT_CODE",$"WORFFL_ACCT_TYPE")
.withColumn("OPENED_DATE",$"QAXOPBNK")
.withColumn("YEAR_NUMBER_OF_ARREARS",$"WORFFL_ARREARS_COUNT_12_MTHS")


// COMMAND ----------

display(TargetDF2)

// COMMAND ----------

// merging starts
val TargetDF1=cif_wasp.select("QAXBRNUM_QA00","QAPCUSNM_QA00","QAXACSUF_QA00_1","QAPHOLMR","WORFFL_ACCT_TYPE","WORFFL_ARREARS_COUNT_12_MTHS","QAPCISA9","QAPMATDT","WORFFL_NO_OF_DISHNR_THIS_MTH","QAPHOLMC","WORFFL_CORPORATE_NUMBER") //TAKING THE COLUMNS REQUIRED FOR TRANSFORMATIONS

val TargetDF2=tablefiveteen.withColumn("FACILITY_ID",concat(lit("CIF"),lpad(trim($"QAXBRNUM_QA00"),4,"0"),lpad(trim($"QAPCUSNM_QA00"),7,"0"),lpad(trim($"QAXACSUF_QA00_1"),2,"0"))) 
//display(TargetDF2)
val TargetDF3=TargetDF2.withColumn("ACCOUNT_BASE_NUMBER",lpad(trim($"QAPCUSNM_QA00"),7,"0"))
//display(TargetDF3)w
val TargetDF4=TargetDF3.withColumn("ACCOUNT_BRANCH_CODE",lpad(trim($"QAXBRNUM_QA00"),4,"0"))
//display(TargetDF4)

val TargetDF5=TargetDF4.withColumn("CUSTOMER_ID",$"QAPCISA9".as[Int])
display(TargetDF5)

val TargetDF6=TargetDF5.withColumn("MATURITY_DATE",$"QAPMATDT")
val TargetDF7=TargetDF6.withColumn("MONTH_NUMBER_OF_DISHONOURS",$"WORFFL_NO_OF_DISHNR_THIS_MTH")
val TargetDF8=TargetDF7.withColumn("NZ_FACILITY_LIMIT_AMOUNT",$"QAPHOLMC" * -1)
val TargetDF9=TargetDF8.withColumn("ACCOUNT_SUFFIX",lpad(trim($"QAXACSUF_QA00_1"),2,"0")) 
val TargetDF10=TargetDF9.withColumn("RESP_INTERNAL_BUSINESS_UNIT_ID",when(isnull($"WORFFL_CORPORATE_NUMBER") || $"WORFFL_CORPORATE_NUMBER" === 501100 || $"WORFFL_CORPORATE_NUMBER" === "001100","1").when(($"WORFFL_CORPORATE_NUMBER">=0) && ($"WORFFL_CORPORATE_NUMBER"< 100000),"2").when(($"WORFFL_CORPORATE_NUMBER">=590000) && ($"WORFFL_CORPORATE_NUMBER"< 593000),"9").when(($"WORFFL_CORPORATE_NUMBER">=800000) && ($"WORFFL_CORPORATE_NUMBER"< 900000),"8").when($"WORFFL_CORPORATE_NUMBER">=900000,"1").otherwise("5"))
val TargetDF11=TargetDF10.withColumn("NZ_LIMIT_REDUCTION_AMOUNT",$"QAPHOLMR")
val TargetDF12=TargetDF11.withColumn("NZ_SOURCE_PRODUCT_CODE",$"WORFFL_ACCT_TYPE")
val TargetDF13=TargetDF12.withColumn("OPENED_DATE",$"QAXOPBNK")
val TargetDF14=TargetDF13.withColumn("YEAR_NUMBER_OF_ARREARS",$"WORFFL_ARREARS_COUNT_12_MTHS")

// COMMAND ----------

TargetDF14.count() ///final result set with all columns
TargetDF14.printSchema()


// COMMAND ----------

val FinalTargetDF = TargetDF14.select("MONTH_KEY",
"PERIOD_ID",                     
"SOURCE_SYSTEM_CODE",            
"NZ_SOURCE_SYSTEM_CODE",         
"FACILITY_ID",                   
"ACCOUNT_BASE_NUMBER",
"ACCOUNT_BRANCH_CODE",           
"ACCOUNT_SUB_TYPE",              
"ACCOUNT_SUFFIX",                
"COUNTRY_CODE",                  
"CUSTOMER_ID",                   
"LIMIT_REDUCTION_FREQUENCY",     
"MATURITY_DATE",                 
"MONTH_NUMBER_OF_DISHONOURS",    
"NZ_FACILITY_LIMIT_AMOUNT",      
"NZ_LIMIT_REDUCTION_AMOUNT",     
"NZ_SOURCE_PRODUCT_CODE",        
"NZ_TOTAL_ARREARS_AMOUNT",       
"OPENED_DATE",                   
"RESP_INTERNAL_BUSINESS_UNIT_ID",
"YEAR_NUMBER_OF_ARREARS"           
)



// COMMAND ----------

display(FinalTargetDF)

// COMMAND ----------

FinalTargetDF.schema.fieldNames.size

// COMMAND ----------

FinalTargetDF.count()

// COMMAND ----------

// MAGIC %fs rm -r /mnt/ifrs9/DT_Facility.csv/

// COMMAND ----------

//need to find out the function to write the target file with timestamp. For the time being, added command to delete the file to recreate the file with latest dataset in below step. 
FinalTargetDF.coalesce(1).write.option("header", "true").csv("dbfs:/mnt/ifrs9/DT_Facility.csv")  

// COMMAND ----------

display(dbutils.fs.ls("dbfs:/mnt/ifrs9/DT_Facility.csv/"))

// COMMAND ----------

dbutils.fs.head("dbfs:/mnt/ifrs9/DT_Facility.csv/part-r-00000-9622727c-9161-4619-a2dc-8e0a3186f021.csv")

// COMMAND ----------

import java.text.SimpleDateFormat

val inputFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
val outputFormat = new SimpleDateFormat("MM/yyyy")

val date = "2015-01-31 12:34:00"

val formattedDate = outputFormat.format(inputFormat.parse(date))

println(formattedDate) // prints "31/01/2015 12:34"