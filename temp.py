# Databricks notebook source
####################################################################################################
#
#  機能概要 ：　(DAM_SBR_MEDALLIA_CHECK_RESULT)
#
#  ノートブック名 ： DAM_SBR_MEDALLIA_CHECK_RESULT_NB002
#  引数 ：
#
#  作成日、作成者:
#       2024-03-26,NSSOL 呉　思ドウ :新規作成
#
#  最終更新日、更新者:
#       YYYY-MM-DD,社名 名前 :修正理由
#
####################################################################################################

# COMMAND ----------

#####################################################
## Import python library
#####################################################
from sb.common import Env, MgtDate, CommonProcessing, Logger
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta
import datetime as dt
# 出力対象テーブル
table_name = "DAM_SBR_MEDALLIA_CHECK_RESULT"
#リネージュAPIキー
x_api_key = dbutils.secrets.get(scope=Env.get_env("DL_SECRET_SCOPE_NAME"), key=Env.get_env("DL_SECRET_LINEAGE_X_API_KEY"))
# 環境変数関連クラスのインスタンス生成
env_databricks_properties = Env.DataBricksProperties(env_path=Env.get_env("DL_DATA_PATH_ENV"), adls_output_ctl_flg=Env.get_env("DL_ADLS_OUTPUT_CTL_FLG"))
env = Env(env_databricks_properties)
# ジョブ日付インスタンス生成
mgt_date = MgtDate(x_api_key)
# 共通加工クラスのインスタンス作成
cmn_proc = CommonProcessing(CommonProcessing.CommonProcessingProperties(table_name, 
                                                                        Env.get_env("DL_SCHEMA"), 
                                                                        Env.get_env("DL_DATA_PATH"), 
                                                                        Env.get_env("DL_DATA_PATH_DWH_LOAD"),
                                                                        dbutils.secrets.get(scope=Env.get_env("DL_SECRET_SCOPE_NAME"), key="sbkkdssha256prefix2"), 
                                                                        dbutils.secrets.get(scope=Env.get_env("DL_SECRET_SCOPE_NAME"), key="sbkkdssha256suffix2"), 
                                                                       x_api_key))
# ロガーのインスタンス生成
logger_properties = Logger.LoggerProperties(logger_name="DAM_SBR_MEDALLIA_CHECK_RESULT_NB002", instrumentation_key=Env.get_env("DL_LOG_INSTRUMENTATION_KEY"))
logger = Logger(logger_properties)

# COMMAND ----------

####################################################################################################
## Initialize                                                                                     ##
####################################################################################################
def init():
  env.load_env("DAM_SBR_MEDALLIA_CHECK_RESULT")

# COMMAND ----------

####################################################################################################
## Load table data into DataFrame DAM_SBR_MEDALLIA_USER_ALL_DAILY_REG                             ##
####################################################################################################
def load_dam_sbr_medallia_user_all_daily_reg():
  dam_sbr_medallia_user_all_daily_reg_df = spark.sql("SELECT * FROM ${env:DL_SCHEMA}.DAM_SBR_MEDALLIA_USER_ALL_DAILY_REG")
  dam_sbr_medallia_user_all_daily_reg_df.createOrReplaceTempView("DAM_SBR_MEDALLIA_USER_ALL_DAILY_REG")
  return dam_sbr_medallia_user_all_daily_reg_df

# COMMAND ----------

####################################################################################################
## チェック処理                                                                                    ##
####################################################################################################
import datetime as dt
from pyspark.sql import DataFrame

def check_record_num(table, key):
    now = dt.date.today()

    opt = spark.sql("select cast(VAL002 as int) from datalake.mst_sbr_shp_medallia where KEY001='MEDALLIA' and KEY002='MEDALLIA_COUNT_CHECK_THRESHOLD' and KEY003='USER_1'").collect()[0][0]
        
    # 最新バージョンを取得
    version = spark.sql("SELECT max(version) FROM (DESCRIBE HISTORY datalake.{0})".format(table)).collect()[0][0]

    df = spark.sql("SELECT {1} FROM datalake.{0}".format(table, "*"))
    df_1 = spark.sql("SELECT {1} FROM datalake.{0}@v{2}".format(table, "*", version - 1))
    
    # y_cnt = df_1.count()
    # t_cnt = df.count()
    # 検証環境で上記のコードに切り替え
    y_cnt = 1
    t_cnt = 2
    
    # (昨日の件数 / 今日の件数) ≦ マスタの閾値 であればチェックOK（exit 0）、チェックNGなら（exit 1）を返却する
    if (abs(y_cnt - t_cnt) / y_cnt > (opt / 100)):
      # Compare the count with the previous version
      THRESHOLD = 0
      ratio = y_cnt / t_cnt
      if ratio > THRESHOLD:
        calc = 'abs(ratio*100)'
        check_contents = '件数'
        load_dam_sbr_medallia_check_result(calc, check_contents)

      # Get the count of changes only
      val = df.exceptAll(df_1)
      changes = df_1.join(val, df_1[key] == df[key], 'inner')
      if changes.count() > THRESHOLD:
        calc = 'abs(changes.count())'
        check_contents = '更新件数'
        load_dam_sbr_medallia_check_result(calc, check_contents)
    
table = "DAM_SBR_MEDALLIA_USER_ALL_DAILY_REG"
key = "USER_ID"
check_record_num(table, key)

# COMMAND ----------

####################################################################################################
## Load table data into DataFrame DAM_SBR_MEDALLIA_CHECK_RESULT                          ##
####################################################################################################
def load_dam_sbr_medallia_check_result(calc,check_contents):
  dam_sbr_medallia_check_result_df = spark.sql("""
  select
    DATE_FORMAT(UDF_CURRENT_TIMESTAMP(),'yyyyMMddHHmmss') as CHECK_DATE
    'DAM_SBR_MEDALLIA_USER_ALL_DAILY_REG' as CHECK_TARGET
    ,{check_contents} as CHECK_CONTENTS
    ,concat({calc},'%') as CALC
    ,'NG' as CHECK_RESULT  
  """.format(calc=calc,check_contents=check_contents))
  table_name = "DAM_SBR_MEDALLIA_CHECK_RESULT"
  dam_sbr_medallia_check_result_df.createOrReplaceTempView(table_name)
  if env.get_adls_output_ctl_flg(table_name) == "1":
    dam_sbr_medallia_check_result_df.insert_overwrite(dam_sbr_medallia_check_result_df, table_name)
  return dam_sbr_medallia_check_result_df

#load_dam_sbr_nps_mail_list_for_medallia().show()

# COMMAND ----------

try:
  #==========#
  # 初期処理
  #==========#
  init()

  #========================#
  # 入力テーブルロード処理
  #========================#
  dam_sbr_medallia_user_all_daily_reg_df = load_dam_sbr_medallia_user_all_daily_reg()

  #============#
  # マート処理
  #============#
  dam_sbr_medallia_check_error_result_df  = load_dam_sbr_medallia_check_error_result(cale,check_contents)
  
  #==============#
  # テーブル出力
  #==============#
  cmn_proc.insert_overwrite_wrap(dam_sbr_medallia_check_error_result_df)
  
except Exception as e:
  logger.error(e)
  cmn_proc.err_proc(e)
  raise e
