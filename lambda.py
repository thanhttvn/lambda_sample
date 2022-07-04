import json
import boto3
import pandas as pd
import awswrangler as wr
import logging
import numpy as np
from sqlalchemy.engine import create_engine
import cx_Oracle

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# DIALECT = 'oracle'
# SQL_DRIVER = 'cx_oracle'
# USERNAME = 'thanhtt'  # enter your username
# PASSWORD = '123456'  # enter your password
# HOST = '13.215.174.61'  # enter the oracle db host url
# PORT = 1521  # enter the oracle port number
# SERVICE = 'DEVDB'  # enter the oracle db service name
# ENGINE_PATH_WIN_AUTH = DIALECT + '+' + SQL_DRIVER + '://' + USERNAME + ':' + PASSWORD + '@' + HOST + ':' + str(
#     PORT) + '/?service_name=' + SERVICE
# SCHEMA = "THANHTT"


def load_to_database(s3_path, cursor, conn):
    """
    read parquet file from s3 folder
    :param s3_path: s3 folder path
    :return:
    """

    # s3://sit-instant-lending-test/instant-lending-tables/RLOS_F_ACT_TRN_ACCOUNT/business_date=2022-03-29/RLOS_F_ACT_TRN_ACCOUNT_20220329_1.parquet
    dfs = wr.s3.read_parquet(path=s3_path, chunked=True, map_types=False)

    for idx, df in enumerate(dfs):
        logger.info("chunk %s" % (idx + 1))
        insert_to_tmp_tbl_stmt = """
        INSERT INTO THANHTT.RLOS_F_ACT_TRN_ACCOUNT
        (SZACCOUNTID,
        SZAPPLICATIONID,
        FAPPROVEDAMOUNT,
        FAPPLIEDAMOUNT,
        IAPPROVEDTENOR,
        IAPPLIEDTENOR,
        FPRODCTAPPLIEDAMT,
        FAPPROVEDINTERESTRATE,
        SZPRODUCTLEVEL2CODE,
        SZPRODUCTLEVEL3CODE,
        SZPRODUCTLEVEL4CODE,
        DTUPDATEDON,
        DTCREATEDON,
        BUSINESS_DATE)
        VALUES(:SZACCOUNTID,
        :SZAPPLICATIONID,
        :FAPPROVEDAMOUNT,
        :FAPPLIEDAMOUNT,
        :IAPPROVEDTENOR,
        :IAPPLIEDTENOR,
        :FPRODCTAPPLIEDAMT,
        :FAPPROVEDINTERESTRATE,
        :SZPRODUCTLEVEL2CODE,
        :SZPRODUCTLEVEL3CODE,
        :SZPRODUCTLEVEL4CODE,
        :DTUPDATEDON,
        :DTCREATEDON,
        :BUSINESS_DATE)
        """

        # logger.info(type(df))
        # logger.info(df.dtypes)
        # logger.info(df.head(2).values.tolist())

        df = df.where(pd.notnull(df), None)  # SUA CAI NAY cung ko dc.
        # for index, row in df.iterrows():
        #     logger.info("load: %s" % index)
        #     logger.info(row.values.tolist())
        #     logger.info(row.dtypes)
        #     cursor.executemany(insert_to_tmp_tbl_stmt, [row.values.tolist()])
        #     conn.commit()

        cursor.prepare(insert_to_tmp_tbl_stmt)
        cursor.executemany(None, df.values.tolist())
        conn.commit()
        logger.info("finished chunk %s" % (idx + 1))


def handler(event, context):
    logger.info("Start execute lambda handler")
    # s3_client = boto3.client('s3')
    # s3_clientobj = s3_client.get_object(Bucket='sit-instant-lending-test', Key='tp-data.txt')
    #
    # for line in s3_clientobj['Body'].iter_lines():
    #     object = json.loads(line)
    #     logger.info(f"Name: {object['name']['s']} ID: {object['id']['n']}")

    # engine = create_engine(ENGINE_PATH_WIN_AUTH)
    conn_str = 'thanhtt/123456@13.215.174.61:1521/DEVDB'
    conn = cx_Oracle.connect(conn_str)
    cursor = conn.cursor()
    # test_df = pd.read_sql_query('SELECT 1 FROM dual', engine)
    load_to_database(
        "s3://sit-instant-lending-test/instant-lending-tables/RLOS_F_ACT_TRN_ACCOUNT/business_date=2022-03-29",
        cursor, conn)

    logger.info("Lambda execution Completed...!")
