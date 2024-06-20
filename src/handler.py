from pandas import DataFrame 
from pandas import concat as pconcat
from json import loads as jloads
from json import dumps as jdumps
from pickle import loads as ploads
from sqlalchemy import create_engine, types
from boto3 import client, resource
from time import time
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

lambda_client = client('lambda')
s3_client = resource('s3')

ex_table_list = []

def bld_cnxn(creds):
    req_fields = ['redshift_username',
                  'redshift_password',
                  'redshift_host',
                  'redshift_port',
                  'redshift_database']
    for xx in req_fields:
        if xx not in creds:
            raise Exception('Missing required field: '+xx)
    rs_un = creds['redshift_username']
    rs_pw = creds['redshift_password']
    rs_host = creds['redshift_host']
    rs_port = creds['redshift_port']
    rs_db = creds['redshift_database']
    engine = create_engine('postgresql+psycopg2://'+rs_un+":"+rs_pw+"@"+rs_host+
                          ":"+rs_port+"/"+rs_db,encoding='latin-1')\
                          .connect().execution_options(autocommit=True)
    return engine

def parent(event, context):
    logs = {'fx':'parent_lambda'}
    records = event['Records']
    for record in records:
        if 'df' in locals():
            df = pconcat([df,DataFrame([jloads(record['body'])])])
        else:
            df = DataFrame([jloads(record['body'])])
    tables = list(set(df['table']))
    logs['tables'] = tables 
    logs['no_tables'] = len(tables)
    print(f'Number of tables in batch: {len(tables)}')
    err_list = []
    for table in tables:
        if table not in ex_table_list:
            print('{Testing Table:'+str(table)+'}')
            temp = df[df['table']==table]['data']
            tss = df[df['table']==table]['timestamp']
            ops = df[df['table']==table]['operation']
            temp = str(temp.to_json(orient='records'))
            tss = str(tss.to_json(orient='records'))
            ops = str(ops.to_json(orient='records'))
            temp = DataFrame(jloads(temp))
            temp['timestamp'] = DataFrame(jloads(tss))
            temp['operation'] = DataFrame(jloads(ops))
            inputParams = {
                "table":table,
                "db_env":'production',
                "data":temp.to_json(orient='records')
            }
#            inputParams = {
#                "table":table,
#                "db_env":'production',
#                "data":temp.to_json(orient='records'),
#                "timestamps":tss.to_json(orient='records'),
#                "operations":ops.to_json(orient='records')
#                }
            try:
                response = lambda_client.invoke(
                    FunctionName = "rb-mb-data-sqs-feed-master-prod-rb_mb_child_fx",
                    InvocationType = "Event",
                    Payload = jdumps(inputParams)
                )
                print('{Invoked Table:'+str(table)+'}')
                print('{Invoked Function:'+str(response)+'}')
            except Exception as err:
                err_list += [table]
                object = s3_client.Object('reservebar-data-engineering',
                    f'lambda_errors/{table}_{str(time()).replace(".","")}.txt')
                object.put(Body=str(jdumps(inputParams)).encode())
#                print('{Payload:'+jdumps(inputParams)+'}')
                print('{Table Failure:'+str(table)+'}')
    logs['error_tables'] = err_list 
    logs['no_errors'] = len(err_list)
    logger.info(logs)

def child(event, context):
    logs = {'fx':'child_lambda'}
    data = DataFrame(jloads(event['data']))
#    data['timestamp'] = DataFrame(jloads(event['timestamps']))
#    data['operation'] = DataFrame(jloads(event['operations']))
    data = data.rename(columns={'percent':'t_percent'})
    table = event['table']
    logs['table'] = table
    if table == 'variants':
        data = data.astype({'sku':str})
    elif table == 'product_grouping_search_data':
        data = data.drop(columns=['orderer_ids','orderer_ids_60day'])
    db_env = event['db_env']
    s3 = client('s3')
    response = s3.get_object(Bucket='reservebar-data-engineering',Key='config/lambda_creds.pkl')
    creds = ploads(response['Body'].read())
    creds['redshift_username'] = db_env+'_'+table
    engine = bld_cnxn(creds)
    try:
        tbl_nm = f's3://reservebar-data-engineering/temp_tables/{table}/{str(time()).replace(".","")}.json'
#        data.to_csv(tbl_nm,index=False,header=False,sep="|")
#        data.to_parquet(tbl_nm,index=False,compression='uncompressed',has_nulls=True)
        data = data.to_json(orient='records')
        s3.put_object(Body=data[1:-1].replace('},{','}{').encode(),
            Bucket='reservebar-data-engineering',
            Key=tbl_nm.replace('s3://reservebar-data-engineering/',''))
#        cols = ''
#        for col in data.columns:
#            if str(col) != 'default':
#                cols += (str(col) + ',')
#            else:
#                cols += ('"' + str(col) + '",')
        sql = f"""copy mb_{db_env}.temp_{table}
        from '{tbl_nm}'
        iam_role 'arn:aws:iam::746872950938:role/S3Redshiftaccess'
        emptyasnull
        json 'auto';
        """
#        sql = f"""copy mb_{db_env}.temp_{table}
#        from '{tbl_nm}'
#        iam_role 'arn:aws:iam::746872950938:role/S3Redshiftaccess'
#        format as parquet fillrecord;
#        """
#        sql = f"""copy mb_{db_env}.temp_{table} --({cols[:-1]})
#from '{tbl_nm}'
#iam_role 'arn:aws:iam::746872950938:role/S3Redshiftaccess'
##region 'us-west-2'
#csv delimiter '|'
#fillrecord emptyasnull;"""
#        connection = engine.connect()
#        connection.execute(sql)
#        connection.commit()
#        connection.close()
        engine.execute(sql)
        engine.close()
        s3_client.Object('reservebar-data-engineering',
            tbl_nm.replace('s3://reservebar-data-engineering/',''))\
                .delete()
        logs['success'] = 'True'
        logs['error'] = ''
        logger.info(logs)
    except Exception as err:
        logs['success'] = 'False'
        logs['error'] = str(err)[:250]
        logger.info(logs)
        engine.close()
        print(f'{table} ERROR!')
#        print(f'Columns: {data.columns}')
        print(str(err)[:250])   