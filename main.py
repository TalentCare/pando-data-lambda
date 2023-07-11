import pandas as pd
import boto3
import os
import io
from openpyxl import load_workbook
import pandas_redshift as pr
from dotenv import load_dotenv
import redshift_connector


def lambda_handler(event, context):
    load_dotenv()

    # Retrieve all environment variables
    db_name = os.getenv("DBNAME")
    host = os.getenv("HOST")
    port = 5439
    user = os.getenv("USER")
    password = os.getenv("PASSWORD")
    aws_access_key = os.getenv("ACCESS_KEY_ID")
    aws_secret_key = os.getenv("SECRET_ACCESS_KEY")
    bucket = os.getenv("BUCKET")
    session_bucket = os.getenv("SESSION_BUCKET")

    # Connect to Redshift
    conn = redshift_connector.connect(
        host=host,
        database=db_name,
        port=port,
        user=user,
        password=password
    )

    cursor = conn.cursor()

    # Start an S3 session and retrieve the most recent file in the bucket
    session = boto3.Session()
    s3_session = session.resource('s3')
    my_bucket = s3_session.Bucket('pando-zapier-results')

    latest_file = None
    latest_timestamp = None

    for my_bucket_object in my_bucket.objects.all():
        if latest_timestamp is None or my_bucket_object.last_modified > latest_timestamp:
            latest_file = my_bucket_object.key
            latest_timestamp = my_bucket_object.last_modified

    if latest_file is not None:
        obj = s3_session.Object('pando-zapier-results', latest_file)
        response = obj.get()
        obj = response['Body'].read()

    # Process the file from xlsx to dataframe
    data = io.BytesIO(obj)
    response_wb = load_workbook(data)
    response_ws = response_wb.active
    data_df = pd.DataFrame(response_ws.values)
    data_df.drop([0,1], axis=0, inplace=True)
    data_df.reset_index(drop=True, inplace=True)
    data_df.columns = data_df.iloc[0]
    data_df.drop([0], axis=0, inplace=True)
    data_df[['job_guid', 'campaign_id']] = data_df['Req Id'].str.split('_', expand=True)
    data_df.drop(['Req Id'], axis=1, inplace=True)
    data_df.dropna(how='all', inplace=True)

    data_df1 = data_df.iloc[:,[20,0,1,19,3,4,5,6,7,8,9,10,11,12,16,13,14,15,18,17]].copy()
    data_df1.rename(columns={
    'Job Title':'job_title',
    'Hiring Company':'hiring_company',
    'Zip Code':'zip_code',
    'Campaign Start Date':'campaign_start_date',
    'Campaign End Date':'campaign_end_date',
    'End of Budget Date':'end_of_budget_date',
    'Deactivation Date':'deactivation_date',
    'Posting Status':'posting_status',
    'Campaign Remaining Days':'campaign_remaining_days',
    'Applicant Rate':'applicant_rate'
    }, inplace=True)
    data_df['collection_date'] = pd.Timestamp.today()

    # Copy the cleaned dataframe to a redshift table
    pr.connect_to_s3(bucket=bucket, aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key)
    pr.connect_to_redshift(dbname=db_name, host=host, port=port, user=user, password=password)
    pr.pandas_to_redshift(data_frame=data_df, redshift_table_name='public.pandologic')

    # Insert data into historic_pando_results table
    cursor.execute("INSERT INTO historic_pando_results SELECT * FROM public.pandologic")

    conn.commit()
    conn.close()

    return {
        'statusCode': 200,
        'body': 'Data cleaned and uploaded successfully!'
    }
