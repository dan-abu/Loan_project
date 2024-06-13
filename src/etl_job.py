"""ETL script based on new file landing in lending-project/landing/"""
import boto3
import pandas as pd
import datetime as dt
import sys
import os

def get_file_suffix(file_path):
    """Returns file root and suffix as a tuple"""
    root, suffix = os.path.splitext(file_path)
    return (root, suffix)
    
def get_rel_filepath(s3_key):
    """Returns relative filepath and extention as tuple"""
    head, tail = os.path.split(s3_key)
    return tail

def list_s3_objects(bucket, prefix):
    """Lists S3 objects in landing bucket."""
    print('Finding S3 objects in landing bucket...')
    try:
        response = s3.list_objects(Bucket=bucket, Prefix=prefix)
    except Exception as e:
        print('Unable to find S3 objects in landing bucket.')
        print(e)
        raise e
    else:
        objects_list = []
        for obj in response['Contents']:
            objects_list.append(obj['Key'])
        print(f'Found S3 objects: {objects_list}.')
    return objects_list

def get_s3_object(bucket, key):
    """Gets S3 object"""
    print('Glue job has started to load data.')
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        data = pd.read_csv(response['Body'], thousands=',')
        print('Successfully loaded data.')
    except Exception as e:
        print('Unable to load file.')
        print(e)
        raise e
    else:
        print('Successfully loaded data.')
    return data

def csv_checker(bucket, prob_key, object, src_key):
    """Checks file is CSV. If not, moves it to the review folder and deletes at source."""
    print('Checking file is suitable for transformation.')
    if get_file_suffix(object)[1] == '.csv':
        print('File is suitable for transformation.')
        return
    else:
        s3.copy_object(Bucket=bucket, Key=prob_key, CopySource={Bucket:bucket, Key:object})
        s3.delete_object(Bucket=bucket, Key=object)
        print('File was not suitable for transformation. File has been moved to the review folder.\nGlue job completed.')
        sys.exit()

def check_metadata(data):
    """Prints metadata about the file and initial analysis of the data"""
    print('Completing initial metadata and data analysis...')
    print(f'This is the data\'s shape: {data.shape}.')
    print(f'These are the data\'s columns:\n{data.columns.values}.')
    print(f'These are the data\'s data types:\n{data.dtypes}.')
    print(f'There are {data["employment_duration"].eq(0).sum()} rows with 0 as a value for the employment_duration column as a value.')
    print(f'There are {data["loan_amnt"].eq(0).sum()} rows with 0 as a value for the loan_amnt column as a value.')
    print(f'There are {data["loan_int_rate"].eq(0).sum()} rows with 0 as a value for the loan_int_rate column as a value.')
    print(f'There are {data["historical_default"].eq(0).sum()} rows with 0 as a value for the historical_default column as a value.')
    print(f'This is the breakdown of null values in the data:\n{data.isna().sum()}')
    print('Completed initial metadata and data analysis.')
    return

def clean_data(data):
    """Removes null values and changes data types and columns where appropriate. Prints statements confirming changes."""
    print("""Started transforming data...""")
    data.dropna(subset=["customer_id", "employment_duration", "loan_amnt", "historical_default", "Current_loan_status"], inplace=True)
    data.fillna({'loan_int_rate': 0}, inplace=True)
    data.rename(columns={data.columns[-6]: 'loan_amnt(£)'}, inplace=True)
    data['loan_amnt(£)'] = data['loan_amnt(£)'].str.replace('£', '').str.replace(',', '').astype(float)
    print(f'There are {data["customer_id"].isna().sum()} rows with null as a value for the customer_id column as a value.')
    print(f'There are {data["employment_duration"].isna().sum()} rows with null as a value for the employment_duration column as a value.')
    print(f'There are {data["loan_amnt(£)"].isna().sum()} rows with null as a value for the loan_amnt column as a value.')
    print(f'There are {data["historical_default"].isna().sum()} rows with null as a value for the historical_default column as a value.')
    print(f'There are {data["loan_int_rate"].isna().sum()} rows with null as a value for the loan_int_rate column as a value.')
    print(f'There are {data["Current_loan_status"].isna().sum()} rows with null as a value for the Current_loan_status column as a value.')
    print(f'What was column "loan_amnt" is now column "loan_amnt(£)" and its data type has changed to: {data.dtypes["loan_amnt(£)"]}.')
    print("Completed transforming data.")
    return

def put_s3_object(data, bucket, og_key):
    """Write data to desired S3 bucket as a CSV"""
    key = get_rel_filepath(og_key)
    print(key[1])
    dest_addr = bucket + '/' + 'cleansed/' + key[1]
    print(f'Attempting to write data to S3 desination {dest_addr}.')
    execution_time = dt.datetime.now().strftime("%Y%m%d_%H:%M")
    key = get_file_suffix(key)
    key = key[0] + execution_time
    try:
        data.to_csv(f's3://lending-project/cleansed/{key}.csv', index=False)
        s3.delete_object(Bucket=bucket, Key=og_key)
    except Exception as e:
        print(f'Unable to write file to S3 bucket {bucket}.')
        print(e)
        raise e
    else:
        print('Successfully uploaded file.')
    return

if __name__ == '__main__':
    print('Loading glue job...')
    s3 = boto3.client('s3')
    bucket = 'lending-project'
    src_key = 'landing/'
    prob_key = 'review/'
    data_object = list_s3_objects(bucket=bucket, prefix=src_key)[1]
    csv_checker(bucket=bucket, prob_key=prob_key, object=data_object, src_key=src_key)
    etl_data = get_s3_object(bucket=bucket, key=data_object)
    check_metadata(etl_data)
    clean_data(etl_data)
    put_s3_object(data=etl_data, bucket=bucket, og_key=data_object)
    print('Glue job complete.')