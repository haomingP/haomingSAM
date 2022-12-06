from __future__ import print_function
import boto3
import urllib.parse
import time, urllib
import pandas as pd
import awswrangler as wr
import datetime
import time
import os

print("Loading Function..")

# landing bucket and prefix
LANDING_BUCKET = os.environ.get('LANDING_BUCKET')
PREFIX = os.environ.get('PREFIX')
LAND_DIR = os.environ.get('LAND_DIR')
BACKUP_DIR = os.environ.get('BACKUP_DIR')

# S3 Object
s3 = boto3.client('s3')
s3_object = boto3.client('s3', region_name='us-west-2')
s3_resource = boto3.resource('s3')


def timestamp2string(timeStamp):
    """
    convert timestamp to string

    param ts: time stamp
    """
    try:
        d = datetime.datetime.fromtimestamp(timeStamp)
        str1 = d.strftime("%d/%m/%Y %H:%M:%S")
        return str1
    except Exception as e:
        print(e)
        return ''


# get the start and end time of this log file
def get_start_end_time(df):
    print(df.columns)

    start_time_str = timestamp2string(df.iloc[0, 1])
    print("start time is: ", start_time_str)
    start_time = start_time_str.split(' ')[1]
    start_hour = start_time.split(':')[0]
    start_date = (start_time_str.split(' ')[0]).split('/')[0]

    end_time_str = timestamp2string(df.iloc[-1, 1])
    print("end time is: ", end_time_str)
    end_time = end_time_str.split(' ')[1]
    end_hour = end_time.split(':')[0]
    # end_date = (end_time_str.split(' ')[0]).split('/')[0]

    file_str = 'canserver_'
    time_list = df['timestamp'].to_list()
    new_time_list = [int((timestamp2string(e).split(' ')[1]).split(':')[0]) +
                     (24 * (int((timestamp2string(e).split(' ')[0]).split('/')[0]) - int(start_date))) for e in
                     time_list]
    target_list = sorted(list(set(new_time_list)))
    print("Set of hours: ", target_list)
    s_e_list = []
    for t in target_list:
        s_e_list.append(helper_search_hour(new_time_list, t))

    filename_list = []
    for l in s_e_list:
        time_str = timestamp2string(df.iloc[l[0], 1])
        hour = int((time_str.split(' ')[1]).split(':')[0])
        date = (time_str.split(' ')[0]).split('/')[0]
        month = (time_str.split(' ')[0]).split('/')[1]
        year = (time_str.split(' ')[0]).split('/')[2]
        if hour >= 9:
            filename_list.append(file_str + year + '-' + month + '-' + date + '_' + str(hour + 1))
        else:
            filename_list.append(file_str + year + '-' + month + '-' + date + '_' + '0' + str(hour + 1))
    print(filename_list)

    return s_e_list, filename_list


# Helper binarysearch function
def helper_search_hour(nums, target):
    def search_index(nums, target, sign):
        left = 0
        right = len(nums)
        while left < right:
            mid = (left + right) // 2
            if nums[mid] > target or (sign and target == nums[mid]):
                right = mid
            else:
                left = mid + 1
        return left

    left_index = search_index(nums, target, True)

    if left_index == len(nums) or nums[left_index] != target:
        return [-1, -1]

    right_index = search_index(nums, target, False) - 1
    return [left_index, right_index]


def lambda_handler(event, context):
    print("=====================================================")
    print(event)

    # raw bucket
    source_bucket = event['Records'][0]['s3']['bucket']['name']
    object_key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')

    # Get S3 Operation
    s3_opt = event['Records'][0]['eventName']
    print(s3_opt)
    if s3_opt == 'ObjectRemoved:Delete':
        # Delete
        s3.delete_object(Bucket=LANDING_BUCKET, Key=BACKUP_DIR + object_key)
        print("File {} has been successfully deleted from Bucket {}".format(BACKUP_DIR + object_key, LANDING_BUCKET))
    else:
        # Copy this file to landing bucket
        copy_source = {'Bucket': source_bucket, 'Key': object_key}
        response = s3.copy_object(Bucket=LANDING_BUCKET, Key=BACKUP_DIR + '/' + object_key, CopySource=copy_source)
        print("This is the {} from bucket {}".format(object_key, source_bucket))

        # access file
        get_file = s3_object.get_object(Bucket=source_bucket,
                                        Key=object_key)
        print("Successfully got the log files!!!")

        # get file content
        get = get_file['Body']
        # if object_key.split('.')[-1] == 'log':
        names = ['timestamp', 'filed', 'value']
        df_parquet = pd.read_csv(get, delimiter=",", header=None)
        df_parquet = df_parquet.rename(columns={0: names[0], 1: names[1], 2: names[2]})
        df_parquet = df_parquet.reset_index()
        df_parquet = pd.DataFrame(df_parquet)
        # CANSERVER_v2_CANSERVER1664886251.078
        df_parquet.iloc[0, 1] = df_parquet.iloc[0, 1][22:]
        df_parquet[['timestamp']] = df_parquet[['timestamp']].astype(float)

        # Merge files which in same hour
        split_list, fn_list = get_start_end_time(df_parquet)

        """
        convert csv to parquet
        """
        print("start to write parquet file!!!")

        # Get the landing bucket path
        landing_bucket = s3_resource.Bucket(LANDING_BUCKET)
        exsit_file_list = ['']
        for object_summary in landing_bucket.objects.filter(Prefix=LAND_DIR):
            exsit_file_list.append(object_summary.key)
        print(exsit_file_list)

        for i in range(len(split_list)):
            part_df = df_parquet.iloc[split_list[i][0]:split_list[i][1] + 1, :]

            # search if have previous data of this hour
            name = LAND_DIR + fn_list[i] + '-00-00' + '.parquet'
            if name in exsit_file_list:
                print(name, " is exist in Landing Bucket, need to be updated")
                last_file = wr.s3.read_parquet(path='s3://' + LANDING_BUCKET + '/' + name)
                if last_file.iloc[-1, 1] < part_df.iloc[0, 1]:
                    part_df = pd.concat([last_file, part_df])
                else:
                    part_df = pd.concat([part_df, last_file])
            landing_path = 's3://' + LANDING_BUCKET + '/' + LAND_DIR + fn_list[i] + '-00-00' + '.parquet'
            print("landing path is: ", landing_path)

            try:
                wr.s3.to_parquet(
                    df=part_df,
                    path=landing_path,
                    dataset=False
                )
                print("Parquet file has been saved into landing bucket!!!")
            except Exception as e:
                print(e)
                print("writing to parquet failed!!!")