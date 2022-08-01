import json
import boto3
import json
import requests
from datetime import datetime
from pytz import timezone
import os

os.chdir('/tmp')

def createFolder(directory):
    client = boto3.client("s3")
    bucket_name = "ops-aico-analysis-config"  # 버킷명 정의
    folder_name = directory
    result = client.list_objects(Bucket=bucket_name, Prefix=folder_name)
    try:
        result["Contents"]
    except:
        client.put_object(Bucket=bucket_name, Key=(folder_name))
        print("Error: Creating directory. " + directory)


def lambda_handler(event,context):
    # datetime.now(timezone('KST')).strftime("%Y-%m-%d %H:%M:%S")
    now_time = datetime.now(timezone("Asia/Seoul"))

    # 여기만 고치고 실행하면됨!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    #  date : 원하는 날짜 입력 default는 today로 설정되어있음
    # model_name에 원하는 모델을  됨!!

    date = now_time.strftime("%Y-%m-%d")  # 수행일자 ex)2021-12-24
    # date = "2022-01-04"

    try:
        if(len(event['date'])==10):
            date=event['date']
    except Exception as e:
        print('You did not enter the date or entered it incorrectly.. So, start today date')
    
    model_index=0

    try:
        if(event['model']>0):
            model_index=event['model']
    except Exception as e:
        print('You did not enter the model name or entered it incorrectly.. So, start all model')

    model_index-=1

    model_list=['UNDERSTANDING', 'SCORE_PREDICTION', 'SOLVING_HABIT']
    # (1) UNDERSTANDING : 이해도 모델 수행시, 이해도 바뀜 주의!!!!!!!!!!!!!!!!!!!!!!!!!!
    # (2) SCORE_PREDICTION : 예상 정답률 모델
    # (3) SOLVING_HABIT : 습관 모델
    
    if (model_index==-1):
        for model_name in model_list:
            # model_name = "SOLVING_HABIT"
            client = boto3.client("s3")
            bucket = "ops-service-api-logs" # 로그 s3
            analysis_bucket_name = "ops-aico-analysis-config" # 분석 s3 
            prefix = f"aicando/analysis_call_errors/{model_name}/p_year={date[:4]}/p_month={date[5:7]}/p_day={date[8:]}"
            # log json file 읽기
            paginator = client.get_paginator("list_objects_v2")
            page_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)
            key_list = []
            for page in page_iterator:
                for content in page["Contents"]:
                    key_list = key_list + [content["Key"]]
            print("수행해야 할 대상 수:", len(key_list))

            folder_name = "common/error_log/{}/{}/".format(model_name, date)
            model_folder_name = "common/error_log/{}/".format(model_name)

            createFolder(folder_name)

            date_time = now_time.strftime("%Y-%m-%d_%H_%M_%S")
            # s3_path='/tmp/'
            file_name = "{}_log.txt".format(date_time)

            # f = open(s3_path+file_name, "w")
            f = open(file_name, "w")
            for key in key_list:
                f.write(key)
                f.write("\n")
            f.close()
        
            s3 = boto3.client("s3")
            s3.put_object(Bucket=analysis_bucket_name, Key="{}{}".format(folder_name, file_name), Body=open(file_name, "rb"))
        
            f = open(file_name, "r")
            total_list = f.readlines()
            f.close()
            fin_count = 0


            fin_file_name = "fin_list.txt"
            except_file_name = "exception_list.txt"
            fail_file_name = "fail_list.txt"

            try:
                client.download_file(
                    # bucket, "{}{}".format(folder_name, fin_file_name), s3_path+fin_file_name
                    analysis_bucket_name, "{}{}".format(folder_name, fin_file_name), fin_file_name
                )
                # f = open(s3_path+fin_file_name, "r")
                f = open(fin_file_name, "r")
                fin_list = f.readlines()
                fin_count = len(fin_list)
                f.close()
            except Exception as e:
                createFolder("{}{}".format(folder_name, fin_file_name))
                fin_list = []

            try:
                client.download_file(
                    # bucket, "{}{}".format(model_folder_name, except_file_name), s3_path+except_file_name
                    analysis_bucket_name, "{}{}".format(model_folder_name, except_file_name), except_file_name
                )
                # f = open(s3_path+except_file_name, "r")
                f = open(except_file_name, "r")
                exception_list = f.readlines()
                f.close()
            except Exception as e:
                createFolder("{}{}".format(model_folder_name, except_file_name))
                exception_list = []


            if len(exception_list) == 0:
                todo_list = list(set(total_list) - set(fin_list))
            else:
                todo_list = list(set(total_list) - set(fin_list) - set(exception_list))

            fail_list = []

            for item in todo_list:
                item = item.replace("\n", "")
                try:
                    bucket = "ops-service-api-logs"
                    json_key = item
                    client = boto3.client("s3")
                    obj = client.get_object(
                        Bucket=bucket,
                        Key=json_key,
                    )
                    json_file = json.loads(obj["Body"].read())
                    file_nm = json_file["data"]["request"]["requestParam"]["s3FileName"]
                    file_path = json_file["data"]["request"]["requestParam"]["s3FolderPath"]
                    child_list = json_file["data"]["request"]["requestParam"]["childAnalysisCodes"]
                    file_url = json_file["data"]["request"]["requestUrl"]
                    headers = {
                        "Content-Type": "application/json; chearset=utf-8",
                        "product-code": "aicando",
                    }
                    data = {
                        "s3BucketName": "ops-service-tier1",
                        "s3FolderPath": f"{file_path}",
                        "s3FileName": f"{file_nm}",
                        "childAnalysisCodes": child_list,
                    }
                    res = requests.post(file_url, data=json.dumps(data), headers=headers)
                    print(str(res.status_code) + " | " + res.text)
                    if res.status_code != 200:
                        raise Exception()
                    fin_list.append(item + "\n")
                except Exception as e:
                    print(e)
                    fail_list.append(item + "\n")

            if len(fail_list) > 0:
                # f = open(s3_path+fail_file_name, "w")
                f = open(fail_file_name, "w")
                for key in fail_list:
                    f.write(key)
                f.close()
                client.upload_file(
                    # s3_path+fail_file_name, bucket, "{}{}".format(model_folder_name, fail_file_name)
                    fail_file_name, analysis_bucket_name, "{}{}".format(model_folder_name, fail_file_name)
                )
            else:
                createFolder("{}{}".format(model_folder_name, fail_file_name))

            if len(fin_list) > fin_count:
                f = open(fin_file_name, "w")
                # f = open(s3_path+fin_file_name, "w")
                for key in fin_list:
                    f.write(key)
                f.close()
                # client.upload_file(s3_path+fin_file_name, bucket, "{}{}".format(folder_name, fin_file_name))
                client.upload_file(fin_file_name, analysis_bucket_name, "{}{}".format(folder_name, fin_file_name))
            
            return 'fail_list :{}'.format(str(fail_list))
    
    else:
        model_name=model_list[model_index]
        # model_name = "SOLVING_HABIT"


        client = boto3.client("s3")
        bucket = "ops-service-api-logs" # 로그 s3
        analysis_bucket_name = "ops-aico-analysis-config" # 분석 s3 
        prefix = f"aicando/analysis_call_errors/{model_name}/p_year={date[:4]}/p_month={date[5:7]}/p_day={date[8:]}"
        # log json file 읽기
        paginator = client.get_paginator("list_objects_v2")
        page_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)
        key_list = []
        for page in page_iterator:
            for content in page["Contents"]:
                key_list = key_list + [content["Key"]]
        print("수행해야 할 대상 수:", len(key_list))

        folder_name = "common/error_log/{}/{}/".format(model_name, date)
        model_folder_name = "common/error_log/{}/".format(model_name)

        createFolder(folder_name)

        date_time = now_time.strftime("%Y-%m-%d_%H_%M_%S")
        # s3_path='/tmp/'
        file_name = "{}_log.txt".format(date_time)

        # f = open(s3_path+file_name, "w")
        f = open(file_name, "w")
        for key in key_list:
            f.write(key)
            f.write("\n")
        f.close()
    
        s3 = boto3.client("s3")
        s3.put_object(Bucket=analysis_bucket_name, Key="{}{}".format(folder_name, file_name), Body=open(file_name, "rb"))
    
        f = open(file_name, "r")
        total_list = f.readlines()
        f.close()
        fin_count = 0


        fin_file_name = "fin_list.txt"
        except_file_name = "exception_list.txt"
        fail_file_name = "fail_list.txt"

        try:
            client.download_file(
                # bucket, "{}{}".format(folder_name, fin_file_name), s3_path+fin_file_name
                analysis_bucket_name, "{}{}".format(folder_name, fin_file_name), fin_file_name
            )
            # f = open(s3_path+fin_file_name, "r")
            f = open(fin_file_name, "r")
            fin_list = f.readlines()
            fin_count = len(fin_list)
            f.close()
        except Exception as e:
            createFolder("{}{}".format(folder_name, fin_file_name))
            fin_list = []

        try:
            client.download_file(
                # bucket, "{}{}".format(model_folder_name, except_file_name), s3_path+except_file_name
                analysis_bucket_name, "{}{}".format(model_folder_name, except_file_name), except_file_name
            )
            # f = open(s3_path+except_file_name, "r")
            f = open(except_file_name, "r")
            exception_list = f.readlines()
            f.close()
        except Exception as e:
            createFolder("{}{}".format(model_folder_name, except_file_name))
            exception_list = []


        if len(exception_list) == 0:
            todo_list = list(set(total_list) - set(fin_list))
        else:
            todo_list = list(set(total_list) - set(fin_list) - set(exception_list))

        fail_list = []

        for item in todo_list:
            item = item.replace("\n", "")
            try:
                bucket = "ops-service-api-logs"
                json_key = item
                client = boto3.client("s3")
                obj = client.get_object(
                    Bucket=bucket,
                    Key=json_key,
                )
                json_file = json.loads(obj["Body"].read())
                file_nm = json_file["data"]["request"]["requestParam"]["s3FileName"]
                file_path = json_file["data"]["request"]["requestParam"]["s3FolderPath"]
                child_list = json_file["data"]["request"]["requestParam"]["childAnalysisCodes"]
                file_url = json_file["data"]["request"]["requestUrl"]
                headers = {
                    "Content-Type": "application/json; chearset=utf-8",
                    "product-code": "aicando",
                }
                data = {
                    "s3BucketName": "ops-service-tier1",
                    "s3FolderPath": f"{file_path}",
                    "s3FileName": f"{file_nm}",
                    "childAnalysisCodes": child_list,
                }
                res = requests.post(file_url, data=json.dumps(data), headers=headers)
                print(str(res.status_code) + " | " + res.text)
                if res.status_code != 200:
                    raise Exception()
                fin_list.append(item + "\n")
            except Exception as e:
                print(e)
                fail_list.append(item + "\n")

        if len(fail_list) > 0:
            # f = open(s3_path+fail_file_name, "w")
            f = open(fail_file_name, "w")
            for key in fail_list:
                f.write(key)
            f.close()
            client.upload_file(
                # s3_path+fail_file_name, bucket, "{}{}".format(model_folder_name, fail_file_name)
                fail_file_name, analysis_bucket_name, "{}{}".format(model_folder_name, fail_file_name)
            )
        else:
            createFolder("{}{}".format(model_folder_name, fail_file_name))

        if len(fin_list) > fin_count:
            f = open(fin_file_name, "w")
            # f = open(s3_path+fin_file_name, "w")
            for key in fin_list:
                f.write(key)
            f.close()
            # client.upload_file(s3_path+fin_file_name, bucket, "{}{}".format(folder_name, fin_file_name))
            client.upload_file(fin_file_name, analysis_bucket_name, "{}{}".format(folder_name, fin_file_name))
        
        return 'fail_list :{}'.format(str(fail_list))

