# coding = utf-8
from qcloud_cos import CosConfig
from qcloud_cos import CosS3Client
import sys
import logging
import os
import datetime
import MySQLdb


def BackupMySQL():
    # !/usr/bin/python
    ###########################################################
    #
    # This python script is used for mysql database backup
    # using mysqldump utility.
    #
    # Written by : Rahul Kumar
    # Website: http://tecadmin.net
    # Created date: Dec 03, 2013
    # Last modified: Dec 03, 2013
    # Tested with : Python 2.6.6
    # Script Revision: 1.1
    #
    ##########################################################

    # MySQL database details to which backup to be done. Make sure below user having enough privileges to take databases backup.
    # To take multiple databases backup, create any file like /backup/dbnames.txt and put databses names one on each line and assignd to DB_NAME variable.

    DB_HOST = 'localhost'
    DB_USER = 'root'
    DB_USER_PASSWORD = '123456'
    # DB_NAME = '/backup/dbnames.txt'
    DB_List = []
    Backup_File_List = []
    BACKUP_PATH = '/backup/dbbackup/'

    # Getting current datetime to create seprate backup folder like "12012013-071334".
    DATETIME = datetime.datetime.now().strftime('%Y%m%d%H%M%s')

    TODAYBACKUPPATH = BACKUP_PATH

    # Checking if backup folder already exists or not. If not exists will create it.
    print "creating backup folder"
    if not os.path.exists(TODAYBACKUPPATH):
        os.makedirs(TODAYBACKUPPATH)

    # Code for checking if you want to take single database backup or assinged multiple backups in DB_NAME.
    print "checking for databases."
    # if os.path.exists(DB_NAME):
    #     file1 = open(DB_NAME)
    #     multi = 1
    #     print "Databases file found..."
    #     print "Starting backup of all dbs listed in file " + DB_NAME
    # else:
    #     print "Databases file not found..."
    #     print "Starting backup of database " + DB_NAME
    #     multi = 0

    conn = MySQLdb.connect(DB_HOST, DB_USER, DB_USER_PASSWORD )
    cursor = conn.cursor()
    cursor.execute("show databases;")
    DB_List_Temp = cursor.fetchall()
    cursor.close()
    conn.close()
    #print DB_List_Temp
    #print DB_List, len(DB_List)
    for DB_Name in DB_List_Temp:
        for item in DB_Name:
            print type(item), item
            if item not in ('information_schema', 'mysql', 'performance_schema', 'sys'):
                DB_List.append(item)
    #print DB_List, len(DB_List)

    # Starting actual database backup process.
    if len(DB_List) > 0:
        DB_Count = len(DB_List)
        p = 1
        while p <= DB_Count:
            DB_Name = DB_List[p - 1]
            Backup_File_List.append(DB_Name + '-' + DATETIME + ".sql")
            dumpcmd = "mysqldump -u " + DB_USER + " -p" + DB_USER_PASSWORD + " " + DB_Name + " > " + TODAYBACKUPPATH + \
                      Backup_File_List[p - 1]
            #print dumpcmd
            os.system(dumpcmd)
            p = p + 1
    print "Backup script completed"
    print "Your backups has been created in '" + TODAYBACKUPPATH + "' directory"
    return Backup_File_List


def Upload2Cos(File_List):

    BACKUP_PATH = '/backup/dbbackup/'
    if len(File_List) > 0:
        BucketName = 'backup-bucket'
        logging.basicConfig(level=logging.INFO, stream=sys.stdout)
        secret_id = 'none'
        secret_key = 'none'
        region = 'ap-guangzhou'
        token = ''
        scheme = 'https'
        config = CosConfig(Region=region, SecretId=secret_id, SecretKey=secret_key, Token=token, Scheme=scheme)
        client = CosS3Client(config)
        for item in File_List:
            FileKey = item
            #TimeStamp = datetime.datetime.now().strftime('%Y%m%d%H%M%s')
            #FileKey = FileKey + '-' + TimeStamp


            #file_name = 'test.txt'
            # with open('/Users/chenzhiyuan/PycharmProjects/python2_scripts/InternetofThingswithPython.pdf', 'rb') as fp:
            #     response = client.put_object(
            #         Bucket='backup-bucket-1253400837',
            #         Body=fp,
            #         Key='InternetofThingswithPython.pdf',
            #         StorageClass='STANDARD',
            #         ContentType='text/html; charset=utf-8'
            #     )
            # print(response['ETag'])

            response = client.upload_file(
                Bucket=BucketName,
                LocalFilePath=BACKUP_PATH + FileKey,
                Key=FileKey,
                PartSize=10,
                MAXThread=10
            )
            os.system('rm -f ' + BACKUP_PATH + FileKey)
    return response['ETag']

    #

    # response = client.get_object(
    #     Bucket='test04-123456789',
    #     Key=file_name,
    # )
    # response['Body'].get_stream_to_file('output.txt')



if __name__ == "__main__":
    result = BackupMySQL()
    Upload2Cos(result)
