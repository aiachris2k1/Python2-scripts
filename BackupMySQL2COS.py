# coding = utf-8
from qcloud_cos import CosConfig
from qcloud_cos import CosS3Client
import sys
import logging
import os
import datetime
import MySQLdb
import urllib
import urllib2
import socket


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

    Logfile = 'RunningLog' + str(DATETIME) + '.log'
    # print Logfile
    FileHandler = open(Logfile, 'w')
    FileHandler.write("creating backup folder\r\n")

    # Checking if backup folder already exists or not. If not exists will create it.
    # print "creating backup folder"
    if not os.path.exists(TODAYBACKUPPATH):
        try:
            os.makedirs(TODAYBACKUPPATH)
            FileHandler.write("Backup folder created successfully!\r\n")
        except:
            FileHandler.write("Error:Backup folder created failed!\r\n")
    # Code for checking if you want to take single database backup or assinged multiple backups in DB_NAME.
    # print "checking for databases."
    # if os.path.exists(DB_NAME):
    #     file1 = open(DB_NAME)
    #     multi = 1
    #     print "Databases file found..."
    #     print "Starting backup of all dbs listed in file " + DB_NAME
    # else:
    #     print "Databases file not found..."
    #     print "Starting backup of database " + DB_NAME
    #     multi = 0

    conn = MySQLdb.connect(DB_HOST, DB_USER, DB_USER_PASSWORD)
    cursor = conn.cursor()
    cursor.execute("show databases;")
    DB_List_Temp = cursor.fetchall()
    cursor.close()
    conn.close()
    # print DB_List_Temp
    # print DB_List, len(DB_List)
    for DB_Name in DB_List_Temp:
        for item in DB_Name:
            # print type(item), item
            if item not in ('information_schema', 'mysql', 'performance_schema', 'sys'):
                DB_List.append(item)
    # print DB_List, len(DB_List)

    # Starting actual database backup process.
    if len(DB_List) > 0:
        DB_Count = len(DB_List)
        p = 1
        while p <= DB_Count:
            DB_Name = DB_List[p - 1]
            Backup_File_List.append(DB_Name + '-' + DATETIME + ".sql")
            dumpcmd = "mysqldump -u " + DB_USER + " -p" + DB_USER_PASSWORD + " " + DB_Name + " > " + TODAYBACKUPPATH + \
                      Backup_File_List[p - 1]
            os.system(dumpcmd)
            # BackupResult = os.popen(dumpcmd)
            print TODAYBACKUPPATH + Backup_File_List[p - 1]
            if os.path.exists(TODAYBACKUPPATH + Backup_File_List[p - 1]):
                FileHandler.write("Backup database " + DB_Name + " successfully!\r\n")
            else:
                FileHandler.write("Error:Backup database " + DB_Name + " failed!\r\n")
            # print "BackupResult:",  BackupResult
            p = p + 1
    FileHandler.write("Backup script completed\r\n")
    FileHandler.write("Your backups has been created in '" + TODAYBACKUPPATH + "' directory\r\n")
    FileHandler.close()
    return Backup_File_List, Logfile


def Upload2Cos(File_List):
    # print type(File_List)
    # print File_List[0]
    BACKUP_PATH = '/backup/dbbackup/'
    FileHandler = open(File_List[1], 'a')
    FileHandler.write("Starting upload processing\r\n")
    if len(File_List[0]) > 0:
        BucketName = 'backup-bucket'
        logging.basicConfig(level=logging.INFO, stream=sys.stdout)
        secret_id = 'none'
        secret_key = 'none'
        region = 'ap-guangzhou'
        token = ''
        scheme = 'https'
        config = CosConfig(Region=region, SecretId=secret_id, SecretKey=secret_key, Token=token, Scheme=scheme)
        client = CosS3Client(config)
        for item in File_List[0]:
            FileKey = item
            # TimeStamp = datetime.datetime.now().strftime('%Y%m%d%H%M%s')
            # FileKey = FileKey + '-' + TimeStamp

            # file_name = 'test.txt'
            # with open('/Users/chenzhiyuan/PycharmProjects/python2_scripts/InternetofThingswithPython.pdf', 'rb') as fp:
            #     response = client.put_object(
            #         Bucket='backup-bucket-1253400837',
            #         Body=fp,
            #         Key='InternetofThingswithPython.pdf',
            #         StorageClass='STANDARD',
            #         ContentType='text/html; charset=utf-8'
            #     )
            # print(response['ETag'])

            try:
                response = client.upload_file(
                    Bucket=BucketName,
                    LocalFilePath=BACKUP_PATH + FileKey,
                    Key=FileKey,
                    PartSize=10,
                    MAXThread=10
                )
                if response['ETag'] is None:
                    FileHandler.write(BACKUP_PATH + FileKey + " upload successfully!\r\n")
                    FileHandler.write(response['ETag'] + "\r\n")
                    os.system('rm -f ' + BACKUP_PATH + FileKey)
                else:
                    FileHandler.write("Error:" + BACKUP_PATH + FileKey + " upload failed!\r\n")
            except:
                FileHandler.write("Error:" + BACKUP_PATH + FileKey + " upload failed!\r\n")
                # FileHandler.write(response['ETag'])
    FileHandler.close()
    return File_List[1]


def ActionLogMail(message):
    MailBody = open(message, 'r').read()
    HostName = socket.getfqdn(socket.gethostname())
    if MailBody.find('Error:'):
        MailSubject = 'Warming:job MySQL2COS at ' + HostName + ' failed'
    else:
        MailSubject = 'Job MySQL2COS at ' + HostName + ' completed successfully'
    Mail = {}
    Mail['token'] = '61f51757bbdcf522fdd895c52c9a7f6d'
    Mail['toAddress'] = 'wenzuojing1@zy.com'
    Mail['subject'] = MailSubject
    Mail['content']= MailBody
    url_values = urllib.urlencode(Mail)
    Html = urllib2.urlopen('http://10.104.58.245:30592/api/message/sendEmail' + '?' + url_values).read()
    os.system('rm -f ' + message)
    if Html.find(":0,") < 0:
        return 1
    return 0


if __name__ == "__main__":
    result = BackupMySQL()
    uploadresult = Upload2Cos(result)
    ActionLogMail(uploadresult)
