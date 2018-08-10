# coding = utf-8
import sys
import logging
import os
import datetime
import MySQLdb

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

conn = MySQLdb.connect(DB_HOST, DB_USER, DB_USER_PASSWORD, port=3306)
cursor = conn.cursor()
cursor.execute("show databases;")
DB_List_Temp = cursor.fetchall()
cursor.close()
conn.close()
print DB_List_Temp
print DB_List, len(DB_List)
for DB_Name in DB_List_Temp:
    for item in DB_Name:
        print type(item), item
        if item not in ('information_schema', 'mysql', 'performance_schema'):
            DB_List.append(item)
print DB_List, len(DB_List)

# Starting actual database backup process.
if len(DB_List) > 0:
    DB_Count = len(DB_List)
    p = 1
    while p <= DB_Count:
        DB_Name = DB_List[p-1]
        Backup_File_List.append(DB_Name + '-' + DATETIME)
        dumpcmd = "mysqldump -u " + DB_USER + " -p" + DB_USER_PASSWORD + " " + DB_Name + " > " + TODAYBACKUPPATH  + Backup_File_List[p-1] + ".sql"
        print dumpcmd
        os.system(dumpcmd)
        p = p + 1
print "Backup script completed"
print "Your backups has been created in '" + TODAYBACKUPPATH + "' directory"
print Backup_File_List