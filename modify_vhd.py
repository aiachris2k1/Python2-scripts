# coding = utf-8
import os
import smtplib
import socket
import time
import shutil
import fileinput

HostName = socket.getfqdn(socket.gethostname())
FilePath = "/share/"
DestPath = "/data/"
FileName = "sharefile.vhd"
MountConfig = '/etc/rc.local'
RsnapshotConfig = '/etc/rsnapshot.conf'
FileSize = os.path.getsize(FilePath + FileName)
AvailableSize = os.statvfs(DestPath).f_frsize * os.statvfs(DestPath).f_bavail
RunningHour = time.strftime("%H")
RestartResult = ""
if 6 < int(RunningHour) < 20:
    RestartResult = "Please run the job between 20 to 6 next day.\r\n"
else:
    #print("Running")
    if FileSize < AvailableSize:
        #print("Phase 2")
        shutil.copyfile(FilePath + FileName, DestPath + FileName)  #复制VHD到磁盘a
        RestartResult = RestartResult + "step copy completed.\r\n"
        for line in fileinput.input(MountConfig, inplace=True, backup='.bak'):
            assert isinstance(line.replace(FilePath, FilePath).strip, object)
            line = line.replace('/srv/',  '/data/').strip()
            print(line)
        fileinput.close()
        RestartResult = RestartResult + "Modify rc.local completed.\r\n"
        for line in fileinput.input(RsnapshotConfig, inplace=True, backup='.bak'):
            if line.strip == 'snapshot_root  /data/rsnapshot':
                print('#' + line.strip())
                print('snappshot_root  /srv/rsnapshot')
            else:
                print(line.strip())
        fileinput.close()
        RestartResult = RestartResult + "Modify rsnapshot.conf completed.\r\n"
        CommandOutput = os.popen("/etc/init.d/samba restart")
        RestartResult = RestartResult + CommandOutput.read()
    else:
        RestartResult = "Disk Fulled!"
HOST = 'smtp.zy.com'
SUBJECT = 'VHD relocation Job @' + socket.getfqdn(socket.gethostname())
TO = 'chenzhiyuan@zy.com;yangshangbin@zy.com'
FROM = 'app@zy.com'
BODY = "From:" + FROM + "\r\n" + "To:" + TO + "\r\n" + "Subject:" +  SUBJECT + "\r\n" + \
       'Job completed \r\n' + RestartResult
Server = smtplib.SMTP()
Server.connect(HOST, '25')
Server.login(FROM, "App_Notify_2016")
Server.sendmail(FROM, TO, BODY)
Server.quit()
