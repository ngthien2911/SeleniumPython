import paramiko
from paramiko import SSHClient, AutoAddPolicy
import mysql.connector
import boto3
from datetime import datetime, timezone
from dotenv import load_dotenv
import os

#Your database and ansible credential 
load_dotenv()
inputshard = input('shard: ')
shard=inputshard #shard number
sqluser=os.getenv('SQL_USER') #SQL username
sqlpassword=os.getenv('SQL_PASSWORD') #SQL password
auth_plugin='mysql_native_password'


def shard_related(shard):
    match shard:
        case '1':
           sqlIP=''
           port = '10306'
           redisContainer = 'shard1_accpmp-cache1'
           ansible = ''
           return sqlIP,port,redisContainer,ansible
        case '2':
           sqlIP=''
           port='3306'
           redisContainer=' redis_shard2-cache'
           ansible=''
           return sqlIP, port,redisContainer,ansible
        case '0':
           sqlIP=''
           port='3306'
           redisContainer='redis_accpmp-cache'
           ansible=''
           return  sqlIP,port,redisContainer,ansible
        case 'indigo':
           sqlIP=''
           port='3306'
           redisContainer='redis_accpmp-cache'
           ansible=''
           return  sqlIP,port,redisContainer,ansible
  
sqlIP=shard_related(shard)[0]
port = shard_related(shard)[1]
redisContainer=shard_related(shard)[2]
ansible=shard_related(shard)[3]

# print(sqlIP+ '\n'+port +'\n'+ redisContainer +'\n'+ ansible)

#your credential
connection = mysql.connector.connect(
host = sqlIP,
user = sqluser ,
password = sqlpassword,
database = "acc_pimp",
port=port,
auth_plugin='mysql_native_password'
)


#function to get the last aws record
def checkAws3(S_ID,profileRegion,selection):
    _PREFIX = f'{S_ID}/{selection}_2/'
    if inputshard=='indigo':
        BUCKET_NAME = f'ind-user-profiles-{profileRegion}'
    else:    
        BUCKET_NAME = f'mla-user-profiles-{profileRegion}'
    client = boto3.client('s3', aws_access_key_id=os.getenv('AWS_KEY_ID'),
                                aws_secret_access_key=os.getenv('AWS_SECRET'))#aws credential


    response = client.list_objects(Bucket=BUCKET_NAME, Prefix=_PREFIX)
    result=response['Contents']
    lst=list(result)

    maxtime = lst[0]['LastModified']
    maxprofile = lst[0]
    i=0
    for time in lst:
        time = lst[i]['LastModified']
        if time > maxtime:
            maxtime=lst[i]['LastModified']
            maxprofile=lst[i]
        else:
            maxtime
            maxprofile
        # print('maxprofile: '+maxprofile['Key'][-36:]+' max '+str(maxtime)+' currenttime '+str(time))
        i+=1
    return [maxprofile,i]


#import the profiles from the file
with open('profile.txt', 'r') as f:
    res = f.read()
res2 = res.split("\n")

#Process every single profile in the imported list
for x in range(len(res2)):
    profileId=res2[x]
    print('Processing profile: ',profileId)

    #get the current information of the profile in ap_browser_session_revisions and ap_browser_session_data
    cursor = connection.cursor()
    region ="""SELECT absr .profile_revision_id,absd.data_bytes,absr.extensions_revision_id,absd.ext_bytes,absr .BS_ID,abs2.object_store_region  from ap_browser_session abs2 join ap_browser_session_revisions absr on abs2.id = absr .BS_ID join ap_browser_session_data absd on abs2 .id = absd .BS_ID where abs2.S_ID=%s"""
    cursor.execute(region,(profileId,))
    record=cursor.fetchall()

    profile_revision_id = record[0][0]
    data_bytes=record[0][1]
    extensions_revision_id = record[0][2]
    ext_bytes= record[0][3]
    bs_id = record[0][4]
    objRegion = record[0][5]

    #get the last modified aws3 profile and its size
    awsProfileResult=checkAws3(profileId,objRegion,'profile')
    recentKey=awsProfileResult[0]['Key'][-36:]
    recentSize=awsProfileResult[0]['Size']
    recordAmount=awsProfileResult[1]

    try:
        awsExtResult = checkAws3(profileId,objRegion,'extensions')
        recentExtension=awsExtResult[0]['Key'][-36:]
        recentExtensionSize=awsExtResult[0]['Size']
        recordExtAmount=awsExtResult[1]
    except:
        recentExtension=None
        recentExtensionSize=0
    
    #Queries to update profile and extension
    updateProfilePhrase=f"""UPDATE ap_browser_session_revisions  SET profile_revision_id ='{recentKey}' WHERE BS_ID={bs_id}"""
    updateDataPhrase= f"""UPDATE ap_browser_session_data  SET data_bytes ={recentSize} WHERE BS_ID={bs_id}"""
    updateExtensionPhrase=f"""UPDATE ap_browser_session_revisions  SET extensions_revision_id ='{recentExtension}' WHERE BS_ID={bs_id}"""
    updateExtPhrase= f"""UPDATE ap_browser_session_data  SET ext_bytes ={recentExtensionSize} WHERE BS_ID={bs_id}"""


    #Print out the comparison
    print(f'<Aws profile_revision    | Database profile_revision    >< {recentKey}   | {profile_revision_id} >')
    print(f'<Aws data_bytes          | Database data_bytes          >< {recentSize}   | {data_bytes} >')
    print(f'<Aws extensions_revision | Database extensions_revision >< {recentExtension}   | {extensions_revision_id} >')
    print(f'<Aws ext_bytes           | Database ext_bytes           >< {recentExtensionSize}   | {ext_bytes} >')


    #Check conditions to see if it should update or not
    if recentKey==profile_revision_id and recentSize==data_bytes:
        print('Profile is up to date')
    else:
        print('Profile not up to date. Updating...')
        cursor.execute(updateProfilePhrase)
        cursor.execute(updateDataPhrase)

    if recentExtension==extensions_revision_id and recentExtensionSize==ext_bytes:
        print('Extension is up to date')
    elif recentExtension==None:
        print('Extension revision does not exist')
    else:
        print('Extension not up to date. Updating...')
        cursor.execute(updateExtensionPhrase)
        cursor.execute(updateExtPhrase)


    #Recheck information of the profile after update
    cursor.execute(region,(profileId,))
    record=cursor.fetchall()

    connection.commit()
    print('Current profile info:')
    for x in record:
        print(x)
    print('...Clearing cache...')


    #Clear cache using paramiko
    ssh = paramiko.SSHClient()
    ssh.load_system_host_keys()
    ssh.set_missing_host_key_policy(AutoAddPolicy())

    #Input your ansible username and path to SSH key
    ssh.connect(f'{ansible}', username='thien', key_filename='C:/Users/ACER/.ssh/id_MULTILOGIN') 
    stdin, stdout, stderr = ssh.exec_command(f'docker exec  $(docker container ls -q --filter label=com.docker.swarm.service.name={redisContainer}) redis-cli del  session-revisions-by-bs-id::{bs_id};docker exec  $(docker container ls -q --filter label=com.docker.swarm.service.name={redisContainer}) redis-cli del ACCPMP_session-by-id::{bs_id}')

    #Output the result of clearing cache
    stdout=stdout.readlines()
    ssh.close()
    print('... Cache Cleared')
    print('Process next profile....')
    

print('ALL DONE!')
input("Press enter to exit;")  