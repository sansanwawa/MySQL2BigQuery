# File name: main.py
# Author: Sandy Haryono <sandyharyono@gmail.com>
# Python Version: 3.7.5

import config.dev as cfg

#import google cloud lib
from google.cloud.bigquery.client import Client
from google.cloud import bigquery

import sys
import binascii
import socket
import struct
import json
import time
import ipaddress

#mysql
import pymysql.cursors

#include helper
from helper.helper import get_type,BINARY_TYPES


#Software Info
SOFTWARENAME                    = 'MySQL to BigQuery Importer'
VERSION                         = 'v.1'

#BigQuery Configuration
PROJECT_ID                      = cfg.bq['projectId']
DATASET_ID                      = cfg.bq['datasetId']
SECRET_SERVICE_ACCOUNT_KEY_FILE = cfg.bq['accountKeyFile']   

# Mysql configuration
DB_HOST                         = cfg.mysql['host']
DB_USER                         = cfg.mysql['user']
DB_PASSWORD                     = cfg.mysql['password']
DB_NAME                         = cfg.mysql['db']



#SQL statement that we need to capture it
SQL_1                         = 'SELECT * FROM `table1`'
SQL_2                         = 'SELECT * FROM `table2`'



tables = [
    ['TableAtBigQuery1' , SQL_1 ],        #table[0] : tableName, table[1] : SQL statement, 
    ['TableAtBigQuery2' , SQL_2 ] 
]



class MySQLDatabase(object):

    #Constructor
    def __init__(self):
       self.connection = pymysql.connect(
            host        =   DB_HOST,
            user        =   DB_USER,
            password    =   DB_PASSWORD,
            db          =   DB_NAME,
            cursorclass =   pymysql.cursors.SSCursor)
    
    def execute(self):
        print('please see toBigQuery()')
        
    def toBigQuery(self):
        if self.connection.open == True:
            for table in tables:
                print('Executing {}'.format(table[0]))
                QueryBuilder(self.connection,table[0],table[1]).run()    
                #time.sleep(5)            
        else:
            print('Connection Could not established!') 

    def getConnection(self):
        return self.connection
    
    def insert(self,table, datas):
        cursor = self.connection.cursor() 
        cursor.execute(datas[0].getDeleteSQLStatement())

        print('Prepare to insert {}'.format(len(datas)))
        if type(datas) is list:
            for data in datas:
                cursor.execute(data.getInsertSQLStatement())
        #commit
        self.connection.commit()

    def getOne(self,table,field, where):
        cursor = self.connection.cursor() 
        sql = 'SELECT {field} FROM {table} WHERE {where} LIMIT 1'.format(field = field,table = table,where = where)
        cursor.execute(sql)
        rows = cursor.fetchall()
		cursor.close()
      
        try:
            return rows[0][0]
        except:
            pass


       

#Query Builder Class
class QueryBuilder(object):

     def __init__(self,connection,table_name,sql):
        self.connection     = connection
        self.table_name     = table_name
        self.sql            =  sql
        self.__schema       = []
        
     def delete(self):
        schema=[]
        BigQueryPlugIn(self.table_name,schema).delete()    
          
     def run(self):
        with self.connection.cursor() as cur:
            cur.execute(self.sql)
            description = cur.description
            totalColumn = 0
            for column in description:
                self.__schema.append(bigquery.SchemaField(column[0],get_type(column[1])))
                totalColumn += 1
            
            rows = cur.fetchall()
            #start Build a Json
            dataRows = '['
            for row in rows:
                strTmp = '{'                
                for i in range(0,totalColumn-1):
                    
                    s = str(row[i])
                    if str(row[i]) == 'None' and (get_type(description[i][1])=='INTEGER' or get_type(description[i][1])=='FLOAT'):
                        s = str(0)
                    elif str(description[i][0]) == "location_ip":
                        if len(row[i]) > 10:
                            #ipv6_addr
                            ipv6_addr = binascii.b2a_hex(row[i]).decode("utf-8") 
                            s = str(ipaddress.ip_address(int(ipv6_addr, 16)))
                        else :    
                            ipv4_addr = struct.pack('!I',int(binascii.b2a_hex(row[i]),16))
                            s = str(socket.inet_ntoa( ipv4_addr ))
                        
                        if self.table_name == 'LogVisitorMaster':
                            if len(row[i]) > 10:
                                ip6 = s.replace(':','')
                                ip6Len = len(ip6)
                                dbIp = MySQLDatabase().getOne('log_ip_tmp','ipAddr', 'SUBSTRING(REPLACE(ipAddr,":",""),1,{})="{}"'.format(ip6Len,ip6))
                                if dbIp != None:
                                    s = dbIp
                                #print('{} {}'.format(s,ip6Len))
                            else:
                                ip4 = s.split('.')
                                ip4 = "{}.{}.{}".format(ip4[0],ip4[1],ip4[2])
                                dbIp = MySQLDatabase().getOne('log_ip_tmp','ipAddr', 'SUBSTRING_INDEX(ipAddr,".",3) ="{}"'.format(ip4))
                                if dbIp != None:
                                    s = dbIp
                             #print('binary type [{}] - {} - {}' .format(len(row[i]),row[i], s))
                    elif str(description[i][0]) == "config_id":
                        cfg_int = int(binascii.b2a_hex(row[i]),16)
                        s = str(cfg_int)
                        #print('binary type [{}] - {} - {}' .format(len(row[i]),row[i], s))
                    elif str(description[i][0]) == 'idvisitor':
                        s = str( binascii.b2a_hex(row[i]).decode('ASCII').upper() )
                        
                
                    strTmp += '"' + description[i][0] + '":"' + s + '",'
                strTmp = strTmp[:-1]
                strTmp +='},'    
                dataRows += strTmp
            
            if(len(dataRows)>1):
                dataRows = dataRows[:-1]    
            dataRows = dataRows + ']'
            
            json_data = json.loads(dataRows)
            BigQueryPlugIn(self.table_name,self.__schema).createTable()
            BigQueryPlugIn(self.table_name,self.__schema).insert(json_data)
            print('---------------------------------------------------')
          


class BigQueryPlugIn(object):
     
   
    #Constructor
    def __init__(self,TABLE_ID,SCHEMA):
        self.__client   = Client.from_service_account_json(SECRET_SERVICE_ACCOUNT_KEY_FILE, project = PROJECT_ID)
        self.__schema   = SCHEMA
        self.__table_id = '{}.{}.{}'.format(PROJECT_ID,DATASET_ID,TABLE_ID)
        print("Checking a Table Id : " + self.__table_id)

    
    def createTable(self):
        #Check if there is already table or not
        #if there no table exists, then not required to create the table
        if(self.isTableExist() == False):
            table = bigquery.Table(self.__table_id, schema=self.__schema)
            table = self.__client.create_table(table) 
            print("Table doesnt exists, creating a table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))
            #set a delay if there is no table has been created
            time.sleep(17)
        else:
            print("Table exists,no need creating a table")


    def isTableExist(self):
        try:
            self.__client.get_table(self.__table_id)
            return True
        except:
            return False
    
    def insert(self,rows_to_insert):
        print('total data : {}'.format(str(len(rows_to_insert))) )
        if len(rows_to_insert) > 0:
            errors = self.__client.insert_rows_json(self.__table_id, rows_to_insert)  # Make an API request.
            if errors == []:
                print("New rows have been added.")
            else:
                print("Encountered errors while inserting rows: {}".format(errors))


    def delete(self):        
        d = 'DELETE FROM {} WHERE true'.format(self.__table_id)
        print(d)
        q = self.__client.query(d)
        q.result()



if __name__ == '__main__':
    import logging
    logging.basicConfig(level=logging.INFO)
    log = logging.getLogger(__name__)
    MySQLDatabase().toBigQuery()
    
    
