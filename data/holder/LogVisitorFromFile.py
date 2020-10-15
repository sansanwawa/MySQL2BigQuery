



class LogVisitorFromFile(object):

    def __init__(self,index,ipAddr,dateTime,method,browser,url,urlComplete):
        self.__tableName    = 'log_ip_tmp'
        self.__index        = index
        self.__ipAddr       = ipAddr
        self.__dateTime     = dateTime
        self.__method       = method
        self.__browser      = browser
        self.__url          = url
        self.__urlComplete  = urlComplete
        

    def getIndex(self):
        return self.__index
    
    def getIpAddr(self):
        return self.__ipAddr
    
    def getDateTime(self):
        return self.__dateTime

    def getUrl(self):
        return self.__url

    def getUrlComplete(self):
        return self.__urlComplete
    
    def getBrowser(self):
        return self.__browser
    
    def getMethod(self):
        return self.__method
        
    
    def __repr__(self):
        return '{"index":"' + str(self.__index) + '", "IPAddr":"' + str(self.__ipAddr) + '","dateTime":"' + str(self.__dateTime) + '"}'

    def getInsertSQLStatement(self):
        sql  = 'INSERT INTO ' + self.__tableName + ' '
        sql += 'SET ipAddr="' + str(self.__ipAddr) + '"'
        sql += ',accessDateTime="' + str(self.__dateTime) + '"'
        sql += ',method="' + str(self.__method) + '"'
        sql += ',browser="' + str(self.__browser) + '"'
        sql += ',url="' + str(self.__url) + '"'
        sql += ',urlComplete="' + str(self.__urlComplete) + '"'
        sql += ';'
        return sql
       
    def getDeleteSQLStatement(self):
        return 'DELETE FROM ' + self.__tableName + ' WHERE date(insertDateTime) < DATE(NOW());'

