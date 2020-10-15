
import ipaddress


class IPChecker(object):
      
    def __init__(self,ipaddr):
          self.__isValidIP = True
          self.__ipAddr = ipaddr
          self.__ipVersion =''
          
          
          try:
            ip = ipaddress.ip_address(ipaddr)
            self.__ipVersion = ip.version
          except ValueError:
             self.__isValidIP = False
    
    def isValid(self):
        return self.__isValidIP
    
    def getIPVersion(self):
        return self.__ipVersion
    
    def getIPAddr(self):
        return self.__ipAddr
