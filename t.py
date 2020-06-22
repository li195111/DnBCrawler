import pymysql

def CONNECT_DB_CURSOR(DB_NAME='test', host= 'localhost', user= 'root', password= 'Aboutx_121'):
    while True:
        connect = pymysql.connect(
            host= host,
            user= user,
            password= password,
            database= DB_NAME,
            port= 3306)
        connect.close()
    return 

if __name__ == "__main__":
    CONNECT_DB_CURSOR()
    
    pass