import mysql.connector
import time
from multiprocessing import Process, Queue

class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

def CONNECT_DB_CURSOR(DB_NAME='test', host= 'localhost', user= 'root', password= 'Aboutx_121'):
    i = 0
    while True:
        try:
            connect = mysql.connector.connect(
                host= host,
                user= user,
                password= password,
                # database= DB_NAME,
                port= 3306)
            i += 1
        except mysql.connector.DatabaseError as e:
            print (f"{bcolors.FAIL}Error:{bcolors.OKGREEN}\t{e} ... {i}{bcolors.ENDC}")
            time.sleep(0.1)
        except mysql.connector.OperationalError as e:
            print (f"{bcolors.FAIL}Error:{bcolors.OKGREEN}\t{e} ... {i}{bcolors.ENDC}")
            time.sleep(0.1)
        if connect.is_connected():
            break
    return 
if __name__ == "__main__":
    jobs = []
    for i in range(10):
        P = Process(target= CONNECT_DB_CURSOR)
        P.start()
        jobs.append(P)
    for P in jobs:
        P.join()
    