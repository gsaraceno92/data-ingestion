import os
import time
class objLogger(object):
    def __init__(self, logfile):
        self.logfile = logfile

    def doLog(self, text):
        # if os.path.isfile(self.logfile):
        out_file = open(self.logfile, "a")
        tStr = time.strftime("%d/%m/%Y %H:%M:%S")+' '+text+"\n"
        out_file.write(tStr)
        out_file.close()
