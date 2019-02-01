#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys, os, inspect
sys.path.append('utility_classes')
import pandas as pd
import requests
import json
from time import sleep
from datetime import datetime as dtime
import datetime 
import shutil
import collections as coll 
from importlib import reload #used only in Python 3.*

# Use this if you want to include modules from a subfolder
cmd_subfolder = os.path.realpath(os.path.abspath(os.path.join(os.path.split(inspect.getfile( inspect.currentframe() ))[0],"../common")))
if cmd_subfolder not in sys.path:
    sys.path.insert(0, cmd_subfolder)

from class_logger import objLogger as Logger
from class_config import objConfig as Config
from class_actionFile import dataRequests as req
from class_Mining import FileInfo
from class_Mining import createArray 


def main():
    info = config.sections['INGESTION']
    filepath = basepath + '/files/' + info['file']
    delimiter = info['delimiter'].replace('"', '')
    encoding = info['encoding']
    api_endpoint = info['url_api_test']
    chunkrows = int(info['chunkrows'])
    rows_to_read = info['rows_to_read']
    engine = info['engine']
    
    try:
        rows_to_read = int(rows_to_read)
    except:
        rows_to_read = None

    try:
        start = int(info['start'])
    except:
        start = None

    date = dtime.date(dtime.today())
    time = dtime.now()
    hasErrors = False

    columns = config.sections['COLUMNSINGESTION']
    arr_columns = createArray(columns)
    column_indexes = arr_columns[0]
    column_names = arr_columns[1]

    # Read file csv
    df = pd.read_csv( filepath, delimiter = delimiter , usecols = column_indexes,
                     names = column_names, skiprows=start, nrows = rows_to_read, dtype = str,
                     quotechar = '"', encoding = encoding, chunksize = chunkrows, engine=engine)

    # building errors file with headers 
    pd.DataFrame(columns=column_names).to_csv('errors.csv', quotechar='"', encoding='utf-8', index=False)

    logger.doLog( 'Inizio invio file')

    for chunk in df:
        
        records = chunk.to_json(orient='records')
        records = json.loads(records)
        
        detlist = len(chunk.index)

        for i in range(detlist):
            
            row_nr = (i + 1 + (start if start is not None else 0))
            detail = records[i]
            data = req(detail, api_endpoint)
            res = data.postRequest()
            code = res.status_code
            response = res.text 
            if code != 200:
                hasErrors = True
                logger.doLog( 'ERRORE inserimento riga numero: ' + str(row_nr) + ' ' + response)
                chunk.iloc[[i]].to_csv('errors.csv', mode='a',quotechar='"', encoding='utf-8', index=False, header=False)

    logger.doLog( 'Invio file completato')
    shutil.copy(basepath + '/errors.csv', basepath + '/history_errors/' + str(date) + '_' + str(time.hour) + str(time.minute) + "_errors.csv")
    os.remove(basepath + '/errors.csv')

    if hasErrors:
        sys.exit(1)                    
    else:
        sys.exit(0) 

if __name__ == '__main__':
    reload(sys)

    #Uncomment the line above if you use Python 2.* 
    # sys.setdefaultencoding('utf8')

    #determining path of execution
    basepath = os.path.dirname(os.path.realpath(__file__))
    logger = Logger(basepath + '/ingestion.log')
    config = Config(basepath + '/project.cfg')
    main()

