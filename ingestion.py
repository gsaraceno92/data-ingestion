#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys, os, inspect
sys.path.append('../../python/python_common_libs')
import pandas as pd
from utils import *
import requests
import json
from time import sleep
# import reload
from datetime import datetime as dtime
import datetime 
import shutil 


# realpath() will make your script run, even if you symlink it :)
cmd_folder = os.path.realpath(os.path.abspath(os.path.split(inspect.getfile( inspect.currentframe() ))[0]))
if cmd_folder not in sys.path:
    sys.path.insert(0, cmd_folder)

# Use this if you want to include modules from a subfolder
cmd_subfolder = os.path.realpath(os.path.abspath(os.path.join(os.path.split(inspect.getfile( inspect.currentframe() ))[0],"../common")))
if cmd_subfolder not in sys.path:
    sys.path.insert(0, cmd_subfolder)

from class_logger import objLogger as Logger
from class_config import objConfig as Config


def replaceOne(column, strout, strin):
    column = column.replace(strout, strin)
    return column

def replaceAll(text, dic):
    for i, j in dic.iteritems():
        text = text.replace(i, j)
    return text


def sendData(row, url):
    headers = {'Content-Type': 'application/json'}
    row = json.dumps(row)
    # logger.doLog(str(row))
    result = ''
    result = requests.post(url, data=row, headers=headers)
    # sleep(0.01)  
    return result

def main():
    info = config.sections['GENERAL']
    filename = basepath + '/' + info['file']
    api_endpoint = info['url_api_test']
    # nrows = int(info['nrows'])
    read_rows = int(info['readrows'])
    start = int(info['start'])
    stop = int(info['stop'])
    date = dtime.date(dtime.today())
    time = dtime.now()
    hasErrors = False

    column_indexes = [0, 1, 2, 4, 5, 7, 8, 9, 10, 12, 13, 14]
    
    column_names = ['link', 'title','cover', 'publisher', 'description', 'authors', 'isbn',
                    'price', 'language', 'advertiser', 'genres', 'category']

    df = pd.read_csv( filename, delimiter=";",  usecols=column_indexes,names=column_names, skiprows=start, nrows=stop,
                     dtype = {"isbn" : "str"}, quotechar = '"',encoding = "utf-8", chunksize=read_rows)

    pd.DataFrame(columns=column_names).to_csv('errors.csv', quotechar='"', encoding='utf-8', index=False)

    for chunk in df:

        dict_genres = {'(Vuoto)' : '', 'sport' : 'sports'}
        chunk['genres'] = replaceAll(chunk['genres'], dict_genres)

        chunk['category'] = replaceOne(chunk['category'], '(Vuoto)', '')
        chunk['price'] = replaceOne(chunk['price'], ',', '.')

        nbooks = chunk.to_json(orient='records')
        nbooks = json.loads(nbooks)
        

        detlist = len(chunk.index)
        for i in range(detlist):
            
            row_nr = (i + 1 + start)
            detail = nbooks[i]
            # detail['price'] = float(str(detail['price']).replace(",", "."))
            
            res = sendData(detail, api_endpoint)
            code = res.status_code
            response = res.text 
            if code != 200:
                hasErrors = True
                # logger.doLog('Codice' + str(res) + '. ERRORE inserimento del libro con isbn ' + str(detail['isbn']) + ' in query nr ' + str(qry_idx))
                logger.doLog( 'riga numero: ' + str(row_nr) + ' ' + response)
                chunk.iloc[[i]].to_csv('errors.csv', mode='a',quotechar='"', encoding='utf-8', index=False, header=False)
    
    # os.rename("errors.csv", str(dtime.now()) + "_errors.csv")
    shutil.copy(basepath + '/errors.csv', basepath + '/history_errors/' + str(date) + '_' + str(time.hour) + str(time.minute) + "_errors.csv")
    os.remove(basepath + '/errors.csv')
    if hasErrors:
        sys.exit(1)                    
    else:
        sys.exit(0) 

if __name__ == '__main__':
    reload(sys)
    sys.setdefaultencoding('utf8')
    #determining path of execution
    basepath = os.path.dirname(os.path.realpath(__file__))
    logger = Logger(basepath + '/ingestion.log')
    config = Config(basepath + '/ubook.cfg')
    main()

