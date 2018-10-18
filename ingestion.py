#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys, os, inspect
sys.path.append('../../python/python_common_libs')
import pandas as pd
import tarfile, zipfile, gzip
from utils import *
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import json
from time import sleep
# import reload
from datetime import datetime as dtime


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
    result = requests.post(str(url), data=row, headers=headers)
    # sleep(0.01)  
    return result.status_code

def main():
    info = config.sections['GENERAL']
    filename = basepath + '/' + info['file']
    api_endpoint = info['url_api_test']

    nrows = int(info['nrows'])
    read_rows = int(info['readrows'])
    nr_repeat = nrows/read_rows
    last_repeat = nrows % read_rows
    start = int(info['start'])
    
    hasErrors = False

    for qry_idx in range(nr_repeat + 1):
        skip_rows = (int(read_rows) * qry_idx) + 1
        if qry_idx > nr_repeat:
            read_rows = last_repeat

        df = pd.read_csv( filename, delimiter=";", dtype = {"isbn" : "str"}, 
                            quotechar = '"',encoding = "utf-8", skiprows=range(start, skip_rows), nrows=read_rows)

        books = df[['title', 'price','isbn', 'authors', 'publisher', 'link', 'advertiser',
                    'cover', 'genres', 'category', 'language', 'description']]

        dict_genres = {'(Vuoto)' : '', 'sport' : 'sports'}
        books['genres'] = replaceAll(books['genres'], dict_genres)


        nbooks = books.to_json(orient='records')
        nbooks = json.loads(nbooks)
        

        detlist = len(books.index)

        for i in range(detlist):
            loc_detail = nbooks.iloc[i]
            detail = nbooks[i]
            detail['price'] = float(str(detail['price']).replace(",", "."))
            
            res = sendData(detail, api_endpoint)
            if res != 200:
                hasErrors = True
                logger.doLog('Codice' + str(res) + '. ERRORE inserimento del libro con isbn ' + str(detail['isbn']) + ' in query nr ' + str(qry_idx))
            
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

