import sys, os, inspect
sys.path.append('../../python/python_common_libs')
sys.path.append('utility_classes')
import pandas as pd
import collections as coll 
from class_logger import objLogger as Logger
from class_config import objConfig as Config
from class_Mining import FileInfo
from class_Mining import createArray


def main():
    info = config.sections['GENERAL']
    filename = basepath + '/' + info['file']
    delimiter = info['delimiter'].replace('"', '')
    encoding = info['encoding']
    engine = info['engine']
    merge = bool(info['flag_merge'])
    modify = bool(info['flag_modify'])

    if not merge and not modify:
        logger.doLog('No action requested')
        sys.exit(1)

    info2 = config.sections['MERGEFILE']
    filename2 = basepath + '/' + info2['file']
    delimiter2 = info2['delimiter'].replace('"', '')
    encoding2 = info2['encoding']
    engine = info2['engine']

    columns = config.sections['COLUMNS']
    arr_columns = createArray(columns)
    column_indexes = arr_columns[0]
    column_names = arr_columns[1]

    # Read file csv
    df = pd.read_csv( filename, delimiter = delimiter , usecols = column_indexes, 
                    names = column_names, dtype = {"isbn" : "str"},
                    quotechar = '"', encoding = encoding, engine=engine)
    logger.doLog('read first file')

    b1 = FileInfo(filename, df)
    b1.merge = merge

    if b1.hasToMerge():
        df2 = pd.read_csv( filename2, delimiter = delimiter2 , usecols = column_indexes, 
                    names = column_names, dtype = {"isbn" : "str"},
                    quotechar = '"', encoding = encoding2, engine=engine)
        logger.doLog('read second file')
        b2 = FileInfo(filename2, df2)
        final_books = b1.anti_join(b2.df)

    else:
        final_books = b1.df

    books = FileInfo('books.csv', final_books)
    books.df.to_csv(books.name_file, quotechar='"', encoding='utf-8', index=False)
    
    logger.doLog('File csv created')

if __name__ == '__main__':
    reload(sys)
    sys.setdefaultencoding('utf8')
    #determining path of execution
    basepath = os.path.dirname(os.path.realpath(__file__))
    logger = Logger(basepath + '/mining.log')
    config = Config(basepath + '/ubook.cfg')
    main()