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
    filename = info['file']
    filepath = basepath + '/files/' + info['file']
    delimiter = info['delimiter'].replace('"', '')
    encoding = info['encoding']
    engine = info['engine']
    merge = bool(info['flag_merge'])
    modify = bool(info['flag_modify'])

    if not merge and not modify:
        logger.doLog('No action requested')
        sys.exit(1)

    info2 = config.sections['MERGEFILE']
    filename2 = info2['file']
    filepath2 = basepath + '/files/' + info2['file']
    delimiter2 = info2['delimiter'].replace('"', '')
    encoding2 = info2['encoding']
    engine = info2['engine']
    on_column = info2['on']

    columns = config.sections['COLUMNSGENERAL']
    arr_columns = createArray(columns)
    column_names = arr_columns[1]

    # Read file csv
    df = pd.read_csv( filepath, delimiter = delimiter , usecols = column_names, 
                     dtype = str, quotechar = '"', encoding = encoding, engine=engine)
    b1 = FileInfo(filename, df)
    logger.doLog('Read first file ' + b1.getName())

    b1.merge = merge
    b1.modify = modify

    if b1.hasToMerge():
        df2 = pd.read_csv(filepath2, delimiter = delimiter2 , usecols = [on_column], 
                          dtype = {'isbn' : str}, quotechar = '"', encoding = encoding2, engine=engine)
        b2 = FileInfo(filename2, df2)
        logger.doLog('Read second file ' + b2.getName())
        "Return rows in left df which are not present in second df"
        merged_books = b1.anti_join(b2.df)
        logger.doLog('Files ' + b1.getName() + ' and ' + b2.getName() + ' merged')
    else:
        merged_books = b1.df

    m = FileInfo('final', merged_books)

    if b1.hasToModify():
        modnames = config.sections['MODIFIERSNAMES']
        m.df = m.df.rename(columns = modnames)
        modval = config.sections['MODIFIERSVALUES']
        merged_books['genre'] = m.replaceValues('genre', modval)
        merged_books['category'] = m.replaceValues('category', modval)
        merged_books['price'] = [val.replace(',', '.') for val in merged_books['price']]
        if config.sections['DROPCOLUMNS']:
            drop_arr = createArray(config.sections['DROPCOLUMNS'])
            m.df = m.df.drop(columns = drop_arr[1])
            logger.doLog('Dropped columns')

        logger.doLog('File modified')

    final_books = merged_books

    books = FileInfo('files/books.csv', final_books)
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