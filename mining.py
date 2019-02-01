import sys, os, inspect
sys.path.append('utility_classes')
import pandas as pd
import collections as coll 
from importlib import reload #used only in Python 3.*
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
    file_info = FileInfo(filename, df)
    logger.doLog('Read first file ' + file_info.getName())

    file_info.merge = merge
    file_info.modify = modify

    if file_info.hasToMerge():
        df2 = pd.read_csv(filepath2, delimiter = delimiter2 , usecols = [on_column], 
                          dtype = {'isbn' : str}, quotechar = '"', encoding = encoding2, engine=engine)
        data_info = FileInfo(filename2, df2)
        logger.doLog('Read second file ' + data_info.getName())
        "Return rows in left df which are not present in second df"
        data_merged = file_info.anti_join(data_info.df)
        logger.doLog('Files ' + file_info.getName() + ' and ' + data_info.getName() + ' merged')
    else:
        data_merged = file_info.df

    m = FileInfo('final', data_merged)

    if file_info.hasToModify():
        modnames = config.sections['MODIFIERSNAMES']
        m.df = m.df.rename(columns = modnames)
        modval = config.sections['MODIFIERSVALUES']
        modcols = config.sections['MODIFIERSCOLS']
        for col in modcols:
            columns = modcols[col]
            data_merged[columns] = m.replaceValues(columns, modval)
        
        if config.sections['DROPCOLUMNS']:
            drop_arr = createArray(config.sections['DROPCOLUMNS'])
            m.df = m.df.drop(columns = drop_arr[1])
            logger.doLog('Dropped columns')

        logger.doLog('File modified')

    final_data = data_merged

    data = FileInfo('files/data.csv', final_data)
    data.df.to_csv(data.name_file, quotechar='"', encoding='utf-8', index=False)
    
    logger.doLog('File csv created')

if __name__ == '__main__':
    reload(sys)

    #Uncomment the line above if you use Python 2.* 
    # sys.setdefaultencoding('utf8')

    #determining path of execution
    basepath = os.path.dirname(os.path.realpath(__file__))
    logger = Logger(basepath + '/mining.log')
    config = Config(basepath + '/project.cfg')
    main()