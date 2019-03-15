import sys, os, inspect
sys.path.append('utility_classes')
import pandas as pd
import collections as coll 
from importlib import reload #used only in Python 3.*
from class_logger import objLogger as Logger
from class_config import objConfig as Config
from class_Mining import FileInfo
from class_Mining import createArray


reload(sys)

#Uncomment the line above if you use Python 2.* 
# sys.setdefaultencoding('utf8')

#determining path of execution
basepath = os.path.dirname(os.path.realpath(__file__))
config = Config(basepath + '/project.cfg')
info = config.sections['GENERAL']
stat = config.sections['SUMMARY']

resultfilepath = basepath + '/statistics/' + stat['results_file']

filename = info['file']
filepath = basepath + '/files/' + info['file']
delimiter = info['delimiter'].replace('"', '')
encoding = info['encoding']
engine = info['engine']

columns = config.sections['COLUMNSGENERAL']
arr_columns = createArray(columns)
column_names = arr_columns[1]

# Read file csv
df = pd.read_csv( filepath, delimiter = delimiter , usecols = column_names, 
                    dtype = str, quotechar = '"', encoding = encoding, engine=engine)
b1 = FileInfo(filename, df)

count = df.count()
first_elements = df.head(5)
cols = config.sections['STATISTICSCOLS']
for col in cols:
    column = cols[col]
    data_col = df.column.unique()

descr = df.describe()
shape = [df.shape[0], df.shape[1]]
elements = ['Size of dataset (nrow, ncols) ', shape,'. Unique values for the columns chosen in .cfg file']



ele_df=pd.DataFrame(data=first_elements)
descr_df=pd.DataFrame(data=descr)

with open(resultfilepath,'w') as fd:
    fd.write('First 5 rows of dataset\n')
ele_df.to_csv(resultfilepath, mode='a')
with open(resultfilepath,'a') as fd:
    fd.write('\nBasic statistics to describe file columns\n')
descr_df.to_csv(resultfilepath, mode='a')

with open(resultfilepath, 'a') as filehandle:  
    for listitem in elements:
        filehandle.write('\n%s\n' % listitem)


