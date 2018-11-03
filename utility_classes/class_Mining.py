
import collections as coll 
import pandas as pd

class FileInfo(object):

    def __init__(self, name_file, df, modify=False, merge=False):
        self.name_file = name_file
        self.df = df
        self.modify = modify
        self.merge = merge

    def getName(self):
        return self.name_file

    def getDF(self):
        return self.df

    def hasToMerge(self):
        if self.merge:
            return True
        else:
            return False

    def hasToModify(self):
        if self.modify:
            return True
        else:
            return False


    def replaceOne(self, column, strout, strin):
        column = self.df[column].replace(strout, strin)
        return column

    def replaceAll(self, ele, dic):
        ele = self.df[ele]
        for i, j in dic.iteritems():
            ele = ele.replace(i, j)
        return ele

    "Return rows in x which are not present in y"
    def anti_join(self, y, on = "isbn"):
        x = self.df
        ans = pd.merge(left=x, right=y, how='left', indicator=True, on=on)
        ans = ans.loc[ans._merge == 'left_only', :].drop(columns='_merge')
        ans.columns = [col.replace('_x', '') for col in ans.columns]
        columns = [col for col in ans.columns if '_y' not in col]
        ans = ans[columns]
        return ans


## Functions to modify elements##
def createArray(dict_values):
    d = {int(k):v for k,v in dict_values.items()}
    od = coll.OrderedDict(sorted(d.items()))
    arr1 = []
    arr2 = []
    arr = [arr1, arr2]
    for key,val in od.iteritems():
        arr1.append(int(key))
        arr2.append(val)
    return arr

    