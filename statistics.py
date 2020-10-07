import pandas as pd

notapplicables = "N/A|NA|Not applicable|#NA"

def BasicStats(dataframe):
    statlist = []
    cols = dataframe.columns.tolist()
    for i in range(0, len(dataframe.columns)):
        if dataframe.iloc[:, i].dtypes == 'float' or dataframe.iloc[:,i].dtypes == 'int' or dataframe.iloc[:,i].dtypes == 'int64' or dataframe.iloc[:,i].dtypes == 'float64' or dataframe.iloc[:,i].dtypes == 'uint8':
            statlist.append([cols[i], dataframe.iloc[:, i].dtypes,
                             format(dataframe.iloc[:,i].min(), '4f'),
                             format(dataframe.iloc[:,i].max(), '4f'),
                             format(dataframe.iloc[:,i].max() - dataframe.iloc[:,i].min(), '4f'),
                             format(dataframe.iloc[:,i].mean(), '4f'),
                             format(dataframe.iloc[:,i].std(), '4f'),
                             int(dataframe.iloc[:,i].count()),
                             format(dataframe.iloc[:,i].sum(), '4f'),
                             int(dataframe.iloc[:,i].nunique()),
                             '',
                             int(dataframe.iloc[:,i].isnull().sum()),
                             ''])
        elif dataframe.iloc[:, i].dtypes == 'object':
            if int(dataframe.iloc[:,i].nunique()) > 15:
                uniquelist = 'too many'
            else:
                uniquelist = set(dataframe.iloc[:,i])
            statlist.append([cols[i],'string',
                             '' ,'' ,'' ,'' ,'' ,
                             int(dataframe.iloc[:,i].count()),
                             '',
                             int(dataframe.iloc[:,i].nunique()),
                             uniquelist,
                             # set(dataframe.iloc[:,i]),
                             int(dataframe.iloc[:,i].isnull().sum()),
                             dataframe.iloc[:,i].str.contains(notapplicables, na=True).sum()])
        elif dataframe.iloc[:, i].dtypes == 'datetime64[ns]':
            statlist.append([cols[i], 'date',
                             '', '', '', '', '',
                             int(dataframe.iloc[:, i].count()),
                             '',
                             int(dataframe.iloc[:, i].nunique()),
                             '',
                             int(dataframe.iloc[:, i].isnull().sum()),
                             ''])
        elif dataframe.iloc[:,i].dtypes == 'bool':
            statlist.append([cols[i],'boolean',
                             '', '', '', '', '',
                             int(dataframe.iloc[:,i].count()),
                             '',
                             int(dataframe.iloc[:,i].nunique()),
                             '',
                             int(dataframe.iloc[:,i].isnull().sum()),
                             ''])
        else:
            statlist.append([cols[i],'unknown',
                             '', '', '', '', '',
                             int(dataframe.iloc[:,i].count()),
                             '',
                             int(dataframe.iloc[:,i].nunique()),
                             '',
                             int(dataframe.iloc[:,i].isnull().sum()),
                             ''])

    columns = (['column',
                'type',
                'min',
                'max',
                'range',
                'mean',
                'stdev',
                'populated',
                'sum',
                'unique',
                'uniquelist',
                'nulls',
                'not applicable'])

    statframe = pd.DataFrame(statlist, columns=columns)
    return statframe

