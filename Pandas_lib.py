# -*- coding: utf-8 -*-

import pandas as pd
import numpy  as np
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
# import sqlite3

# Create_CSV
# drop_fields
# Sum
# Get_min_max_ofGroup
# Count_field
# Len_field
# Filter_df
# Groupby_and_count
# Dict
# del_null
# map_Index_null
# map_Columns_null
# interpolate_null
# drop_null
# del_if_in
# Group_and_Rank
# return1_if_in_list
# Check_Corr
# Replace
# Outo_Replace
# Outo_Date
# Outo_Corr
# Fix_date_foramt
# sql_sentence
# Time_Month_Count
# Time_Delta
# read_excel_sheets
# Replace_Cha_to_num
# convert_datatype

class Layer_Engine():

    def __init__(self,csv,multi_sheets = False):
        
        numerics = ['int16', 'int32', 'int64', 'float16', 'float32', 'float64']
        
        self.df              = pd.read_csv  (csv)
        if multi_sheets:
            self.df = read_excel_sheets(csv)
            
        self.len_rows        = self.df.shape[0]
        self.columns         = self.df.columns
        self.df_count        = False
        self.numaric_df      = self.df.select_dtypes(include=numerics)
        self.numaric_columns = self.numaric_df.columns
        

    def Create_CSV(self,csv_name,set_index = ''):
        if set_index:
            df2 = self.df.set_index(set_index)
            return df2.to_csv(csv_name)
        self.df.to_csv(csv_name)


    def drop_fields(self,fields_list):
        self.df.drop(columns = fields_list, axis=1)


    def Sum(self,field):
        return self.df[field].sum()
    
    def Get_min_max_ofGroup(self,GroupingField,SearcField):
        gb_obj            = self.df.groupby (by = GroupingField)
        df_min            = gb_obj.agg     ({SearcField : np.min})
        df_max            = gb_obj.agg     ({SearcField : np.max})
        df_edge           = pd.concat      ([df_min,df_max])
        df2               = pd.merge       (self.df,df_edge, how='inner', on='index1')
        return df2


    def Count_field(self,field):
        self.df['count'] = self.df.groupby(field)[field].transform('count')


    def Len_field(self,field,as_int = False):

        if as_int:
            len_field = self.df[field].apply(str).apply(len).astype(int)
            if len_field.shape[0] > 1:
                len_field = len_field[0]
            return int(len_field)
        else:
            self.df[field + '_len'] = self.df[field].apply(len)

    def Filter_df(self,field,Value,Update_df = False):
        if Update_df:
            self.df = self.df[self.df[field] == Value]
        else:
            df_filter = self.df[self.df[field] == Value]
            return df_filter


    def Groupby_and_count(self,field,name_field_count = ''):

        if name_field_count == '':
            name_field_count = str(field) + "_num"
        count_data    = self.df.groupby(field).size()
        count_data    = count_data.to_frame().reset_index()
        self.df_count = count_data


    def Dict(self,index_key):

        dict_  = self.df.set_index(index_key)
        dict_2 = dict_.T.to_dict()
        return dict_2

    def del_null(self,field):
        self.df = self.df[self.df[field].isnull()]
        
    
    def map_Index_null(self,value = ''):
        index_null = self.df.isnull().sum(axis=1)
        plt.hist(index_null)
        plt.show()
        if value != '':
            self.df = self.df[index_null.sort_values(ascending = False) <= float(value)]
            index_null = self.df.isnull().sum(axis=1)
            plt.hist(index_null)
            plt.show()

    def map_Columns_null(self):
        # columns =  result.isnull().sum()[result.isnull().sum() > 0]
        null_columns = self.df.columns[self.df.isnull().any()]
        columns      = self.df[null_columns].isnull().sum()
        return columns
        
    def interpolate_null(self,fields_list):
        self.df = self.df.sort_values(fields_list).interpolate()
        
    def drop_null(self,field):
        self.df = self.df.dropna(how = 'any',subset=[field])

    def del_if_in(self,field,df_or_list,reverse = False):
        ''' df_or_list = result['index1']'''
        if reverse:
            self.df = self.df.loc[~self.df[field].isin(df_or_list)]

    def Group_and_Rank(self,GroupField,RankField,first_rank = True,Update_df = False):
        df2 = self.df.copy()
        df2["RANK"] = self.df.groupby(GroupField)[RankField].rank(method='first',ascending=False)
        if first_rank:
            df2     = self.df[self.df['RANK'] == 1]
            return df2
        if Update_df:
            self.df = df2


    def return1_if_in_list(self,new_field,check_field,listValues):
        self.df[new_field] = self.df.apply(lambda row: check_syn(row[check_field],listValues), axis=1)
        
  
    def Check_Corr(self):

        plt.matshow(self.df.corr(),cmap = 'summer')
        plt.colorbar()
        plt.xticks(list(range(len(self.numaric_columns))),self.numaric_df)
        plt.yticks(list(range(len(self.numaric_columns))),self.numaric_df)
        plt.show()

    def Replace(self,field, replace_dict):
        '''replace_dict = {'m': 0, 'w': 1} '''
        self.df[field] = self.df[field].replace(replace_dict)

        
    def Outo_Replace(self):
        for i in self.columns:
            dic = {}
            uni = self.df[i].unique()
            if len(uni) == 2:
                dic[uni[0]] = 0
                dic[uni[1]] = 1
                self.df[i]  = self.df[i].replace(dic)
                self.df[i]  = pd.to_numeric(self.df[i])
                print ("On column: {}".format(i))
                print ("Convert: {}".format(dic))
                
    def Outo_Date(self):
        for i in self.columns:
            if self.df[i].astype(str).str.contains('/').all():
                print (i)
                self.df['DATE']    = pd.to_datetime(self.df[i])
                self.df['month']   = self.df['DATE'].dt.month
                self.df['year']    = self.df['DATE'].dt.year
                self.df['quarter'] = self.df['DATE'].dt.quarter
                self.df['weekday'] = self.df['DATE'].dt.weekday
                
                
    def Outo_Corr(self,Y_Field,value = 0.6):
        columns_low_corr = [k for k,v in dict(self.df.corr()[Y_Field].\
                           sort_values(ascending = False)[1:]).items()if v > value]
        
        return columns_low_corr

    def Fix_date_foramt(self,date_field,year = ''):

        '''
        fix dates if there are mix types like: אוג-26  and 26/8/2019
        you need to pass year or it will take the first year it see and brodcast
        '''

        def dict_():
            nam   = ['ינו','נוב','פבר','מרץ','אפר','מאי','יונ','יול','אוג','ספט','אוק','נוב','דצמ']
            num   = [1,2,3,4,5,5,6,7,8,9,10,11,12]
            Dict_ = dict(zip(nam,num))
            return Dict_

        def Convert_Wrong_Dates(value,year,Dict_):
            # convert bad dates as: אוק-21 to 21/10/2019
            if '-' in str(value):
                key   = value.split('-')[1]
                if Dict_.get(key):
                    month = Dict_[key]
                    day   = str(value.split('-')[0])
                    value = day + '/' + str(month) + '/' + str(year)   
            return value

        if year == '':
            year = str(self.df[self.df[date_field].astype(str).str.contains("/")][date_field].iloc[0]).split('/')[2]

        self.df[date_field] = self.df[date_field].apply(lambda x: Convert_Wrong_Dates(x,year,dict_()))
        
                
    def sql_sentence(self,sql_query,tabel_name):
        '''
        tabel_name = table
        sql_query  = SELECT DISTINCT gender from table;
        '''
        engine = create_engine('sqlite://'  ,  echo = False) 
        self.df.to_sql     (tabel_name, con = engine)
        return engine.execute(sql_query).fetchall()
    
    def Time_Month_Count(self):
        df_layer.df.month.value_counts().sort_index().plot()
        
    def Time_Delta(self):
        return (self.df['DATE'].max() - self.df['DATE'].min()).days

    def Replace_Cha_to_num(self,field):
        def Change_data(x):
            x = str(x)
            li = []
            for i,v in enumerate(x):
                if v.isnumeric() or v == '.':
                    li.append(v)

            str2 = ''.join(li)
            if not str2:
                str2 = None
            return str2

        self.df[field] = self.df[field].apply(lambda x: Change_data(x))

    def convert_datatype(self):
        self.df = self.df.convert_dtypes()



def check_syn(value,list_check):
    '''
    list_check = ['חבד','חב"ד','בי"כ','בני ברק','חסידי','בית מדרש',"בית כנסת",'תורה','ישיבה','בית הכנסת','ישיבת']
    '''
    a = [i for i in list_check if value if i in value]
    return 1 if a else 0


def read_excel_sheets(path2):
    x1 = pd.ExcelFile(path2)
    df = pd.DataFrame()
    columns = None
    for idx,name in enumerate(x1.sheet_names):
        try:
            sheet = x1.parse(name)
            if idx == 0:
                columns = sheet.columns
            sheet.columns = columns
        except:
            print ("coudent read sheet {}".format(name))
        df = df.append(sheet,ignore_index = True)
            
    return df

# path = r"C:\Users\Administrator\Desktop\CoronaProj\data.csv"

path = r"C:\Users\Administrator\Desktop\CoronaProj\data_test.csv"
df_layer = Layer_Engine   (path)
df_layer.Outo_Replace     ()
df_layer.Outo_Date        ()
df_layer.Check_Corr       ()
df_layer.Time_Month_Count ()
df_layer.Time_Delta       ()
