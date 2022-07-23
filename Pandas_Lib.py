# -*- coding: utf-8 -*-

import pandas as pd
import numpy  as np
import matplotlib.pyplot as plt

import osr,ogr
from sqlalchemy import create_engine
### Class ###


# Layer_Engine

# Create_CSV
# drop_fields
# drop_nulls
# Sum
# Get_min_max_ofGroup
# Count_field
# Len_field
# Filter_df
# Groupby_and_count
# Dict
# del_null
# del_if_in
# Group_and_Rank
# coord_from_WGS84_to_IsraelUTM
# sql_sentence
# Check_Corr
# Replace
# Outo_Replace
# Outo_Corr
# Fix_date_foramt
# read_excel_sheets
# rank_And_Index

class Layer_Engine():

    def __init__(self,data,columns):

        self.data            = data
        self.len_columns     = len(columns)
        self.df              = pd.DataFrame(data = self.data, columns = columns)
        self.len_rows        = self.df.shape[0]
        self.columns         = columns

        self.df_count = False
        

    def Create_CSV(self,csv_name,set_index = ''):
        if set_index:
            df2 = self.df.set_index(set_index)
            return df2.to_csv(csv_name)
        self.df.to_csv(csv_name)


    def drop_fields(self,fields_list):
        self.df.drop(columns = fields_list, axis=1)

    def drop_nulls(self,field):
        self.df = self.df.dropna(how = 'any',subset=[field])

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

    def del_if_in(self,field,df_or_list,reverse = False):
        ''' df_or_list = result['index1']'''
        if reverse:
            self.df = self.df.loc[~self.df[field].isin(df_or_list)]

    def Group_and_Rank(self,GroupField,RankField,first_rank = True,Update_df = False):
        df2 = self.df.copy()
        df2["RANK"] = self.df.groupby(GroupField)[RankField].rank(method='first',ascending=False)
        if first_rank:
            df2     = self.df[self.df['RANK'] == 1]
        if Update_df:
            self.df = df2

    def coord_from_WGS84_to_IsraelUTM(self,field_X,field_Y):
        self.df['X_Y']  = self.df.apply(lambda row: get_proj_osr(row[field_X] , row[field_Y]), axis=1)

    def sql_sentence(self,sql_query,tabel_name):
        '''
        tabel_name = table
        sql_query  = SELECT DISTINCT gender from table;
        '''
        engine = create_engine('sqlite://'  ,  echo = False) 
        self.df.to_sql     (tabel_name, con = engine)
        return engine.execute(sql_query).fetchall()


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


def rank_And_Index(df,field_to_groupby):

    '''
    [INFO] - Create a column based on a colum u want to groupby and his place in the data

    [Input]  - field
    [Output] - field_GroupMe

    [Example] - 

             field       field_GroupMe

             a       -       a-1
             a       -       a-1
             b       -       b-2
             a       -       a-3
    '''

    num = 0

    def find_when_change(row):
        nonlocal num
        if row['new']:
            num += 1
            return num
        return num

    df['new']     = df[field_to_groupby].shift() != df[field_to_groupby]
    df['New_num'] = df.apply(find_when_change,axis = 1)

    y = np.where(df[field_to_groupby],df[field_to_groupby] +'-' + df['New_num'].astype('str'),df[field_to_groupby])

    key_field     = field_to_groupby +'_'+'GroupMe'
    df[key_field] = y

    df = df.drop(['new','New_num'],axis = 1)

    return df


#####  Func   ####




def get_proj_osr(pointX,pointY):
    inputEPSG = 4326
    outputEPSG = 2039
    
    # create a geometry from coordinates
    point = ogr.Geometry(ogr.wkbPoint)
    point.AddPoint(pointX, pointY)
    
    # create coordinate transformation
    inSpatialRef = osr.SpatialReference()
    inSpatialRef.ImportFromEPSG(inputEPSG)
    
    outSpatialRef = osr.SpatialReference()
    outSpatialRef.ImportFromEPSG(outputEPSG)
    
    coordTransform = osr.CoordinateTransformation(inSpatialRef, outSpatialRef)
    
    # transform point
    point.Transform(coordTransform)
    
    # print point in EPSG 4326
    return str(point.GetX()) +'-'+ str(point.GetY())


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
