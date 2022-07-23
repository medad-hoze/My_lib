# -*- coding: utf-8 -*-
import matplotlib.pyplot as plt
import geopandas as gpd
import pandas as pd
from difflib import SequenceMatcher
import numpy as np
from shapely.geometry import MultiPoint ,MultiLineString, MultiPolygon ,Point , mapping,Polygon ,LineString
import random
from shapely.ops import transform
from shapely.wkt import loads

# # # # # General fun # # # # #
#delete_Duplicates

def delete_Duplicates(path):

    gdf = gpd.read_file(path)

    gdf['WKT'] = gdf['geometry'].apply(lambda x: str(x))

    grouped_gdf = gdf.groupby('WKT').mean().reset_index()


    result_gdf  = grouped_gdf[grouped_gdf.columns.tolist()]

    result_gdf['geometry'] = result_gdf['WKT'].apply(lambda x: loads(x))
    result_gdf = gpd.GeoDataFrame(result_gdf)

class gpd_Manager():

    # plot
    # data
    # plot_multi_layers
    # intersect
    # Get_centroid
    # Create_buffer
    # Split_layer
    # CSV_To_Point
    # Connect_by_name(self,layer2,field_name,Out_put)
    # removes_holes
    # multi_to_single
    # Move_Vrtx_Randomly
    
    def __init__(self,path):
        if path.endswith('.shp'):
            self.path = path
            self.layer = gpd.read_file(path)
            self.crs   = gpd.read_file(path).crs
        else:
            self.path = path
        
    def plot(self,column = ""):
        print (self.layer.plot(figsize = (10,10)))
        
    def data(self):
        print (self.layer.columns)
        print (self.layer.head())
        
    def plot_multi_layers(self,list_of_layers):
        new_list = [gpd.read_file(i) for i in list_of_layers]
        new_list = new_list + [self.layer]
        fig,ax = plt.subplots(1)
        for i in new_list:
            i.plot(ax=ax, cmap = 'rainbow')
            
    def intersect(self,ref_layer):
        ref_lyr  = gpd.read_file(ref_layer,crs = self.crs)
        if ref_lyr.crs == self.layer.crs:
            inter    = ArithmeticError
            
            
            gpd.overlay(self.layer,ref_lyr, how = 'intersection')
            inter.plot(figsize = (10,10), cmap = 'jet')

        else:
            print ("Layers dosent have the same crs")
            
    def Get_centroid(self,out_put):
        centroid= self.layer.centroid
        fig,ax = plt.subplots(1)
        self.layer.plot(ax = ax)
        centroid.plot(ax = ax, cmap = 'rainbow')
        centroid = gpd.GeoDataFrame(centroid,crs = self.crs)
        centroid = centroid.rename(columns={0:'geometry'}).set_geometry('geometry')
        centroid = gpd.sjoin(centroid, self.layer, op="within")
        centroid.to_file(out_put)
        return centroid
    
    def Create_buffer(self,out_put,num = 1):
        poly = self.layer.copy()
        poly["geometry"] = self.layer.geometry.buffer(num)
        poly.to_file(out_put)
        fig,ax = plt.subplots(1)
        poly.plot(ax = ax)
        self.layer.plot(ax = ax, cmap = 'rainbow')

    def Split_layer(self,Out_put,Field_value = "OBJECTID"):
        list_layers = [self.layer[self.layer["OBJECTID"] == i] for  i in range(self.layer.shape[0])]
        for layer_country in list_layers:
            li_name = layer_country[Field_value].values
            name    = ''.join([i for i in li_name if i not in ['@','.','%','^','&','*','!',' ','-','#','(',')','[',']','{','}']])
            if li_name:
                shp_path = Out_put + '\\' + name + '.shp'
                layer_country.to_file(shp_path)

    def SequenceMatcher_To_Table(self,layer_f,table_f,index_limit = 0):

        def similar(a, b):
            return SequenceMatcher(None, a, b).ratio()

        list1   = [i.upper() for i in layer_f if i[0] != None]
        ref     = [i.upper() for i in table_f if i[0] != None]

        all_list   = [[i,similar(i, n),n] for n in ref for i in list1]
        df         = pd.DataFrame(all_list,columns= ['KEY','NUM','KEY2'])
        df["RANK"] = df.groupby  ('KEY')['NUM'].rank(method='first',ascending=False)
        df2        = df[df["RANK"] == 1]
        data_t_gis = {getattr(row, "KEY"): getattr(row, "KEY2") for row in df2.itertuples(index=True, name='Pandas') if getattr(row, "NUM") > index_limit}

        return data_t_gis
        
    def CSV_To_Point(self,shp_out_put,X ='X',Y='Y',crs_num = '2039'):

        df      = pd.read_csv(self.path)
        table_f = df.columns
        layer_f = [X,Y]
        li_sq   = self.SequenceMatcher_To_Table(layer_f,table_f,index_limit = 0)

        points = df.apply(lambda row: Point(row[li_sq[X]],row[li_sq[Y]]),axis = 1)
        Fin_point     = gpd.GeoDataFrame(df,geometry = points)
        Fin_point.crs = {'init':'epsg:'+crs_num}
        Fin_point.to_file(shp_out_put)


    def Connect_by_name(self,layer2,field_name,Out_put):
        def Get_Point(Orig,New,key):
            if key != None:
                return New
            else:
                return Orig
        layer2 = gpd.read_file(layer2)
        layer3 = pd.merge(self.layer,layer2,on = field_name,how='left',sort=False)
        layer3['geometry'] = layer3.apply(lambda row: Get_Point(row['geometry_x'],row['geometry_y'],row["COUNTRY"]), axis=1)
        Fin_df = gpd.GeoDataFrame(layer3,geometry = layer3['geometry'])
        Fin_df['wkt']= Fin_df['geometry'].map(str)
        Fin_df=Fin_df.sort_values('wkt', ascending=False).drop_duplicates('wkt').sort_index()
        Fin_df[['geometry','COUNTRY']].to_file(Out_put)
        delete_Duplicates(Out_put)

    def removes_holes(self,Out_put,area):

        def removes_holes_from_multipolygon_by_area(poly , area):
            return MultiPolygon(list(map(lambda p: removes_holes_from_polygon_by_area(p, area), list(poly))))
            
        def removes_holes_from_polygon_by_area(poly , area):
            coords = [];list_hole =[]
            coords = tuple(poly.exterior.coords)
            for hole in poly.interiors:
                if Polygon(hole).area> area :
                    list_hole.append(tuple(hole.coords))
            return Polygon(coords,list_hole)
            
        def removes_holes_from_by_area(poly , area):
            if 'Polygon' == poly.geom_type :
                poly = removes_holes_from_polygon_by_area(poly , area)

            elif 'MultiPolygon' == poly.geom_type:
                
                poly = removes_holes_from_multipolygon_by_area(poly , area)
            else:
                pass
            return poly


        gdf2 = self.layer[:]
        gdf2['geometry'] = self.layer['geometry'].apply(lambda poly: removes_holes_from_by_area(poly , area))
        gdf2.to_file(Out_put)


    def multi_to_single(self,Out_put):
        gpdf_singlepoly = self.layer[self.layer.geometry.type == 'Polygon']
        gpdf_multipoly  = self.layer[self.layer.geometry.type == 'MultiPolygon']
        for i, row in gpdf_multipoly.iterrows():
            Series_geometries = pd.Series(row.geometry)
            df = pd.concat([gpd.GeoDataFrame(row, crs=gpdf_multipoly.crs).T]*len(Series_geometries), ignore_index=True)
            df['geometry']  = Series_geometries
            gpdf_singlepoly = pd.concat([gpdf_singlepoly, df])
        gpdf_singlepoly.reset_index(inplace=True, drop=True)
        gpdf_singlepoly.to_file(Out_put)

    def Move_Vrtx_Randomly(self,Out_put,X_move = 5,Y_move = 5):
        gdf = self.layer[:]
        gdf.to_crs(self.crs)
        gdf['geometry'] =  self.layer['geometry'].apply(lambda poly: transform(lambda y, x: (y + random.uniform(1, Y_move),x +random.uniform(1, X_move)), poly))
        gdf.to_file(Out_put)



