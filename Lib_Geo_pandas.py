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
import os
from itertools import product, groupby

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
    # findRasters
    # findLayerSHP
    # Connect_Lines
    # conectCloseNodesByLine
    # renameByConnectivity
    
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



# Find Rasters


def convert_bytes(num):
    for x in ['bytes','KB','MB','GB','TB']:
        if num < 1024.0:
            return "%3.lf %s" % (num,x)
        num /= 1024.0

				
def file_size(file_path):
    if os.path.exists(file_path):
        if os.path.isfile(file_path):
            file_info = os.stat(file_path)
            return convert_bytes(file_info.st_size)
    else:
        return 'no file found'


def Run_over_Folders(folder,typeSearch = '.tif'):
    img = [root +'\\' + file for root, dirs, files in os.walk(folder)\
           for file in files if file.endswith(typeSearch)]
    return img


def Count_copys(path):

    poly  = gpd.read_file(path)
    count_data = poly.groupby('name').size()
    count_data = count_data.to_frame().reset_index()
    count_data = poly.merge(count_data, on='name').reset_index()
    count_data = count_data.rename(columns={0: 'Num'})
    count_data.to_file(path)

class raster_manager():
    
    def __init__(self, filename):
        ds                = gdal.Open( filename )
        self.ds           = ds
        self.filename     = filename                     # שם הקובץ
        self.name         = os.path.basename(filename)          
        self.bands        = ds.RasterCount               # מספר ערוצים
        self.xsize        = ds.RasterXSize               # גודל פיקסל X
        self.ysize        = ds.RasterYSize               # גודל פיקסך Y
        self.band_type    = ds.GetRasterBand(1).DataType # סוג 
        self.projection   = ds.GetProjection()
        self.geotransform = ds.GetGeoTransform()
        self.ulx  = self.geotransform[0]                         # נקודה שמאלית עליונה X
        self.uly  = self.geotransform[3]                         # נקודה שמאלית עליונה Y
        self.lrx  = self.ulx + self.geotransform[1] * self.xsize # נקודה ימינית תחתונה X
        self.lry  = self.uly + self.geotransform[5] * self.ysize # נקודה שמאלית תחתונה Y
        self.musk = zip([self.lrx,self.ulx,self.ulx,self.lrx],[self.lry,self.lry,self.uly,self.uly])

        self.np_array     = np.array(ds.GetRasterBand(1).ReadAsArray())# Get as np array

        ct = ds.GetRasterBand(1).GetRasterColorTable()
        if ct is not None:
            self.ct = ct.Clone()
        else:
            self.ct = None


def findRasters(path,out_put):
    try:
        raster1 = raster_manager(path)
    
        polygon_geom      = Polygon(raster1.musk)
        crs               = {'init': 'epsg:2039'}
        polygon           = gpd.GeoDataFrame( crs=crs, geometry=[polygon_geom])    
        polygon["bands"]  = raster1.bands
        polygon["P_Size"] = raster1.xsize/10000
        polygon["d_type"] = raster1.band_type
        polygon["path"]   = raster1.filename
        polygon["name"]   = raster1.name
        polygon["Size"]   = file_size(path)
        try:
            polygon["CRS"] = raster1.projection[8:20]
        except:
            polygon["CRS"] = 'Unknown'
        
        if os.path.exists(out_put):
            poly  = gpd.read_file(out_put)
            merge = poly.append(polygon)
            merge.to_file(out_put)
        else:
            polygon.to_file(out_put)
    except:
        print ("Coudnt Read: {}".format(path))
        


def findLayerSHP(folder,out_put):
    list_shp = Run_over_Folders(folder,'.shp')

    for shp in list_shp:
        gdf       = gpd.read_file(shp)
        envelop   = gdf.unary_union.envelope
        
        crs                  = {'init': 'epsg:2039'}
        polygon              = gpd.GeoDataFrame( crs=crs, geometry=[envelop]) 
        polygon['dis']       = 1
        polygon              = polygon[polygon['geometry'].geom_type == 'Polygon']
        polygon              = polygon.dissolve(by='dis')
        polygon['geom_type'] = gdf['geometry'].geom_type[0]
        polygon['File_Size'] = file_size(shp)
        polygon['Full_Path'] = shp
        polygon['name']      = os.path.basename(shp).split('.')[0]

        if os.path.exists(out_put):
            poly  = gpd.read_file(out_put)
            if shp in poly['Full_Path'].tolist():
                print ('All ready exists')
                continue
            merge = poly.append(polygon)
            merge.to_file(out_put)

        else:
            polygon.to_file(out_put)



def check_if_theSame(x,y,x_mean,y_mean):
    return 1 if x == x_mean and y == y_mean else 0



def Connect_Lines(path,out_put,dis_search = 50):

    def getStart_End_XY(layer):
        new_data  = []
        geometrys = []
        # index2d   = index.Index()
        
        for index2, row in layer.iterrows():
            coords = [(coords) for coords in list(row['geometry'].coords)]
            first_last_coord = [Point(coords[0]),Point(coords[-1])]
            
            
            for i in range(len(first_last_coord)):
                key      = row['key']
                geometry = first_last_coord[i]
                if i == 0:
                    loc = 'start'
                else:
                    loc = 'end'
                
                new_data.append([key,loc])
                geometrys.append(geometry)
                
        return new_data,geometrys


    def Snap_tool(gdf_prepare,parcel_in_proc,precision):

        parcel_in_proc       = parcel_in_proc.to_crs({'init': 'epsg:2039'})
        parcel_in_proc_union = parcel_in_proc.geometry.unary_union
        gdf_prepare.geometry = gdf_prepare.geometry.apply(lambda x: snap(x, parcel_in_proc_union, precision))
        
        return gdf_prepare
    

    layer             = gpd.read_file(path)
    layer['key']       = layer.index
    layer             = layer.to_crs(epsg=2039)
    crs               = layer.crs
    
    
    new_data,geometrys = getStart_End_XY(layer)
                
    # points end and start
    start_end = gpd.GeoDataFrame(data = new_data,columns = ['key','loc'],geometry=geometrys,crs = crs)
    
    # buffer start end points, dnion them and get and key for each part
    buffer           = gpd.GeoDataFrame(data = new_data,columns = ['buffer_key','buffer_loc'],\
                                       geometry = start_end['geometry'].buffer(dis_search/2),crs = crs)
    df2              = gpd.GeoDataFrame(geometry = [geom for geom in buffer.unary_union.geoms],crs = crs)
    df2['key_diss']  = df2.index
    
    # intersect buffer-union with points to know for each point what buffer connect with
    res_intersection      = gpd.overlay(start_end,df2 , how='intersection')
    
    # get x and y for each point
    res_intersection['x'] = res_intersection.geometry.apply(lambda val: val.x)
    res_intersection['y'] = res_intersection.geometry.apply(lambda val: val.y)
    
    # get x_mean and y_mean for each buffer
    stats_pt = res_intersection.groupby('key_diss')[['x','y']].agg('mean').reset_index()
    stats_pt = stats_pt.rename({'x': 'x_mean', 'y': 'y_mean'}, axis=1)  # new method
    
    # connect between points and buffer
    points_ = res_intersection.merge(stats_pt, on='key_diss')
    
    points_['new_point'] = points_.apply(lambda j: check_if_theSame(j.x,j.y,j.x_mean,j.y_mean),axis = 1)
    points_              = points_[points_['new_point'] == 0]
    points_['geometry']  = points_.apply(lambda row: Point(row.x_mean,row.y_mean),axis = 1 )
    
    new_layer            = Snap_tool(layer,points_,50)
    
    new_layer.to_file(out_put)


def conectCloseNodesByLine(path,out_put,dis_search = 50):


    def getStart_End_XY(layer):
        new_data  = []
        geometrys = []
        # index2d   = index.Index()
        
        for index2, row in layer.iterrows():
            coords = [(coords) for coords in list(row['geometry'].coords)]
            first_last_coord = [Point(coords[0]),Point(coords[-1])]
            
            
            for i in range(len(first_last_coord)):
                key      = row['key']
                geometry = first_last_coord[i]
                if i == 0:
                    loc = 'start'
                else:
                    loc = 'end'
                
                new_data.append([key,loc])
                geometrys.append(geometry)
                
        return new_data,geometrys


    layer             = gpd.read_file(path)
    layer['key']       = layer.index
    layer             = layer.to_crs(epsg=2039)
    crs               = layer.crs



    new_data,geometrys = getStart_End_XY(layer)
                
    # points end and start
    start_end = gpd.GeoDataFrame(data = new_data,columns = ['key','loc'],geometry=geometrys,crs = crs)

    # buffer start end points, dnion them and get and key for each part
    buffer           = gpd.GeoDataFrame(data = new_data,columns = ['buffer_key','buffer_loc'],\
                                    geometry = start_end['geometry'].buffer(dis_search/2),crs = crs)
    df2              = gpd.GeoDataFrame(geometry = [geom for geom in buffer.unary_union.geoms],crs = crs)
    df2['key_diss']  = df2.index

    # intersect buffer-union with points to know for each point what buffer connect with
    res_intersection      = gpd.overlay(start_end,df2 , how='intersection')

    # get x and y for each point
    res_intersection['x'] = res_intersection.geometry.apply(lambda val: val.x)
    res_intersection['y'] = res_intersection.geometry.apply(lambda val: val.y)

    # get x_mean and y_mean for each buffer
    stats_pt = res_intersection.groupby('key_diss')[['x','y']].agg('mean').reset_index()
    stats_pt = stats_pt.rename({'x': 'x_mean', 'y': 'y_mean'}, axis=1)  # new method

    # connect between points and buffer
    points_ = res_intersection.merge(stats_pt, on='key_diss')



    points_['new_point'] = points_.apply(lambda j: check_if_theSame(j.x,j.y,j.x_mean,j.y_mean),axis = 1)

    points_              = points_[points_['new_point'] == 0]
        
    geoms                = [LineString([Point(xy) for xy in [[i[4],i[5]],[i[6],i[7]]]]) for i in points_.values.tolist()]
        
    geo_df2              = gpd.GeoDataFrame(geometry=geoms)

    geo_df2.to_file(out_put)




#  Check if roads are in the same dir and name them 


def renameByConnectivity(layer):

    def flatten_list(list_):
        #[INFO] - flatten list then get uniqes, return as list
            # Input  = [[1,2,3],[1],[5,1]]
            # output = [1,2,3,5] 
            
        return list(set(np.array(list(list_)).flatten()))

    def dict_to_Roads_connection(dict_):
        #  [INFO] - get a dict containing the distance and every 2 nodes,and check the 
        #           farest roads for each other, making  no road will be twice
            # Input  = {1.467:[1,2],2.467:[1,3],2.0:[2,4]}
            # output = [[1,3],[2,4]]
        
        all_values = flatten_list(dict_.values())
        exists     = []
        data       = []
        
        for i in all_values:
            if [k for k, v in dict_.items()]:
                roads = dict_[max(k for k, v in dict_.items())]
                dict_ = {key:val for key, val in dict_.items() if val != roads}
                if roads[0] not in flatten_list(exists) and roads[1] not in flatten_list(exists):
                    data .append(roads)
                    exists = exists + roads
                    
        return data


    def Find_Farest_distance(all_intersection,field = 'Id_Circle'):
        # [INFO] - func get dataframe and find the farest points of in a specific column, 
        #            will return a sublist in list of the 2 farest in each column group
            # input   = Dataframe, field
            # out put = [[3,2],[6,7],[7,5],[1,4]]    (every value can be found 1 time, 
            #           and represent the farest disntace in each intersection)
        
        data = []
        cir  = list(all_intersection[field].unique())
        for i in cir:
            current = list(all_intersection[all_intersection[field]== i].values.tolist())
            dic_    = {}
            for n in current:
                for j in current:
                    dis =  n[2].distance(j[2])
                    if dis > 0:        
                        dic_[dis] = [n[0],j[0]]
                        
            if dic_:
                connections = dict_to_Roads_connection(dic_)
                data += connections
            
        return data     


    def Connect_lists_by_same_value(data):
        # [INFO] - connect all sublists that contain a shared value
            #   input  = [[1,2],[3,4],[1,4],[5]]
            #   output = [[1,2,3,4],[5]]

        l=[set(x) for x in data]
        for a,b in product(l,l):
            if a.intersection(b):
                a.update(b)
                b.update(a)
        
        l = sorted( [sorted(list(x)) for x in l])
        return list(l for l,_ in groupby(l))


    def connect(x,final_list):
        # [INFO] - Run over the id column of each part and check if he's in the same 
        #          road group, every group will get the number of the group index.
            #   input  = 39, [[1,23,31],[39,3,4]]
            #   output = 1
        
        exe = [final_list.index(i) for i in final_list if x in i]
        if exe: return exe[0] + 1  

    # [INFO] - func clustring continuous polylines, combine strets\roads with
    #           with the must liklyhood of continuous
        # input  = polyline as shp
        # output = polyline as shp with connected parts.
    
    layer              = layer.to_crs(epsg=2039)
    crs                = layer.crs
    layer['Id']        = layer.index
    
    # get the intersections between 2 lines
    g                = [i for i in layer.geometry]
    points           = [np.dstack(pts.xy)[0][0].tolist() for i in g for pts in i.boundary]
    points_intersecs = list({str(points[i]): Point(points[i]) for i in range(len(points))\
                       if points.count(points[i]) > 1}.values())
        
    # Create a Circle around the intersects, and convert him to line
    Fin_point          = gpd.GeoDataFrame(geometry = points_intersecs,crs = crs)
    circle             = Fin_point.copy().to_crs(crs= crs)
    circle["geometry"] = Fin_point.geometry.buffer(1).boundary
    circle['Id']       = circle.index
    
    # get points between the line_buffer to the road
    columns_data = [(r.Id,t.Id,r.geometry.intersection(t.geometry)) for r in \
                    layer.itertuples() for t in circle.itertuples()]
    
    # create gdf from the point layer and delete all empty rows from new road-circle intersection
    all_intersection = gpd.GeoDataFrame(data = columns_data,columns = ['Id_line','Id_Circle','geometry'])
    all_intersection = all_intersection[~all_intersection.is_empty]
    
    # check the distance between each nodes  
    data       = Find_Farest_distance(all_intersection,field = 'Id_Circle')
    
    # return the id of the roads that have the must distance between them
    final_list = Connect_lists_by_same_value(data)
    
    # return the data of each road to what group it belongs
    layer['name'] = layer['Id'].apply(lambda x:connect(x,final_list))
    
    layer = layer.drop(columns='Id')
    
    return layer
