
# -*- coding: utf-8 -*-
from difflib import SequenceMatcher
from shutil import copyfile
from osgeo import ogr
import pandas as pd
import os,gc,osr,sys
from osgeo import gdalconst
import matplotlib as plt
from osgeo import gdalnumeric
import subprocess


from osgeo import gdal
import numpy as np


'''
version: 2
date: 23/07/2022
'''

     #### SUM ####
##############################

# folder_manager() 
          
#   folder_info
#   folder_content
#   mosaic

##############################

# raster_manager ()
          
    # - raster_data
    # - polygonize
    # - replace_value
    # - Cut_raster_to_pices
    # - extract xyz from DEM
    # - clean_empty_img
    # - visualize
    # - Pyramid
    # - resampl_img
    # - Checking_Pix_count
    # - avr_bands(in_ds)
    # - RasterRize_gdal
    # - del_zero_value_in_raster

##############################

#layer_manager()
          
#   Get_fields_name
#   Get_Atrributes
#   Get_Numaric_Atrributes
#   Get_layer_Count
#   Get_Fields_max
#   Get_Fields_min
#   Get_field_count
#   Get_layers_in_folder
#   get_unique
#   get_geom
#   Get_Extent
#   buffer
#   Line_to_point
#   poly_to_line
#   poly_to_point
#   delete_layer
#   createFolder
#   get_unique
#   Poly_To_Centroid
#   Select_layer_by_atrributes
#   Dissolve_Polygons
#   Buffer_in_and_out
#   RasterRize
#   Clean_Vrtx
#   Intersect_poly_point
#   layerToDataFrame
#   layerToDataFrame
#   geom_simplfy
#   shp_2_VRT






class folder_manager():
    
    '''
    - folder_info
    - folder_content
    - mosaic
    '''
    
    def __init__(self, folder):
        
        self.folder     = folder

        img = []
        shp = []
        for root, dirs, files in os.walk(self.folder):
            for file in files:
                if file.endswith('tif'):
                    img.append(root +'\\' + file)
                if file.endswith('shp'):
                    shp.append(root +'\\' + file)
                    
        self.tiff_li  = img
        self.tiff_num = len(img)
        self.shp_li   = shp
        self.shp_num  = len(shp)
        
    
    def folder_info(self):
        
        print ("folder path: " + self.folder)
        
        print ("number of tif in folder %d"\
               % (self.tiff_num))
            
        print ("number of shp in folder %d"\
               % (self.shp_num))
        
        if self.tiff_num > 0 :
            val_name    = ['Raster name','Size','bands']
            list_values = []
            for ras in self.tiff_li:
                ras1 = raster_manager(ras)
                list_values.append([ras , str(ras1.xsize)+'-' +str(ras1.ysize), ras1.bands])
    
            df =  pd.DataFrame(data = list_values, columns = val_name)
            print (df)
            print ("Number of unique size:   {}".format(df['Size'].nunique()))
            print ("Number of unique bands:  {}".format(df['bands'].nunique()))
            print ("Number of uniquerasters: {}".format(df['Raster name'].nunique()))
        
        
    def folder_content(self,num = 5):
        
        if self.tiff_num < num:
            print ('{} first values in tif list: {}'.format(self.tiff_num,self.tiff_li))
        else:
            print ('tiff in folder: {}'.format(self.tiff_li))
            
        if self.shp_num < num:
            print ('{} first values in tif list: {}'.format(self.shp_num,self.shp_li))
        else:
            print ('tiff in folder: {}'.format(self.shp_li))
            
            
    def mosaic(self,out_file, format = 'GTiff'):
        gdal.TermProgress = gdal.TermProgress_nocb
        global verbose, quiet
        verbose = 0
        quiet = 0
        #input_rasters = ['input1.tif','input2.tif']
        #format = 'GTiff'
        #out_file = 'out.tif'
    
        ulx = None
        psize_x = None
        separate = 0
        copy_pct = 0
        nodata = None
        create_options = []
        pre_init = []
        band_type = None
        createonly = 0
        
        gdal.AllRegister()
    
        if len(self.tiff_li) == 0:
            print('No input files selected.')
            sys.exit( 1 )
    
        Driver = gdal.GetDriverByName(format)
        if Driver is None:
            print('Format driver %s not found, pick a supported driver.' % format)
            sys.exit( 1 )
    
        DriverMD = Driver.GetMetadata()
        if 'DCAP_CREATE' not in DriverMD:
            print('Format driver %s does not support creation and piecewise writing \n select GTiff (the default) or HFA (Erdas Imagine).' % format)
            sys.exit( 1 )
    
        
        file_infos = names_to_fileinfos( self.tiff_li )
    
        if ulx is None:
            ulx = file_infos[0].ulx
            uly = file_infos[0].uly
            lrx = file_infos[0].lrx
            lry = file_infos[0].lry
            
            for fi in file_infos:
                ulx = min(ulx, fi.ulx)
                uly = max(uly, fi.uly)
                lrx = max(lrx, fi.lrx)
                lry = min(lry, fi.lry)
    
        if psize_x is None:
            psize_x = file_infos[0].geotransform[1]
            psize_y = file_infos[0].geotransform[5]
    
        if band_type is None:
            band_type = file_infos[0].band_type
    
       
        gdal.PushErrorHandler( 'CPLQuietErrorHandler' )
        try:
            t_fh = gdal.Open(out_file, gdal.GA_Update )
        except:
            t_fh = None
    
        gdal.PopErrorHandler()
        
    
        if t_fh is None:
            geotransform = [ulx, psize_x, 0, uly, 0, psize_y]
    
            xsize = int((lrx - ulx) / geotransform[1] + 0.5)
            ysize = int((lry - uly) / geotransform[5] + 0.5)
    
            if separate != 0:
                bands = len(file_infos)
            else:
                bands = file_infos[0].bands
    
            t_fh = Driver.Create( out_file, xsize, ysize, bands,
                                  band_type, create_options )
            if t_fh is None:
                print('Creation failed, terminating gdal_merge.')
                sys.exit( 1 )
                
            t_fh.SetGeoTransform( geotransform )
            t_fh.SetProjection( file_infos[0].projection )
    
            if copy_pct:
                t_fh.GetRasterBand(1).SetRasterColorTable(file_infos[0].ct)
        else:
            if separate != 0:
                bands = len(file_infos)
                if t_fh.RasterCount < bands :
                    print('Existing output file has less bands than the number of input files. You should delete it before. Terminating gdal_merge.')
                    sys.exit( 1 )
            else:
                bands = min(file_infos[0].bands,t_fh.RasterCount)
    
    
        if pre_init is not None:
            if t_fh.RasterCount <= len(pre_init):
                for i in range(t_fh.RasterCount):
                    t_fh.GetRasterBand(i+1).Fill( pre_init[i] )
            elif len(pre_init) == 1:
                for i in range(t_fh.RasterCount):
                    t_fh.GetRasterBand(i+1).Fill( pre_init[0] )
    
       
        t_band = 1
    
        if quiet == 0 and verbose == 0:
            gdal.TermProgress( 0.0 )
        fi_processed = 0
        
        for fi in file_infos:
            if createonly != 0:
                continue
            
            if verbose != 0:
                print("")
                print("Processing file %5d of %5d, %6.3f%% completed." \
                      % (fi_processed+1,len(file_infos),
                         fi_processed * 100.0 / len(file_infos)) )
                fi.report()
    
            if separate == 0 :
                for band in range(1, bands+1):
                    fi.copy_into( t_fh, band, band, nodata )
            else:
                fi.copy_into( t_fh, 1, t_band, nodata )
                t_band = t_band+1
                
            fi_processed = fi_processed+1
            if quiet == 0 and verbose == 0:
                gdal.TermProgress( fi_processed / float(len(file_infos))  )
        
       
        t_fh = None
        gc.enable()
        
        
    def del_zero_value_in_raster(self,file_type = 'tif'):
        rasters = []
        
        for root, dirs, files in os.walk(self.folder):
            for f in files:
                if f.endswith(file_type):
                    rasters.append(root +'\\'+f)
                    
        
        for ras in rasters:
            raster = gdal.Open(ras)
            
            
            num_bands = raster.RasterCount
            
            num = 0
            for i in range(1,num_bands+1):
                band       = raster.GetRasterBand(i)
                band_array = band.ReadAsArray()
                mean_band  = np.mean(band_array)
                num += mean_band
            
            del raster
            del band
            del band_array
            del mean_band
            
            if num == 0:
                os.remove(ras)
                print ("deleted {} because of no value".format(ras))
            else:
                pass
                #print ("{} have_value".format(ras))

class raster_manager():
    
    '''
    - raster_data
    - polygonize
    - replace_value
    - Cut_raster_to_pices
    - extract xyz from DEM
    - clean_empty_img
    - visualize
    - Pyramid
    - resampl_img
    - Checking_Pix_count
    - avr_bands(in_ds)
    - RasterRize_gdal
    - del_zero_value_in_raster
    - RasterRize_gdal_subprocess
    - findRasters
    - get_BandsTo3_And_BitTo8
    '''


    def __init__(self, filename):
        ds                = gdal.Open( filename )
        self.ds           = ds
        self.filename     = filename                     # שם הקובץ
        self.bands        = ds.RasterCount               # מספר ערוצים
        self.xsize        = ds.RasterXSize               # גודל פיקסל X
        self.ysize        = ds.RasterYSize               # גודל פיקסך Y
        self.band_type    = ds.GetRasterBand(1).DataType # סוג 
        self.projection   = ds.GetProjection()
        self.geotransform = ds.GetGeoTransform()
        self.ulx = self.geotransform[0]                         # נקודה שמאלית עליונה X
        self.uly = self.geotransform[3]                         # נקודה שמאלית עליונה Y
        self.lrx = self.ulx + self.geotransform[1] * self.xsize # נקודה ימינית תחתונה X
        self.lry = self.uly + self.geotransform[5] * self.ysize # נקודה שמאלית תחתונה Y

        ct = ds.GetRasterBand(1).GetRasterColorTable()
        if ct is not None:
            self.ct = ct.Clone()
        else:
            self.ct = None

    def raster_data( self ):
        print('Filename: '+ self.filename)
        print('File Size: %dx%dx%d' \
              % (self.xsize, self.ysize, self.bands))
        print('Pixel Size: %f x %f' \
              % (self.geotransform[1],self.geotransform[5]))
        print('UL:(%f,%f)   LR:(%f,%f)' \
              % (self.ulx,self.uly,self.lrx,self.lry))
        print ("Data Type: %d" \
               % (self.band_type))
        print ("bands Number: %d" \
               % (self.bands))
        
    def polygonize(self,shp_path):
        # mapping between gdal type and ogr field type
        type_mapping = {gdal.GDT_Byte: ogr.OFTInteger,
                        gdal.GDT_UInt16: ogr.OFTInteger,
                        gdal.GDT_Int16: ogr.OFTInteger,
                        gdal.GDT_UInt32: ogr.OFTInteger,
                        gdal.GDT_Int32: ogr.OFTInteger,
                        gdal.GDT_Float32: ogr.OFTReal,
                        gdal.GDT_Float64: ogr.OFTReal,
                        gdal.GDT_CInt16: ogr.OFTInteger,
                        gdal.GDT_CInt32: ogr.OFTInteger,
                        gdal.GDT_CFloat32: ogr.OFTReal,
                        gdal.GDT_CFloat64: ogr.OFTReal}
    
        # open polygon and set Projection

        srcband       = self.ds.GetRasterBand (1)
        dst_layername = "Shape"
        
        # Create polygon
        drv          = ogr.GetDriverByName      ("ESRI Shapefile")
        dst_ds       = drv.CreateDataSource     (shp_path)
        srs          = osr.SpatialReference     (wkt=self.projection)
        dst_layer    = dst_ds.CreateLayer       (dst_layername, srs=srs)
        raster_field = ogr.FieldDefn            ('id', type_mapping[srcband.DataType]) # get gdal based field type to ogr 
        
        dst_layer.CreateField           (raster_field)
        gdal.Polygonize                 (srcband, srcband, dst_layer, 0, [], callback=None)
        
        del srcband, dst_ds, dst_layer
               

    def replace_value(self,raster_path,first_value,To_value):
        
        band1 = self.ds.GetRasterBand(1)
        array = band1.ReadAsArray()
        
        myarray   = np.array(array)
        new_array = np.where(myarray == first_value,To_value,first_value)
        
        driver    = gdal.GetDriverByName ('GTiff')                                         # פתיחת קריאה  TIFF
        out_ds    = driver.Create        (raster_path, self.xsize, self.ysize, self.bands) # יצירה של אובייקט חדש בגודל בשם, גודל ומספר ערוצים
        out_ds.SetProjection             (self.projection)
        out_ds.SetGeoTransform           (self.geotransform)
        out_band  = out_ds.GetRasterBand (1)
        out_band.WriteArray              (new_array)
        
        out_band.FlushCache              ()
        
        gc.enable()
        del out_ds
        del driver
        
    @staticmethod
    def createFolder(dic):
        try:
            if not os.path.exists(dic):
                os.makedirs(dic)
        except OSError:
            print ("Error Create dic")
        
        
    def Cut_raster_to_pices(self,out_put_folder,tilesize = 512):
    
        name = os.path.basename(self.filename).split('.')[0]
        

        width  = self.xsize
        height = self.ysize
    
        for i in range(0,width,tilesize):
            for j in range(0,height,tilesize):
                w = tilesize
                h = tilesize

                gdaltranString = "gdal_translate -of GTIFF -srcwin "+str(i)+", "+str(j)+", "+str(w)+", " \
                        +str(h)+" " + self.filename + " " + out_put_folder + "\\_"+ str(name)+"__" +str(i)+"_"+str(j)+".tif"

                os.system(gdaltranString)
                
    def Extract_XYZ_from_DEM(self,csv_out):

        # TIFF to CSV 

        xyz = gdal.Translate("dem.xyz", self.ds)
        xyz = None
        
        df = pd.read_csv("dem.xyz", sep = " ", header = None)
        df.columns = ["x","y", "value"]
        df.to_csv(csv_out , index = False)




def visualize(**images):
    """
    PLot images in one row.
    320X320X3 or 320X320X1 !!!3X333X333 will transform ()
    """
    n = len(images)
    plt.figure(figsize=(16, 5))
    for i, (name, image) in enumerate(images.items()):
        plt.subplot(1, n, i + 1)
        plt.xticks([])
        plt.yticks([])
        plt.title(' '.join(name.split('_')).title())
        if image.shape[0]==3:  image = image.transpose(1,2,0)
        plt.imshow(image)
    plt.show()


def clean_empty_img(path_to_img_stamp):
    print ('cleaning images with no mask')
    for img in os.listdir(path_to_img_stamp):
        img_file = path_to_img_stamp + "\\" + img
        array = np.array(gdalnumeric.LoadFile(img_file))
        if np.max(array)==255 and  np.min(array)==255 :
             print ('removing empty img: ',img)
             os.remove(img_file)



class Pyramid():
    def __init__(self,folder):
        self.folder  = folder
        self.rasters = []
    
    def pyramids(self):
        if self.rasters:
            for gdaladdoFile in self.rasters:
                print ('working on: {}'.format(gdaladdoFile))
                Image = gdal.Open(gdaladdoFile, 0)  
                gdal.SetConfigOption('COMPRESS_OVERVIEW', 'DEFLATE')
                Image.BuildOverviews('CUBIC', [2,4, 8, 16, 32, 64, 128], gdal.TermProgress_nocb)
                
                del Image 

    def readTif(self):
        self.rasters = [root +'\\' + file for root, dirs, files in os.walk(self.folder) for file in files if file.endswith('tif')]

    def activate(self):
        self.readTif  ()
        self.pyramids ()


def Combine_dict(ListOfDict):
    '''
    input: List of dictionary: [{1: 10, 2: 20, 3: 30},{1: 10, 2: 20, 4: 30},{2: 20, 5: 30}]
    Out put:                   {1: 20, 2: 60, 3: 30, 4: 30, 5: 30}
    '''
    new_dic = {}
    for i in ListOfDict:
        for key, val in i.items():
            try:
                new_dic[key] += val
            except:
                new_dic[key] = val
    return new_dic


def Checking_Pix_count(folder):
    '''
    input:   folder contains tif files
    Out put: 1) count pix as txt file of every tif
             2) sum of all tif pix
    '''
    total = []   
    for root, dirs, files in os.walk(folder):
        for file in files:
            if file.endswith('.tif'):
                path_raster = root + '\\' + file
                raster_file = gdalnumeric.LoadFile (path_raster)
                flat        = raster_file.flatten  ()
                data        = {value: len(list(freq)) for value, freq in groupby(sorted(flat))}
                text_path   =  path_raster.split('.')[0] + '_text.txt'
                text_file   = open(text_path, "w")
                text_file.write(str(data))
                total.append   (data)
                text_file.close()
                print ('in Put: '  + path_raster)
                print ('Out Put: ' + text_path)


def resampl_img(img_in, img_out, res_out=0.4):
    print (f"[info] resampling to resolution {res_out}")
    ds = gdal.Open(img_in)
    ds = gdal.Translate(img_out, img_in, options = \
                        (gdal.TranslateOptions\
                         (xRes=res_out,yRes=res_out,outputType=gdal.gdalconst.GDT_Byte)))
    ds = None

    return img_out


def avr_bands(in_ds):
    
    name = os.path.basename(in_ds).split('.')[0]
    out = os.path.dirname(in_ds) + '\\'+name+'_gray.tif'
    in_ds = gdal.Open(in_ds)
    num_bands = in_ds.RasterCount
    band1 = in_ds.GetRasterBand(1)    
    band2 = in_ds.GetRasterBand(2) 
    band3 = in_ds.GetRasterBand(3)
    red       = band1.ReadAsArray()*0.3
    blue      = band3.ReadAsArray()*0.11
    if num_bands == 4:
        band4 = in_ds.GetRasterBand(4)
        green_inf = np.mean([band2.ReadAsArray(),band4.ReadAsArray()],axis = 0)*0.59
    else:
        print ("didn't find the 4th band")
        green_inf = np.mean([band2.ReadAsArray()],axis = 0)*0.59
        
    new_array = np.sum([green_inf,blue,red],axis = 0)
    driver    = gdal.GetDriverByName('GTiff')
    out_ds    = driver.Create(out, in_ds.RasterXSize, in_ds.RasterYSize, 1, gdal.GDT_CFloat64)
    
    out_ds.SetProjection  (in_ds.GetProjection())
    out_ds.SetGeoTransform(in_ds.GetGeoTransform())
    
    out_band = out_ds.GetRasterBand(1)
    out_band.WriteArray            (new_array)
    out_band.FlushCache            ()
    out_ds.FlushCache              ()

    return out


def RasterRize_gdal(TemplatRas,shp,output):
    

    data = gdal.Open(TemplatRas, gdalconst.GA_ReadOnly)
    geo_transform = data.GetGeoTransform()
    
    x_min = geo_transform[0]
    y_max = geo_transform[3]
    y_min = y_max + geo_transform[5] * data.RasterYSize
    x_res = data.RasterXSize
    y_res = data.RasterYSize
    mb_v = ogr.Open(shp)
    mb_l = mb_v.GetLayer()
    pixel_width = geo_transform[1]
    
    target_ds = gdal.GetDriverByName('GTiff').Create(output, x_res, y_res, 1, gdal.GDT_Byte)
    target_ds.SetGeoTransform((x_min, pixel_width, 0, y_min, 0, pixel_width))
    band = target_ds.GetRasterBand(1)
    NoData_value = -999999
    band.SetNoDataValue(NoData_value)
    band.FlushCache()
    gdal.RasterizeLayer(target_ds, [1], mb_l, options=["ATTRIBUTE=Class_num"])
    
    target_ds = None
    
    return output




def del_zero_value_in_raster(folder):

    rasters = [root +'\\'+f for root, dirs, files in os.walk(folder) for f in files if f.endswith(r'tif')]
                
    for ras in rasters:
        raster    = gdal.Open(ras)
        num_bands = raster.RasterCount
        
        num = 0
        for i in range(1,num_bands+1):
            band       = raster.GetRasterBand(i)
            band_array = band.ReadAsArray()
            mean_band  = np.mean(band_array)
            num += mean_band
        
        del raster
        del band
        del band_array
        del mean_band
        
        if num == 0:
            os.remove(ras)
            print ("deleted {} because of no value".format(ras))


def RasterRize_gdal_subprocess(shp_in,img_ref,img_out):

    #   retriving size and resulution from the input reference image

    print ("[info]reading image data")
    
    img_ref        = gdal.Open(img_ref)
    img_ref_cols   = img_ref.RasterXSize
    img_ref_rows   = img_ref.RasterYSize
    geotransform   = img_ref.GetGeoTransform()
    top_left_X     = geotransform[0]
    pixel_size     = float(geotransform[1]) # pixel size in the X direction
    top_left_Y     = geotransform[3]
    xmin = top_left_X
    ymin = top_left_Y - img_ref_rows*pixel_size
    xmax = top_left_X + img_ref_cols*pixel_size
    ymax = top_left_Y 

    print ("[info]runing gdal_rasterize") 

    subprocess.call('gdal_rasterize -burn 250 -ot Byte -te {} {} {} {} -tr {} {}  {} {} '.\
              format( xmin, ymin,xmax,ymax,pixel_size,pixel_size,shp_in,img_out),shell = True)

    return img_out

# Private Class
class FindRasters():

    def __init__(self,raster):
        self.path = raster
        self.name = os.path.basename(raster)
        self.raster = gdal.Open(raster)

    def getCount(self):
        if self.raster is not None: return str(self.raster.RasterCount)
        return None

    def getDimensions(self):

        gt =self.raster.GetGeoTransform()

        self.pixelSizeX = gt[1]
        self.pixelSizeY =-gt[5]

        return  str(round(self.pixelSizeX,1)) + ' x ' + str(round(self.pixelSizeY,1))

    def getArea(self):

        ima = np.array(self.raster.GetRasterBand(1).ReadAsArray())

        rows = ima.shape[0] * self.pixelSizeX
        cols = ima.shape[1] * self.pixelSizeY
        total = (rows * cols)/10000000

        return round(total,2)


def makeTxT(txt):
    splitme = txt.split('.')
    if len(splitme) == 1:
        return  txt +'.txt'
    return txt

def findRasters(folder,outPut):
    rasters = [root +'\\' + file for root, dirs, files in os.walk(folder)\
            for file in files if (file.endswith('tiff')or file.endswith('tif'))]

    listdata = []
    for ras in rasters:
        Raster = FindRasters(ras)
        listdata.append([Raster.path,Raster.name,Raster.getDimensions(),Raster.getArea(),Raster.getCount()])

    df = pd.DataFrame(data =listdata,columns = ['path','name','Dimensions','area','countBands'])
    df.to_csv(makeTxT(outPut))


# end private class


def get_BandsTo3_And_BitTo8(inputRaster,outputTif):

    raster = raster_manager(inputRaster)

    if raster.bands == 3: return

    band     = raster.ds.GetRasterBand(1)
    BandType = str(gdal.GetDataTypeName(band.DataType))

    print ('Current Data Type:   {}'.format(BandType))
    print ('Current bands count: {}'.format(raster.bands))


    columns, rows = (band.XSize, band.YSize)
    driver        = gdal.GetDriverByName("GTiff")
    dst_ds        = driver.Create(outputTif,columns,rows,3,gdal.GDT_Byte)

    if (raster.bands == 4) and (raster.band_type != 1):
        for i in range(raster.bands):
            if i == 0: continue
            band = raster.ds.GetRasterBand(i).ReadAsArray()
            dst_ds.GetRasterBand(i).WriteArray( band )
        dst_ds.SetGeoTransform(raster.geotransform) 

    del dst_ds
    del driver
    gc.enable()




class layer_manager():
    
    '''
    - Get_fields_name
    - Create_field
    - Calculate_Field
    - Calculate_Area
    - Get_Atrributes
    - Get_Numaric_Atrributes
    - Get_layer_Count
    - Get_Fields_max
    - Get_Fields_min
    - Get_field_count
    - Get_layers_in_folder
    - get_unique
    - get_geom
    - Get_Extent
    - buffer
    - Line_to_point
    - poly_to_line
    - poly_to_point
    - delete_layer
    - createFolder
    - get_unique
    - Poly_To_Centroid
    - Select_layer_by_atrributes
    - Dissolve_Polygons
    - Buffer_in_and_out
    - RasterRize
    - Clean_Vrtx
    - Intersect_poly_point
    - layerToDataFrame
    - layerToDataFrame
    - geom_simplfy
    - resampl_img
    - shp_2_VRT

    '''
    
    def __init__(self,path):
        self.path   = path
        self.source = ogr.Open(path,update=True)
        self.layer  = self.source.GetLayer()
        self.folder = os.path.dirname(self.path)
        ds = ogr.Open(os.path.dirname(self.path), 1) 
        self.ds         = ds
        self.layer_name = os.path.basename(path).split('.')[0]
        self.sr         = self.layer.GetSpatialRef()
        layer_defn = self.layer.GetLayerDefn()
        schema     = [layer_defn.GetFieldDefn(i).GetName() for i in range(layer_defn.GetFieldCount())]
        self.schema = schema
                
    def Get_fields_name(self):

        layer_defn = self.layer.GetLayerDefn()
        schema     = [layer_defn.GetFieldDefn(i).GetName() for i in range(layer_defn.GetFieldCount())]
        print (schema)

    def Create_field(self,field_Name,type1 = 'TEXT'):

        def Convert_Type(type1):
            type1 = type1.upper()

            Conv_types =  {'LONG' :ogr.OFTInteger,
                           'FLOAT':ogr.OFTReal,
                           'TEXT' :ogr.OFTString}

            return Conv_types[type1]

        type_ogr  = Convert_Type('TEXT')
        new_field = ogr.FieldDefn(field_Name, type_ogr)
        self.layer.CreateField(new_field)


    def Calculate_Field(self,field,value):
        
        if (str(value)[0]) == '[' and (str(value)[-1] == ']'):
            value = value[1:-1]
            GET_field = True

        if GET_field:
                for row in self.layer:
                    new_value = row.GetField(value)
                    row.SetField(field, new_value)
                    self.layer.SetFeature(row)

        else:
            for row in self.layer:
                row.SetField(field, value)
                self.layer.SetFeature(row)


    def Calculate_Area(self,field1 = ''):


        if field1 == '':
            self.Create_field('AREA',"FLOAT")
            field1 = 'AREA'

        fields_name = self.Get_fields_name() + ["AREA"]
        if field1 in fields_name:
            for row in self.layer:
                Area        = round(row.geometry().GetArea(),3)
                row.SetField(field1,Area)
                self.layer.SetFeature(row)
        else:
            print ("Field: {}, does not exists")
    
    def Get_Atrributes(self):
        
        self.Get_fields_name()

        # get all fields and there values in a list
        values     = [[feat.GetFID(),i,feat.GetField(i)] for i in self.schema for feat in self.layer]
        df          = pd.DataFrame(values, columns = ['OBJECTID','field','Value'])
        self.df     = df
    
        return  df
    
    def Get_Numaric_Atrributes(self):
        
        self.Get_fields_name()
        values_num = [[feat.GetFID(),i,float(feat.GetField(i))] for i in self.schema for feat in self.layer if str(feat.GetField(i)).isdigit()]
        df_num      = pd.DataFrame(values_num, columns = ['OBJECTID','field','Value'])
        self.df_num = df_num
        return  df_num
        
    def Get_layer_Count(self):
        return (len([feat for feat in self.layer]))
        
        
    def Get_Fields_max(self):

        self.Get_Numaric_Atrributes()
        print (self.df_num)
        df_field_MAX = self.df_num.groupby('field').agg({'Value':'max'})
        return  df_field_MAX
    
    def Get_Fields_min(self):

        self.Get_Numaric_Atrributes()
        print (self.df_num)
        df_field_MAX = self.df_num.groupby('field').agg({'Value':'max'})
        return  df_field_MAX
    
    def Get_field_count(self):

        self.Get_Atrributes()
        return (self.df['field'].nunique())
    
    def Get_layers_in_folder(self):
        
        layer_in_folder = [os.path.join(self.folder, filename) for filename in os.listdir(self.folder) if filename.endswith('.shp') if filename]
        return layer_in_folder
    
    def get_unique(self,field_name):
        
        datasource  = self.source
        
        sql    = 'SELECT DISTINCT {0} FROM {1}'.format(field_name, self.layer_name)
        lyr    = datasource.ExecuteSQL(sql)
        values = [row.GetField(field_name) for row in lyr]
        
        datasource.ReleaseResultSet(lyr)
        return values

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

    def get_geom(self):
        """Return the geometry for a state."""
    
        geom = []
        lyr = self.layer
        for feat in lyr:
            geom.append(feat.geometry().Clone())
        return geom
    
    def Get_Extent(self):
        extent = self.layer.GetExtent()
        return extent
            
    def Buffer(self,Output,num):
        
        if not Output:
            Output = self.path.split('.')[0]+ '_Buffer_'+str(abs(num)) + '.shp'
        
        shpdriver = ogr.GetDriverByName('ESRI Shapefile')
        if os.path.exists(Output):
            shpdriver.DeleteDataSource(Output)
            
        outputBufferds = shpdriver.CreateDataSource(Output)
        bufferlyr      = outputBufferds.CreateLayer(Output, geom_type=ogr.wkbPolygon)
        featureDefn    = bufferlyr.GetLayerDefn()
        
        fieldNames = []
        for i in range(self.layer.GetLayerDefn().GetFieldCount()):
            fieldDefn = self.layer.GetLayerDefn().GetFieldDefn(i)
            bufferlyr.CreateField(fieldDefn)
            fieldNames.append(fieldDefn.name)
            
        for feature in self.layer:
            ingeom = feature.GetGeometryRef()
            fieldVals = [] # make list of field values for feature
            for f in fieldNames: fieldVals.append(feature.GetField(f))
    
            outFeature = ogr.Feature(featureDefn)
            geomBuffer = ingeom.Buffer(num)
            outFeature.SetGeometry(geomBuffer)
            for v, val in enumerate(fieldVals): # Set output feature attributes
                outFeature.SetField(fieldNames[v], val)
            bufferlyr.CreateFeature(outFeature)
            
        copyfile(self.path.replace('.shp', '.prj'), Output.replace('.shp', '.prj'))

        del outputBufferds
        return Output
    
    def Line_to_point(self, pt_name):

        """Creates a point layer from vertices in a line layer."""

        line_lyr = self.layer
        sr       = line_lyr.GetSpatialRef()
    
        dst_layername = "pnt"
        drv           = ogr.GetDriverByName("ESRI Shapefile")
        dst_ds        = drv.CreateDataSource(pt_name)
        
        # Delete the point layer if it exists.
        if dst_ds.GetLayer(pt_name):
            dst_ds.DeleteLayer(pt_name)

        pt_lyr = dst_ds.CreateLayer(dst_layername, sr,ogr.wkbPoint)

    
        # Create a feature and geometry to use over and over.
        pt_feat = ogr.Feature(pt_lyr.GetLayerDefn())
        pt_geom = ogr.Geometry(ogr.wkbPoint)
    
        # Loop through all of the lines.
        for line_feat in line_lyr:
    
            atts = line_feat.items()
            for fld_name in atts.keys():
                pt_feat.SetField(fld_name, atts[fld_name])
    

            for coords in line_feat.geometry().GetPoints():
                pt_geom.AddPoint(*coords)
                pt_feat.SetGeometry(pt_geom)
                pt_lyr.CreateFeature(pt_feat)
    
        del dst_ds
    
    
    def poly_to_line(self,line_name,Driver = "ESRI Shapefile"):
        """Creates a line layer from a polygon layer."""
        # Delete the line layer if it exists.

    
        # Get the polygon layer and its spatial reference.
        poly_lyr = self.layer
        sr = poly_lyr.GetSpatialRef()
    
        
        drv           = ogr.GetDriverByName("ESRI Shapefile")
        dst_ds        = drv.CreateDataSource(line_name)
        
        if dst_ds.GetLayer(line_name):
            dst_ds.DeleteLayer(line_name)
        
        line_lyr = dst_ds.CreateLayer(line_name, sr, ogr.wkbLineString)
        line_lyr.CreateFields(poly_lyr.schema)
    
        # Create a feature to use over and over.
        line_feat = ogr.Feature(line_lyr.GetLayerDefn())
    
        # Loop through all of the polygons.
        for poly_feat in poly_lyr:
    
            atts = poly_feat.items()
            for fld_name in atts.keys():
                line_feat.SetField(fld_name, atts[fld_name])
    
            # Loop through the rings in the polygon.
            poly_geom = poly_feat.geometry()
            for i in range(poly_geom.GetGeometryCount()):
                ring = poly_geom.GetGeometryRef(i)
    
                # Create a new line using the ring's vertices.
                line_geom = ogr.Geometry(ogr.wkbLineString)
                for coords in ring.GetPoints():
                    line_geom.AddPoint(*coords)
    
                # Insert the new line feature.
                line_feat.SetGeometry(line_geom)
                line_lyr.CreateFeature(line_feat)
    
        del dst_ds
    
    def delete_layer(self,layer):
        
        name = os.path.basename(layer).split('.')[0]
        ds = ogr.Open(os.path.dirname(layer)) 
        if ds.GetLayer(name):
            ds.DeleteLayer(name) 
        
        list_formats = ['.shp','.sbn','.prj','.dbf','.cpg','.shp.xml','.sbx',]
        for i in list_formats:
            delete = layer.split('.')[0] + i
            if os.path.exists(delete):
                try:
                    os.unlink(delete)
                except:
                    pass
                try:
                    os.remove(delete)
                except:
                    pass
                
    def createFolder(self,dic):
        try:
            if not os.path.exists(dic + '\\' + 'try1'):
                os.makedirs(dic+ '\\' + 'try1')
                return dic+ '\\' + 'try1'
            else:
                return dic+ '\\' + 'try1'

        except OSError:
            print ("Error Create dic")
        
    
    def Poly_to_point(self,point):
        #line_name = os.path.dirname(point) +'\\'+ 'Temp_line.shp'
        self.createFolder('C:\\temp')
        line_name = r'C:\temp' +'\\'+ 'Temp_line.shp'
        self.poly_to_line(line_name,'memory')
        line = layer_manager(line_name)
        line.Line_to_point(point)
        
        self.delete_layer(line_name)
        
    
    def CSV_to_layer(self,csv_fn,shp_fn):
        

        df = pd.read_csv(csv_fn)
        columns = list(df.columns.values.tolist()) 
        print (columns)
        
        
        col_X_Y = self.SequenceMatcher_To_Table(['X','Y'],columns)
        
        X = col_X_Y['X']
        Y = col_X_Y['Y']
        
        #sr = osr.SpatialReference(osr.SRS_WKT_WGS84)
        
        #sr = osr.SpatialReference()
        #sr.ImportFromEPSG(2039)
        
        # Create the shapefile with two attribute fields.
        shp_ds = ogr.GetDriverByName('ESRI Shapefile').CreateDataSource(shp_fn)
        
        shp_lyr = shp_ds.CreateLayer('new_point', self.sr, ogr.wkbPoint)
        for col in columns:
            shp_lyr.CreateField(ogr.FieldDefn(col, ogr.OFTString))
            
            
        shp_row = ogr.Feature(shp_lyr.GetLayerDefn())
        
        # Open the csv and loop through each row.
        csv_ds = ogr.Open(csv_fn)
        csv_lyr = csv_ds.GetLayer()
        for csv_row in csv_lyr:
        
            # Get the x,y coordinates from the csv and create a point geometry.
            x = csv_row.GetFieldAsDouble(X)
            y = csv_row.GetFieldAsDouble(Y)
            shp_pt = ogr.Geometry(ogr.wkbPoint)
            shp_pt.AddPoint(x, y)
        
            # Get the attribute data from the csv.
            field_val = []
            for col in columns:
                field_val.append([col,csv_row.GetField(col)])
        
        
            # Add the data to the shapefile.
            shp_row.SetGeometry(shp_pt)
            for col in field_val:
                shp_row.SetField(col[0], col[1])
            shp_lyr.CreateFeature(shp_row)
        
        del csv_ds, shp_ds


    def Poly_To_Centroid(self,pt_name):
        
        sr = self.layer.GetSpatialRef()
          
        dst_layername = "pnt_name"
        drv           = ogr.GetDriverByName("ESRI Shapefile")
        dst_ds        = drv.CreateDataSource(pt_name)
        
        # Delete the point layer if it exists.
        if dst_ds.GetLayer(pt_name):
            dst_ds.DeleteLayer(pt_name)
        
        pt_lyr = dst_ds.CreateLayer(dst_layername, sr,ogr.wkbPoint)
        pt_lyr.CreateFields(self.layer.schema)
        
        pt_feat = ogr.Feature(pt_lyr.GetLayerDefn())
        pt_geom = ogr.Geometry(ogr.wkbPoint)
        
        
        for feat in self.layer:
                  
            atts = feat.items()
            for fld_name in atts.keys():
                pt_feat.SetField(fld_name, atts[fld_name])
        
        
            geom     = feat.geometry().Clone()
            centroid = geom.Centroid()
            x        = centroid.GetX()
            y        = centroid.GetY()
        
        
            pt_geom.AddPoint     (x,y)
            pt_feat.SetGeometry  (pt_geom)
            pt_lyr.CreateFeature (pt_feat)
            
        del dst_ds


    def Select_layer_by_atrributes(self,output,sql_in):
        sql = '''SELECT COUNTRY FROM "'''+self.layer_name+'''" WHERE '''+ sql_in +''' '''
        print (sql)
        layer = self.ds.ExecuteSQL(sql)
        
        sr = layer.GetSpatialRef()
        
        #print(layer.GetFeatureCount())
        
        
        dst_layername = "pnt_name"
        drv           = ogr.GetDriverByName("ESRI Shapefile")
        dst_ds        = drv.CreateDataSource(output)
        
        
        if dst_ds.GetLayer(output):
            dst_ds.DeleteLayer(output)
        
        pt_lyr = dst_ds.CreateLayer(dst_layername, sr,ogr.wkbPolygon)
        pt_lyr.CreateFields(layer.schema)
        
        pt_feat = ogr.Feature(pt_lyr.GetLayerDefn())
        
        for feat in layer:
                  
            atts = feat.items()
            for fld_name in atts.keys():
                pt_feat.SetField(fld_name, atts[fld_name])
        
            geom     = feat.geometry().Clone()
        
            pt_feat.SetGeometry  (geom)
            pt_lyr.CreateFeature (pt_feat)
            
        del dst_ds
        
        
    def Dissolve_Polygons(self,out_put):

        drv     = ogr.GetDriverByName('ESRI Shapefile')
        out_ds  = drv.CreateDataSource(out_put)
        out_lyr = out_ds.CreateLayer('name', self.sr, ogr.wkbPolygon)
        
        defn = out_lyr.GetLayerDefn()
        multi = ogr.Geometry(ogr.wkbMultiPolygon)
        for feat in self.layer:
            if feat.geometry():
                feat.geometry().CloseRings() # this copies the first point to the end
                wkt = feat.geometry().ExportToWkt()
                multi.AddGeometryDirectly(ogr.CreateGeometryFromWkt(wkt))
        union = multi.UnionCascaded()
        
        
        out_feat = ogr.Feature(defn)
        out_feat.SetGeometry(union)
        out_lyr.CreateFeature(out_feat)
        out_ds.Destroy()

    
    def Buffer_in_and_out(self,Output,num):
        
        
        if not Output:
            Output = self.path.split('.')[0]+ '_Buffer_'+str(abs(num)) + '.shp'
        
        shpdriver = ogr.GetDriverByName('ESRI Shapefile')
        if os.path.exists(Output):
            shpdriver.DeleteDataSource(Output)
            
        outputBufferds = shpdriver.CreateDataSource(Output)
        bufferlyr      = outputBufferds.CreateLayer(Output, geom_type=ogr.wkbPolygon)
        featureDefn    = bufferlyr.GetLayerDefn()
        
        fieldNames = []
        for i in range(self.layer.GetLayerDefn().GetFieldCount()):
            fieldDefn = self.layer.GetLayerDefn().GetFieldDefn(i)
            bufferlyr.CreateField(fieldDefn)
            fieldNames.append(fieldDefn.name)
            
        for feature in self.layer:
            ingeom = feature.GetGeometryRef()
            fieldVals = [] # make list of field values for feature
            for f in fieldNames: fieldVals.append(feature.GetField(f))
    
            outFeature = ogr.Feature(featureDefn)
            num_minus  = num * -1
            geomBuffer = ingeom.Buffer(num_minus)
            geomBuffer = geomBuffer.Buffer(num)
            outFeature.SetGeometry(geomBuffer)
            for v, val in enumerate(fieldVals): # Set output feature attributes
                outFeature.SetField(fieldNames[v], val)
            bufferlyr.CreateFeature(outFeature)
            
        copyfile(self.path.replace('.shp', '.prj'), Output.replace('.shp', '.prj'))

        del outputBufferds
        return Output        
        
    
    def RasterRize(self,ndsm,output):
        
        data = gdal.Open(ndsm, gdalconst.GA_ReadOnly)
        geo_transform = data.GetGeoTransform()
        
        x_min = geo_transform[0]
        y_max = geo_transform[3]
        y_min = y_max + geo_transform[5] * data.RasterYSize
        x_res = data.RasterXSize
        y_res = data.RasterYSize
        pixel_width = geo_transform[1]
        
        target_ds = gdal.GetDriverByName('GTiff').Create(output, x_res, y_res, 1, gdal.GDT_Byte)
        target_ds.SetGeoTransform((x_min, pixel_width, 0, y_min, 0, pixel_width))
        band = target_ds.GetRasterBand(1)
        NoData_value = -999999
        band.SetNoDataValue(NoData_value)
        band.FlushCache()
        gdal.RasterizeLayer(target_ds, [1], self.layer)
        
        target_ds = None
        
        return output


    def Clean_Vrtx(self,Output,val = 0):
    
        def collinearity(p1, p2, p3):
        
            x1, y1 = p1[0], p1[1]
            x2, y2 = p2[0], p2[1]
            x3, y3 = p3[0], p3[1]
            res = x1 * (y2 - y3) + x2 * (y3 - y1) + x3 * (y1 - y2)
            return abs(res) 
        
        total_vrtx        = 0
        num_vertx_deleted = 0
        wkt = 'MULTIPOLYGON ('
        num  = 0
        for feat in self.layer:
            geom   = feat.GetGeometryRef()
            ring   = geom.GetGeometryRef(0)
            points = ring.GetPointCount()
            wkt +='(('
            for p in range(points):
                if (p > 0) and (p < points-1):
                    x1, y1, z1 = ring.GetPoint(p-1)
                    x2, y2, z2 = ring.GetPoint(p)
                    x3, y3, z3 = ring.GetPoint(p+1)
                    colly = collinearity([x1, y1], [x2, y2], [x3, y3])
        
                if p == 0:
                    x1, y1, z1 = ring.GetPoint(points-2)
                    x2, y2, z2 = ring.GetPoint(p)
                    x3, y3, z3 = ring.GetPoint(p +1)
                    colly = collinearity([x1, y1], [x2, y2], [x3, y3])
        
                if p == points-1:
                    x1, y1, z1 = ring.GetPoint(points-2)
                    x2, y2, z2 = ring.GetPoint(p)
                    x3, y3, z3 = ring.GetPoint(0)
                    colly = collinearity([x1, y1], [x2, y2], [x3, y3])
        
                if colly >= val:
                    wkt += str(x2) + ' ' +str(y2) +' 0,'
                else:
                    num_vertx_deleted +=1
                total_vrtx +=1
                
            wkt = wkt[0:-1]
            wkt += ')),'
            num+=1
                   
        wkt = wkt[:-1] + ')'
                
        print ("Total vrtx:   {}".format(str(total_vrtx)))
        print ("vrtx deleted: {}".format(str(num_vertx_deleted)))
        
        
        shpdriver = ogr.GetDriverByName('ESRI Shapefile')
        if os.path.exists(Output):
            shpdriver.DeleteDataSource(Output)
            
            
        driver = shpdriver.CreateDataSource(Output)
        new_layer      = driver.CreateLayer(Output, geom_type=ogr.wkbPolygon)
    
        pt_feat = ogr.Feature(new_layer.GetLayerDefn())
        
        geom_poly = ogr.CreateGeometryFromWkt(wkt)   
        
        pt_feat.SetGeometry  (geom_poly)
        new_layer.CreateFeature (pt_feat)
        
        del x1,x2,x3,y1,y2,y3,z1,z2,z3,num,colly,points,p,Output
        
        del driver

    def Intersect_poly_point(self,pt_path,Output):

        def Create_field(layer,field_Name,type1 = 'TEXT'):

            def Convert_Type(type1):
                    type1 = type1.upper()

                    Conv_types =  { 'LONG' :ogr.OFTInteger,
                                    'FLOAT':ogr.OFTReal,
                                    'TEXT' :ogr.OFTString}

                    return Conv_types[type1]

            type_ogr  = Convert_Type('TEXT')
            new_field = ogr.FieldDefn(field_Name, type_ogr)
            layer.CreateField(new_field)

        point_layer = layer_manager(pt_path)

        list_geom_id = []
        for n in point_layer.layer:
            geom_pt    = n.geometry().Clone()
            atts1      = n.items()
            feat_list  = []
            for fld_name in atts1.keys():
                feat_list.append([fld_name, atts1[fld_name]])
            list_geom_id.append([geom_pt,feat_list])


        shpdriver = ogr.GetDriverByName('ESRI Shapefile')
        if os.path.exists(Output):
            shpdriver.DeleteDataSource(Output)
            
        driver      = shpdriver.CreateDataSource   (Output)
        new_layer   = driver.CreateLayer           (Output ,self.sr,geom_type=ogr.wkbPoint)
        layer_defn  = self.layer.GetLayerDefn      ()

        op1 = [Create_field(new_layer,i) for i in point_layer.schema]
        op2 = [Create_field(new_layer,i) for i in self.schema]

        del op1
        del op2

        pt_feat = ogr.Feature(new_layer.GetLayerDefn())

        for feat in self.layer:
            geom = feat.geometry()
            for n in list_geom_id:
                new_geom = geom.Intersection(n[0])
                if str(new_geom) != "GEOMETRYCOLLECTION EMPTY":
                    pt_feat.SetGeometry  (n[0])
                    for fld_n in n:
                        pt_feat.SetField(n[1][0][0],n[1][0][1])
                    atts = feat.items()
                    for fld_name in atts.keys():
                        pt_feat.SetField(fld_name, atts[fld_name])
                    new_layer.CreateFeature (pt_feat)


def delIfExists(line_name):
    if os.path.exists(line_name):
        if dst_ds.GetLayer(line_name):
            dst_ds.DeleteLayer(line_name)


def Create_field(layer,field_Name,type1 = 'TEXT'):

    def Convert_Type(type1):
        type1 = type1.upper()

        Conv_types =  {'LONG' :ogr.OFTInteger,
                        'FLOAT':ogr.OFTReal,
                        'TEXT' :ogr.OFTString,
                        'DOUBLE':ogr.OFTReal}

        return Conv_types[type1]

    type_ogr  = Convert_Type(type1)
    new_field = ogr.FieldDefn(field_Name, type_ogr)
    layer.CreateField(new_field)


def layerToDataFrame(path):

    '''
    [INFO]   - get shp file and create dataframe with the attributes and geometry of the df
    [INPUT]  - SHP file
    [OUTPUT] - DataFrame, coordinate system
    '''

    source = ogr.Open    (path,update=True)
    layer  = source.GetLayer()

    feature      = layer.GetNextFeature    ()
    geometry     = feature.GetGeometryRef  ()
    geometrytype = geometry.GetGeometryName()
 
    sr          = layer.GetSpatialRef()
    layer_defn  = layer.GetLayerDefn ()
    columns     = [layer_defn.GetFieldDefn(i).GetName()\
                for i in range(layer_defn.GetFieldCount())]

    data    = []
    for feat in layer:
        atts = feat.items()
        temp = []
        for fld_name in atts.keys():
            temp.append(atts[fld_name])
        temp.append(feat.geometry().ExportToWkt())
        data.append(temp)


    columns.append('Geometry')
    df = pd.DataFrame(data = data,columns = columns)
    return df,sr,geometrytype


def createLayerFromDataFrame(df,sr,geometrytype,line_name):


    dict_types = {'int64':'LONG','object':'TEXT','float64':'DOUBLE','int32':'LONG'}


    drv           = ogr.GetDriverByName("ESRI Shapefile")
    dst_ds        = drv.CreateDataSource(line_name)

    delIfExists(line_name)

    dictGeom      = {'POINT':ogr.wkbPoint,'POLYLINE':ogr.wkbLineString,'POLYGON':ogr.wkbPolygon}
    line_lyr      = dst_ds.CreateLayer(str(line_name), sr, dictGeom[geometrytype])
    columns       = list(df.columns)
    dict_col_type = {col_name:dict_types[str(type_)] for col_name, type_ in df.dtypes.to_dict().items()}

    for i in columns:
        if i != 'Geometry':
            Create_field(line_lyr,i,dict_col_type[i])

    pt_feat = ogr.Feature(line_lyr.GetLayerDefn())

    for dataItem in df.to_dict('records'):
        for fld_name in dataItem.keys():
            if fld_name != 'Geometry':
                pt_feat.SetField(fld_name, dataItem[fld_name])

        geom = ogr.CreateGeometryFromWkt(dataItem['Geometry'])
        pt_feat.SetGeometry(geom)
        line_lyr.CreateFeature(pt_feat)

    del dst_ds



def geom_simplfy(shp_in, smooth_val=0.3):
    
    
    inputds  = ogr.Open(shp_in, 1)
    inputlyr = inputds.GetLayer()

    for feature in inputlyr:
        ingeom = feature.GetGeometryRef()#.Clone()
        try:
            ingeom_simpilfy = ingeom.SimplifyPreserveTopology(smooth_val)
            feature.SetGeometry(ingeom_simpilfy)
            inputlyr.SetFeature(feature)
        except:
            print (f'err in FID: { feature.GetFID() }' )

    inputds.SyncToDisk()
    inputds.Destroy()
    return shp_in



def shp_2_VRT(shp_in):

    #this will not work as it is virtual function and canot be omitted outside a function sapce
    print ('creating virtual layer (in-memory)')
    VRT_Driver  = ogr.GetDriverByName('MEMORY')
    shp_Driver  = ogr.GetDriverByName("ESRI Shapefile")
    shp_in      = shp_Driver.Open(shp_in)
    VRT_out     = VRT_Driver.CreateDataSource('VRT')
    VRT_out     = VRT_out.CopyLayer(shp_in.GetLayer()  ,'VRT')
    print ('returning virtual layer ')
    return VRT_out







#############################################################################################################################

############################################  Additional functions  #########################################################




def raster_copy_with_nodata( s_fh, s_xoff, s_yoff, s_xsize, s_ysize, s_band_n,
                             t_fh, t_xoff, t_yoff, t_xsize, t_ysize, t_band_n,
                             nodata ):
    try:
        import numpy as Numeric
    except ImportError:
        import Numeric
    
    if verbose != 0:
        print('Copy %d,%d,%d,%d to %d,%d,%d,%d.' \
              % (s_xoff, s_yoff, s_xsize, s_ysize,
             t_xoff, t_yoff, t_xsize, t_ysize ))

    s_band = s_fh.GetRasterBand( s_band_n )
    t_band = t_fh.GetRasterBand( t_band_n )

    data_src = s_band.ReadAsArray( s_xoff, s_yoff, s_xsize, s_ysize,
                                   t_xsize, t_ysize )
    data_dst = t_band.ReadAsArray( t_xoff, t_yoff, t_xsize, t_ysize )
    nodata_test = Numeric.equal(data_src,nodata)
    to_write = Numeric.choose( nodata_test, (data_src, data_dst) )                   
    t_band.WriteArray( to_write, t_xoff, t_yoff )
    return 0
    

def names_to_fileinfos( names ):  
    file_infos = []
    for name in names:
        fi = file_info()
        if fi.init_from_name( name ) == 1:
            file_infos.append( fi )

    return file_infos


class file_info:

    def init_from_name(self, filename):
        fh = gdal.Open( filename )
        if fh is None:
            return 0

        self.filename = filename
        self.bands = fh.RasterCount
        self.xsize = fh.RasterXSize
        self.ysize = fh.RasterYSize
        self.band_type = fh.GetRasterBand(1).DataType
        self.projection = fh.GetProjection()
        self.geotransform = fh.GetGeoTransform()
        self.ulx = self.geotransform[0]
        self.uly = self.geotransform[3]
        self.lrx = self.ulx + self.geotransform[1] * self.xsize
        self.lry = self.uly + self.geotransform[5] * self.ysize

        ct = fh.GetRasterBand(1).GetRasterColorTable()
        if ct is not None:
            self.ct = ct.Clone()
        else:
            self.ct = None
        return 1


    def report( self ):
        print('Filename: '+ self.filename)
        print('File Size: %dx%dx%d' \
              % (self.xsize, self.ysize, self.bands))
        print('Pixel Size: %f x %f' \
              % (self.geotransform[1],self.geotransform[5]))
        print('UL:(%f,%f)   LR:(%f,%f)' \
              % (self.ulx,self.uly,self.lrx,self.lry))

    def copy_into( self, t_fh, s_band = 1, t_band = 1, nodata_arg=None ):
        
        t_geotransform = t_fh.GetGeoTransform()
        t_ulx = t_geotransform[0]
        t_uly = t_geotransform[3]
        t_lrx = t_geotransform[0] + t_fh.RasterXSize * t_geotransform[1]
        t_lry = t_geotransform[3] + t_fh.RasterYSize * t_geotransform[5]

    
        tgw_ulx = max(t_ulx,self.ulx)
        tgw_lrx = min(t_lrx,self.lrx)
        if t_geotransform[5] < 0:
            tgw_uly = min(t_uly,self.uly)
            tgw_lry = max(t_lry,self.lry)
        else:
            tgw_uly = max(t_uly,self.uly)
            tgw_lry = min(t_lry,self.lry)
        
      
        if tgw_ulx >= tgw_lrx:
            return 1
        if t_geotransform[5] < 0 and tgw_uly <= tgw_lry:
            return 1
        if t_geotransform[5] > 0 and tgw_uly >= tgw_lry:
            return 1
            
        tw_xoff = int((tgw_ulx - t_geotransform[0]) / t_geotransform[1] + 0.1)
        tw_yoff = int((tgw_uly - t_geotransform[3]) / t_geotransform[5] + 0.1)
        tw_xsize = int((tgw_lrx - t_geotransform[0])/t_geotransform[1] + 0.5) \
                   - tw_xoff
        tw_ysize = int((tgw_lry - t_geotransform[3])/t_geotransform[5] + 0.5) \
                   - tw_yoff

        if tw_xsize < 1 or tw_ysize < 1:
            return 1

       
        sw_xoff = int((tgw_ulx - self.geotransform[0]) / self.geotransform[1])
        sw_yoff = int((tgw_uly - self.geotransform[3]) / self.geotransform[5])
        sw_xsize = int((tgw_lrx - self.geotransform[0]) \
                       / self.geotransform[1] + 0.5) - sw_xoff
        sw_ysize = int((tgw_lry - self.geotransform[3]) \
                       / self.geotransform[5] + 0.5) - sw_yoff

        if sw_xsize < 1 or sw_ysize < 1:
            return 1

        
        s_fh = gdal.Open( self.filename )

        return \
            raster_copy( s_fh, sw_xoff, sw_yoff, sw_xsize, sw_ysize, s_band,
                         t_fh, tw_xoff, tw_yoff, tw_xsize, tw_ysize, t_band,
                         nodata_arg )


def raster_copy( s_fh, s_xoff, s_yoff, s_xsize, s_ysize, s_band_n,
                 t_fh, t_xoff, t_yoff, t_xsize, t_ysize, t_band_n,
                 nodata=None ):

    if nodata is not None:
        return raster_copy_with_nodata(
            s_fh, s_xoff, s_yoff, s_xsize, s_ysize, s_band_n,
            t_fh, t_xoff, t_yoff, t_xsize, t_ysize, t_band_n,
            nodata )

    if verbose != 0:
        print('Copy %d,%d,%d,%d to %d,%d,%d,%d.' \
              % (s_xoff, s_yoff, s_xsize, s_ysize,
             t_xoff, t_yoff, t_xsize, t_ysize ))

    s_band = s_fh.GetRasterBand( s_band_n )
    t_band = t_fh.GetRasterBand( t_band_n )

    data = s_band.ReadRaster( s_xoff, s_yoff, s_xsize, s_ysize,
                              t_xsize, t_ysize, t_band.DataType )
    t_band.WriteRaster( t_xoff, t_yoff, t_xsize, t_ysize,
                        data, t_xsize, t_ysize, t_band.DataType )
        

    return 0




