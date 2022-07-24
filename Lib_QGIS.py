
# -*- coding: utf-8 -*-



from psycopg2 import extensions

from qgis.PyQt.QtCore import QSettings, QTranslator, QCoreApplication
from qgis.PyQt.QtGui import QIcon
from qgis.PyQt.QtWidgets import QAction,QMessageBox


from qgis.core import QgsVectorLayer,QgsFeatureRequest,QgsVectorFileWriter
import os
import pandas as pd
import processing

# Initialize Qt resources from file resources.py
from .resources import *
# Import the code for the dialog
from .combineData_dialog import Combine_DataDialog
import os.path

# Code examples

# CreateDBpostgreSQL
# Combine_Data
# findDXFS
# Create_mask_layer
# Tran_WGS_to_ISR



def CreateDBpostgreSQL(DB_NAME,user = 'postgres',password = '1515'):
    
    conn   = psycopg2.connect("dbname='' user={} password={}".format(user,password))

    autocommit = extensions.ISOLATION_LEVEL_AUTOCOMMIT
    conn.set_isolation_level( autocommit )

    cursor = conn.cursor()
    cursor.execute('CREATE DATABASE ' + str(DB_NAME))
    cursor.close()
    conn.close()
    print ('finish')

def connection(dbname,user = 'postgres',password = '1515'):
    conn   = psycopg2.connect("dbname={} user={} password={}".format(dbname,user,password))
    cursor = conn.cursor()
    return cursor,conn
    
def Connect_to_postgres(dbname,table,user = 'postgres',password = '1515'):
    
    cursor,conn = connection(dbname,user,password)
    cursor.execute("SELECT * from {}".format(table))

    record = cursor.fetchall()
    cursor.close()
    conn.close()
    return record


def List_to_SQLfields(schama):

    '''
    convert: [['id','bigint'],['title','varchar(128)'],['story','text'],['AGE','INT'],['INCOME','FLOAT']]
    to:      "(id bigint, title varchar(128), story text, AGE INT, SEX CHAR(1), INCOME FLOAT)"
    '''

    str_ = ''
    for i in schama: str_ = str_ + ', ' + ' '.join(n for n in i)
    return '(' + str_[2:] + ')'
    


def Check_if_table_exists(DBname,TBLname):
    cursor,conn = connection(DBname)
    cursor.execute("select exists(select * from information_schema.tables where table_name=%s)", (TBLname,))
    ans = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    print (ans)
    return ans
    

def InsertDataToTable(DBname,TBLname,data,fields_list):

    #data        = [[1, 'Hussein'],[2, 'mohamad']]
    #fields_list = ['id', 'title']

    fields_str  = ', '.join(i for i in fields_list)
    data_str    = [str(tuple(j)) for j in data]

    cursor,conn = connection(DBname)

    for i in data_str: cursor.execute("insert into "+ TBLname +" ("+fields_str+") values "+ i +"")

    cursor.execute("select {} from {}".format(fields_str, TBLname))
    rows = cursor.fetchall()
    for r in rows:
        print (f"id {r[0]} name {r[1]}")
    conn.commit()
    cursor.close()
    conn.close()

def Create_Tale(DBname,TBLname,schama):

    schama = List_to_SQLfields(schama)

    cursor,conn = connection(DBname)
    cursor.execute("select exists(select * from information_schema.tables where table_name=%s)", (TBLname,))
    ans = cursor.fetchone()[0]
    print(ans)
    if not ans:
        # cursor.execute("DROP TABLE IF EXISTS " +TBLname)
        cursor,conn = connection(DBname)
        sqlCreateTable = "create table "+TBLname+" {};".format(schama)
        cursor.execute(sqlCreateTable)
        conn.commit()
    cursor.close()
    conn.close()



#data = Connect_to_postgres(dbname = "Tel_aviv",table = "bshbldg")
#print (data)

#Check_if_table_exists("Tel_aviv","bshbldg")
#CreateDBpostgreSQL()

# schama = [['id','int'],['title','varchar(128)'],['story','text'],['AGE','INT'],['INCOME','FLOAT']]
# Create_Tale("Tel_aviv","NEWpythonaa")

# data        = [[1, 'Hussein'],[2, 'mohamad']]
# fields_list = ['id', 'title']
# InsertDataToTable("Tel_aviv","NEWpython",data,fields_list)



sql_stat = '''
select * 
from (
	select *, st_intersection(bshbldg.geom,tbl_stat1.geom)as geometry
	from bshbldg,(select * from bshstat where STAT_ID = 1) as tbl_stat1
	where st_intersects(bshbldg.geom,tbl_stat1.geom)
	) as newGeom
where geometry is not null
'''

cursor,conn = connection('Tel_aviv')

cursor.execute(sql_stat)
ans = cursor.fetchall()
for i in ans:
    print (i)


# sqlGetTableList = "SELECT table_schema,table_name FROM information_schema.tables where table_schema='test' ORDER BY table_schema,table_name ;"
# cursor.execute(sqlGetTableList)
# tables = cursor.fetchall()
# for table in tables:
#     print(table)

# cursor.close()
# conn.close()


##############################################################################################################################################


class Combine_Data:
    """QGIS Plugin Implementation."""

    def __init__(self, iface):

        # Save reference to the QGIS interface
        self.iface = iface
        # initialize plugin directory
        self.plugin_dir = os.path.dirname(__file__)
        # initialize locale
        locale = QSettings().value('locale/userLocale')[0:2]
        locale_path = os.path.join(
            self.plugin_dir,
            'i18n',
            'Combine_Data_{}.qm'.format(locale))

        if os.path.exists(locale_path):
            self.translator = QTranslator()
            self.translator.load(locale_path)
            QCoreApplication.installTranslator(self.translator)

        # Declare instance attributes
        self.actions = []
        self.menu = self.tr(u'&Combine data')

        # Check if plugin was started the first time in current QGIS session
        # Must be set in initGui() to survive plugin reloads
        self.first_start = None

    # noinspection PyMethodMayBeStatic
    def tr(self, message):

        # noinspection PyTypeChecker,PyArgumentList,PyCallByClass
        return QCoreApplication.translate('Combine_Data', message)


    def add_action(
        self,
        icon_path,
        text,
        callback,
        enabled_flag=True,
        add_to_menu=True,
        add_to_toolbar=True,
        status_tip=None,
        whats_this=None,
        parent=None):


        icon = QIcon(icon_path)
        action = QAction(icon, text, parent)
        action.triggered.connect(callback)
        action.setEnabled(enabled_flag)

        if status_tip is not None:
            action.setStatusTip(status_tip)

        if whats_this is not None:
            action.setWhatsThis(whats_this)

        if add_to_toolbar:
            # Adds plugin icon to Plugins toolbar
            self.iface.addToolBarIcon(action)

        if add_to_menu:
            self.iface.addPluginToMenu(
                self.menu,
                action)

        self.actions.append(action)

        return action

    def initGui(self):
        """Create the menu entries and toolbar icons inside the QGIS GUI."""

        icon_path = ':/plugins/combineData/icon.png'
        self.add_action(
            icon_path,
            text=self.tr(u''),
            callback=self.run,
            parent=self.iface.mainWindow())

        # will be set False in run()
        self.first_start = True


    def unload(self):
        """Removes the plugin menu item and icon from QGIS GUI."""
        for action in self.actions:
            self.iface.removePluginMenu(
                self.tr(u'&Combine data'),
                action)
            self.iface.removeToolBarIcon(action)



    def run(self):

        def select_by_id(self,layer,select_id):
            layer.select(select_id)

            selection = layer.selectedFeatures()

            for feat in selection:
                print (feat['GUSH_NUM'])
                
            return selection
            
        def handle_csv(csv_path):

            df      = pd.read_csv(csv_path,encoding = "utf-8")
            df      = df[['TALAR_NUM', 'TALAR_YEAR', 'GUSH_NUM', 'GUSH_SUFFIX','DESCRIPTION','GUSH_STATUS','LOCALITY_NAME','REG_MUN_NAME',"ORDERER"]]

            list_          = df["GUSH_NUM"].values.tolist()
            list_to_filter = ','.join(str(i) for i in list_)[0:-1]
            
            return df,list_to_filter


        def Filter_by_field_name(layer_path,list_to_filter,out_put):

            layer   = QgsVectorLayer(layer_path, "sub_gush_all", "ogr")
            layer.selectByExpression("\"GUSH_NUM\" in ("+list_to_filter+")")

            _writer      = QgsVectorFileWriter.writeAsVectorFormat(layer,out_put,"utf-8",layer.crs(),"ESRI Shapefile",onlySelected = True)
            
            return out_put
            
            
        def spatial_join(filter_path,mun_path,fnout = '',fields_to_copy = []):

            processing.run("native:joinbynearest",\
            {'INPUT':filter_path,\
            'INPUT_2':mun_path,\
            'FIELDS_TO_COPY':fields_to_copy,\
            'DISCARD_NONMATCHING':True,\
            'PREFIX':'',\
            'NEIGHBORS':1,\
            'MAX_DISTANCE':2,\
            'OUTPUT':fnout})

            data = Filter_data_from_layers(fnout)

            # alg_params = {
            #     'DISCARD_NONMATCHING': True,
            #     'INPUT': filter_path,
            #     'JOIN': mun_path,
            #     'JOIN_FIELDS': [''],
            #     'METHOD': 0,
            #     'PREDICATE': [0],
            #     'PREFIX': '',
            #     'OUTPUT': fnout
            # }

            # processing.run('native:joinattributesbylocation', alg_params)
            # data = Filter_data_from_layers(fnout)


            # filter_path = QgsVectorLayer(filter_path, "polygon", "ogr")
            # mun_path    = QgsVectorLayer(mun_path   , "polygon", "ogr")

            # data   = []
            # exists = []
            # for gush in filter_path.getFeatures():
            #     for muni in mun_path.getFeatures():
            #         geom = muni.geometry().intersection(gush.geometry())
            #         if geom.area() > 100:
            #             fields_str = str(gush['GUSH_NUM']) + str(gush['GUSH_SUFFI']) + str(gush['STATUS_TEX'])\
            #                        + str(muni["SETTEL_NAM"]) + str(muni["Sug_Muni"]) + str(muni['Muni_Heb'])
            #             if fields_str not in exists:
            #                 data.append([gush['GUSH_NUM'],gush['GUSH_SUFFI'],gush['STATUS_TEX'],\
            #                     muni["SETTEL_NAM"],muni["Sug_Muni"],muni['Muni_Heb']])
            #                 exists.append(fields_str)

            return data


        def createFolder(dic):
            try:
                if not os.path.exists(dic):
                    os.makedirs(dic)
            except OSError:
                print ("Error Create dic")
            return dic
            
        def Get_Name_to_None(REG_MUN_NA,REG_MUN_NAME):
            return REG_MUN_NA if REG_MUN_NA != u' ' else REG_MUN_NAME

        def select_by_location(muni,filter_,out_put):

            muni    = QgsVectorLayer(muni, "polygon", "ogr")
            filter_ = QgsVectorLayer(filter_, "polygon", "ogr")

            processing.run("native:selectbylocation", {'INPUT':muni,'PREDICATE':[0],'INTERSECT':filter_,'METHOD':0})
            writer = QgsVectorFileWriter.writeAsVectorFormat(muni, out_put, 'utf-8', \
            driverName='ESRI Shapefile', onlySelected=True)

            return out_put
            
            
        def dict_():
            dict_1 = {'TALAR_ID':'מספר_תלר','TALAR_NUM':'מספר_תצר','TALAR_YEAR':'שנת_תצר','GUSH_NUM':'גוש','MUNI_HEB':'גבולות_שיפוט',\
                    'REG_MUN_NA':'ועדה','SETL_NAME':'שם_ישוב','GUSH_SUFFIX':'תת_גוש','DESCRIPTION':'סטאטוס_תצר','STATUS_TEX':'סטאטוס_גוש',\
                    'SETTEL_NAM':'שם_ישוב','REG_MUN_NAME':'שם_אזור_מונציפלי','SETL_NAME':'שם_ישוב','NAME_HEB':'שם_בעברית','ORDERER':'מזמין_עבודה',\
                    'NAFA1':'נפה','Sug_Muni':'סוג_מוניציפאלי','FIRST_Nafa':'נפה','Muni_Heb':'גבולות_שיפוט'}
            return dict_1

        def Filter_data_from_layers(spatial_merge):
            merge_lyr   = QgsVectorLayer(spatial_merge, "filter_path", "ogr")

            merge_lyr.setProviderEncoding(u'C-1255')
            merge_lyr.dataProvider().setEncoding(u'C-1255')

            data    = []
            request = QgsFeatureRequest()
            request.setFilterExpression("\"GUSH_NUM\" in ("+list_to_filter+")")

            data = [[lyr['GUSH_NUM'],lyr['GUSH_SUFFI'],lyr["STATUS_TEX"],\
                    lyr["SETTEL_NAM"],lyr["Sug_Muni"],lyr["Muni_Heb"]]\
                    for lyr in merge_lyr.getFeatures(request)]
            return data


        """Run method that performs all the real work"""

        if self.first_start == True:
            self.first_start = False
            self.dlg = Combine_DataDialog()


        self.dlg.show()
        result = self.dlg.exec_()
        if result:

            # layer_path     = r"C:\Users\Administrator\Desktop\medad\python\Work\for_dariel\Data\sub_gush_all.shp"
            # mun_path       = r"C:\Users\Administrator\Desktop\medad\python\Work\for_dariel\Data\muni_il.shp"
            # csv_path       = r"C:\Users\Administrator\Desktop\medad\python\Work\for_dariel\files\2021-07.csv"

            lyr_gush   = self.dlg.btn_gush.currentLayer()
            layer_path = str(lyr_gush.dataProvider().dataSourceUri())

            lyr_muni   = self.dlg.btn_muni.currentLayer()
            mun_path   = str(lyr_muni.dataProvider().dataSourceUri())

            lyr_csv    = self.dlg.btn_csv.currentLayer()
            csv_path   = str(lyr_csv.dataProvider().dataSourceUri())

            csv_date   = os.path.basename(csv_path)
            folder     = createFolder(r'C:\temp')

            filter_path   = folder + '\\' + 'filter_gush4.shp'
            muni_by_loc   = folder + '\\' + 'muni_by_loc.shp'
            spatial_merge = folder + '\\' + 'spatial_merge4.shp'


            out_put = self.dlg.lineEdit.text()
            if not out_put: 
                out_put       = folder + '\\' + 'Result_' + csv_date


            #QMessageBox.information(self.dlg, "Message", str(lyr_gush.sourceName()))
            QMessageBox.information(self.dlg, "Message", str(lyr_gush.dataProvider().dataSourceUri()))

            df,list_to_filter = handle_csv(csv_path)

            Filter_by_field_name (layer_path,list_to_filter,filter_path)
            select_by_location   (mun_path,filter_path,muni_by_loc)

            fields_to_keep = ["Sug_Muni","Muni_Heb",'SETTEL_NAM']
            data           = spatial_join        (filter_path,muni_by_loc,spatial_merge,fields_to_keep)

            dict_1               = dict_()
            df_shp               = pd.DataFrame(data = data, columns = ["GUSH_NUM","GUSH_SUFFIX","STATUS_TEX","SETTEL_NAM","Sug_Muni","Muni_Heb"])

            df['GUSH_SUFFIX']     = df    ['GUSH_SUFFIX'].fillna(value=0)
            df_shp['GUSH_SUFFIX'] = df_shp['GUSH_SUFFIX'].fillna(value=0)

            result                = df.merge(df_shp, how = 'left', left_on=['GUSH_NUM','GUSH_SUFFIX'], right_on=['GUSH_NUM','GUSH_SUFFIX'])
            result                = result.rename(columns=dict_1)
            result                = result.drop_duplicates()

            result.to_csv            (out_put,encoding='ISO-8859-8')
                

            pass




##############################################################################################################################################

##############################################################################################################################################


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


def Cheak_if_exists(dxf_path,value_to_check,add_to_map = True):
        dxf_output_filename = os.path.splitext(os.path.basename(dxf_path))[0]
        dxf_vl = QgsVectorLayer(dxf_path, dxf_output_filename+'_temp', 'ogr')
        if add_to_map:
            if dxf_vl.isValid() == True:
                    registry.addMapLayer(dxf_vl)


        layer_check = []
        for feature in dxf_vl.getFeatures():
                if feature['Layer'] == value_to_check:
                        layer_check.append(feature['Layer'])

        print (len(layer_check))
        return len(layer_check)




def Create_Mask(vl,in_put_layer,data_list = []):

    pr      = vl.dataProvider()
    filter_ = QgsVectorLayer(in_put_layer, "polygon", "ogr")

    e = filter_.extent()

    x_max = e.xMaximum()
    x_min = e.xMinimum()
    y_min = e.yMinimum()
    y_max = e.yMaximum()

    fet   = QgsFeature()

    coords  = [(x_min, y_min), (x_min, y_max), (x_max, y_max), (x_max, y_min), (x_min, y_min)]   
    polygon = QgsGeometry.fromPolygonXY( [[ QgsPointXY( pair[0], pair[1] ) for pair in coords ]] ) 
    fet.setGeometry(polygon)
    fet.setAttributes(data_list)
    pr.addFeatures([fet])

    vl.updateExtents()
    
    
def Get_list_of_layers(Folder,ends_with):
    list_shp = []
    for root, dirs, files in os.walk(Folder):
        for file in files:
            if file.endswith(ends_with):
                list_shp.append(root + '\\' +file)
    return list_shp
    
    
def Create_mask_layer():
    vl = QgsVectorLayer(str('Polygon?crs='+'2039'), "polygon", "memory")
    pr      = vl.dataProvider()
    # add fields
    pr.addAttributes([QgsField("name", QVariant.String),\
                      QgsField("PATH", QVariant.String),\
                      QgsField("SIZE", QVariant.String),\
                      QgsField("COUNT", QVariant.String),\
                      QgsField("ID",  QVariant.Int)])
    vl.updateFields() # tell the vector layer to fetch changes from the provid
    return vl
#  #  #    S T A R T   #  #  #

def findDXFS(Folder):

    dxfs         = Get_list_of_layers(Folder,'.dxf')
    vl           = Create_mask_layer()

    ID = 1
    for dxf_ in dxfs:
        print (dxf_)
        COUNT        = Cheak_if_exists(dxf_,'M2200',False)
        dxf          = dxf_ + '|layername=entities|geometrytype=Line'
        data_list    = [dxf,dxf_,file_size(dxf_),COUNT,ID]
        Create_Mask(vl,dxf,data_list)
        
        ID += 1



    QgsProject.instance().addMapLayer(vl)





##############################################################################################################################################

##############################################################################################################################################



def getCrsNumber(layerPath):
    layer = QgsVectorLayer(layerPath, "polygon", "ogr")
    lyrCRS = layer.crs().authid()


    return ''.join([i for i in lyrCRS if i.isdigit()])

def Create_Mask(vl,in_put_layer,data_list = []):

    pr      = vl.dataProvider()
    filter_ = QgsVectorLayer(in_put_layer, "polygon", "ogr")

    e = filter_.extent()

    x_max = e.xMaximum()
    x_min = e.xMinimum()
    y_min = e.yMinimum()
    y_max = e.yMaximum()

    fet   = QgsFeature()

    coords  = [(math.ceil(x_min), math.ceil(y_min)), (math.floor(x_min), math.floor(y_max)),\
               (math.ceil(x_max), math.ceil(y_max)), (math.floor(x_max), math.floor(y_min)),\
               (math.ceil(x_min), math.ceil(y_min))]
               
    print (coords) 

    polygon = QgsGeometry.fromPolygonXY( [[ QgsPointXY( pair[0], pair[1] ) for pair in coords ]] ) 
    fet.setGeometry(polygon)
    fet.setAttributes(data_list)
    pr.addFeatures([fet])

    vl.updateExtents()
    
    
def Create_mask_layer(layerCRS):
    Crs = getCrsNumber(layerCRS)
    vl = QgsVectorLayer(str('Polygon?crs='+'{}'.format(Crs)), "polygon", "memory")
    pr      = vl.dataProvider()
    # add fields
    pr.addAttributes([QgsField("name", QVariant.String)])
    vl.updateFields() # tell the vector layer to fetch changes from the provid
    return vl
#  #  #    S T A R T   #  #  #


#################  INPUT  #####################
# shps         = Get_fcs_shps(Folder)
shp  = r'C:\temp\point.shp'

################  Analysis  #########################
data_list    = [shp]
vl           = Create_mask_layer(shp)
Create_Mask(vl,shp,data_list)


QgsProject.instance().addMapLayer(vl)




##############################################################################################################################################

##############################################################################################################################################



def Tran_WGS_to_ISR(longitude,latitude):

    def degreesToRadians(degrees):
        return degrees * math.pi / 180
    def pow2(x):
        return pow(x, 2)
    def pow3(x):
        return pow(x, 3)
    def pow4(x):
        return pow(x, 4)

    longitude       = degreesToRadians(longitude)
    latitude        = degreesToRadians(latitude)
    centralMeridian = degreesToRadians(35.2045169444444);  # central meridian of ITM projection
    k0              = 1.0000067;  # scale factor

    # Ellipsoid constants (WGS 80 datum)

    a    = 6378137      #equatorial radius
    b    = 6356752.3141 # polar radius
    e    = math.sqrt(1 - b*b/a/a);  ## eccentricity
    e1sq = e*e/(1-e*e)
    n    = (a-b)/(a+b)

    tmp = e*math.sin(latitude)
    nu  = a/math.sqrt(1 - tmp*tmp)

    ## Meridional arc length

    n3 = pow3(n)
    n4 = pow4(n)

    A0 = a*(1-n+(5*n*n/4)*(1-n) +(81*n4/64)*(1-n))
    B0 = (3*a*n/2)*(1 - n - (7*n*n/8)*(1-n) + 55*n4/64)
    C0 = (15*a*n*n/16)*(1 - n +(3*n*n/4)*(1-n))
    D0 = (35*a*n3/48)*(1 - n + 11*n*n/16)
    E0 = (315*a*n4/51)*(1-n)

    S = A0*latitude - B0*math.sin(2*latitude) + C0*math.sin(4*latitude)- D0*math.sin(6*latitude) + E0*math.sin(8*latitude);

    ## Coefficients for ITM coordinates

    p    = longitude-centralMeridian
    Ki   = S*k0
    Kii  = nu*math.sin(latitude)*math.cos(latitude)*k0/2
    Kiii = ((nu*math.sin(latitude)*pow3(math.cos(latitude)))/24)*(5-pow2(math.tan(latitude))+9*e1sq*pow2(math.cos(latitude))+4*e1sq*e1sq*pow4(math.cos(latitude)))*k0;
    Kiv  = nu*math.cos(latitude)*k0
    Kv   = pow3(math.cos(latitude))*(nu/6)*(1-pow2(math.tan(latitude))+e1sq*pow2(math.cos(latitude)))*k0;

    easting  = round(219529.58+ Kiv*p+Kv*pow3(p) - 60)
    northing = round(Ki+Kii*p*p+Kiii*pow4(p) - 3512424.41+ 626907.39 - 45)

    return easting,northing


def Add_fields(vr,fields,type_ = 'String'):

    if type_ == 'String':
        type_ = QVariant.String
    else:
        type_ = QVariant.Double

    # name    = os.path.basename(path)
    # layer   = QgsVectorLayer(path, name, "ogr")
    caps    = vr.dataProvider().capabilities()

    for field in fields:
        if caps & QgsVectorDataProvider.AddAttributes:
            vr.dataProvider().addAttributes([QgsField(field, type_)])
            vr.updateFields()


def Update_layer(vr,id_object,field_,new_value):
    # layer  = QgsVectorLayer(path, 'layerMe', "ogr")
    pr     = vr.dataProvider()

    flds   = vr.fields()
    if vr.featureCount():
        attrs = {flds.indexOf(field_):new_value}
        pr.changeAttributeValues({id_object:attrs})


def Get_attribute(path):
    layer  = QgsVectorLayer(path, 'layerMe', "ogr")
    flds   = layer.fields().names()
    data   = []

    for field in flds:
        num = 1
        for lyr in layer.getFeatures():
            data.append([num,field, lyr.attribute(field)])
            num+=1
    return data


def Get_fields_name_type(layer):
    name_type = []
    flds      = layer.fields()
    for fld in flds:name_type.append([fld.name(),fld.typeName()])
    return name_type


def WGS_ISR_polygon(path):

    layer            = QgsVectorLayer(path, "polyLayer", "ogr")
    name_type_fields = Get_fields_name_type(layer)

    vl    = QgsVectorLayer(str('Polygon?crs='+'2039'), "polygon", "memory")
    for feature in layer.getFeatures():
        print (feature)
        geometry = feature.geometry().asQPolygonF()
        new_list = []
        for i in geometry:
            x,y = Tran_WGS_to_ISR(i.x(),i.y())
            point = QgsPointXY(x,y)
            new_list.append(point)

        polygon = QgsGeometry.fromPolygonXY([new_list])
        pr      = vl.dataProvider()
        fet     = QgsFeature()
        fet.setGeometry(polygon)
        #fet.setAttributes(data_list)
        pr.addFeatures([fet])

    vl.updateExtents()
        
    QgsProject.instance().addMapLayer(vl)


    for name_type in name_type_fields:Add_fields(vl,[name_type[0]],type_ = name_type[1])
    data = Get_attribute(path)
    for i in data: Update_layer(vl,i[0],i[1],i[2])


def WGS_ISR_point(path):

    layer            = QgsVectorLayer(path, "PointLayer", "ogr")
    name_type_fields = Get_fields_name_type(layer)

    vl    = QgsVectorLayer(str('Point?crs='+'2039'), "point", "memory")
    for feature in layer.getFeatures():
        geometry = feature.geometry()
        point    = geometry.asPoint()
        x,y      = Tran_WGS_to_ISR(point.x(),point.y())
        geom_pnt = QgsPointXY(x,y)

        point_g = QgsGeometry.fromPointXY(geom_pnt)

        pr      = vl.dataProvider()
        fet     = QgsFeature()

        fet.setGeometry(point_g)
        #fet.setAttributes(data_list)
        pr.addFeatures([fet])
    vl.updateExtents()
    QgsProject.instance().addMapLayer(vl)


    for name_type in name_type_fields:Add_fields(vl,[name_type[0]],type_ = name_type[1])
    data = Get_attribute(path)
    for i in data: Update_layer(vl,i[0],i[1],i[2])
        


def WGS_ISR_Line(path_line):
    
    layer  = QgsVectorLayer(path_line, "Cut_gush_", "ogr")
    vl     = QgsVectorLayer(str('LineString?crs='+'2039'), "polyline", "memory")

    for feature in layer.getFeatures():

        pr      = vl.dataProvider()
        fet     = QgsFeature()

        geometry = feature.geometry().asMultiPolyline ()
        
        for i in geometry:
            new_list = []
            for j in i:
                x,y = Tran_WGS_to_ISR(j.x(),j.y())
                point = QgsPoint(x,y)
                new_list.append(point)
            print (len(new_list))

        line    = QgsGeometry.fromPolyline(new_list)
        fet.setGeometry(line)
        #fet.setAttributes(data_list)
        pr.addFeatures([fet])

    vl.updateExtents()
        
    QgsProject.instance().addMapLayer(vl)


    name_type_fields = Get_fields_name_type(layer)
    for name_type in name_type_fields:Add_fields(vl,[name_type[0]],type_ = name_type[1])
    data = Get_attribute(path)
    for i in data: Update_layer(vl,i[0],i[1],i[2])


def Main_WGS_TO_ISR(path):

    layer            = QgsVectorLayer(path, "checkType", "ogr")
    typeLayer = layer.wkbType()
    print (typeLayer)

    if typeLayer > 1:
        WGS_ISR_polygon (path)
    elif typeLayer == 1:
        WGS_ISR_point   (path)
    else:
        WGS_ISR_Line    (path)




path = r'C:\temp\building.shp'

Main_WGS_TO_ISR (path)
