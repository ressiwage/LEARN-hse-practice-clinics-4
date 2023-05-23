import osmnx as ox
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import os 
import geopandas as gpd
from shapely import wkt
import shapely

dir_path = os.path.dirname(os.path.realpath(__file__))

place, csv = 'Россия', 'Россия_utf16.csv'


def edit_csv(csv, osmid, specialities, name):
    df = pd.read_csv(csv, delimiter=",", encoding="utf-16", low_memory=False)
    df.loc[df['osmid'] == int(osmid), 'name'] = name
    df.loc[df['osmid'] == int(osmid), 'healthcare:speciality'] = specialities
    df.to_csv(csv, index=False,  encoding='utf-16')

def edit_sql(cnx, osmid, specialities, name):
    cursor = cnx.cursor()
    cursor.execute(f'UPDATE main.clinics SET name={name}, specialities={specialities} WHERE osmid={osmid}')
    print(f"UPDATE main.clinics SET name='{name}', specialities='{specialities}' WHERE osmid={osmid}")
    cnx.commit()
    cursor.close()

def mark_csv(csv, source, osmid):
    df = pd.read_csv(csv, delimiter=",", encoding="utf-16", low_memory=False)
    df2 = pd.read_csv(source, delimiter=",", encoding="utf-16", low_memory=False)
    row=df2.loc[df2['osmid'] == int(osmid)]
    df = df.append(row , ignore_index=True)
    df.to_csv(csv, index=False,  encoding='utf-16')

def mark_sql(cnx, osmid):
    df = sql_to_gpd(cnx, 'clinics')
    cursor = cnx.cursor()
    row=df.loc[df['osmid'] == int(osmid)]
    columns = ['osmid', 'element_type', 'amenity', 'latitude', 'longitude', 'geometry']
    q = f'INSERT INTO main.markers ({", ".join(columns)}) VALUES ({", ".join([row.iloc[0][c] if isinstance(row.iloc[0][c], int) else rf""" "{row.iloc[0][c]}" """ for c in columns])})'
    cursor.execute( q )
    cnx.commit()
    cursor.close() 

def mini_df(df):
    df_2 = df[['geometry', 'osmid', 'name', 'addr:street', 'addr:housenumber', 'contact:website', 'contact:phone', 'healthcare:speciality', 'latitude', 'longitude']]    
    return df_2

def create_csv(csv, osmid, kwargs):
    df = pd.read_csv(csv, delimiter=",", encoding="utf-16", low_memory=False)
    df = df.append({
        'element_type':'node', 
        'geometry':shapely.Point(kwargs["latitude-form"], 
                                 kwargs["longitude-form"]),
        'osmid':osmid, 
        'latitude':kwargs['latitude-form'], 
        'longitude':kwargs['longitude-form'], 
        'name': kwargs['name-form'], 
        'healthcare:speciality':kwargs['specialities-form']} , 
        ignore_index=True)
    df.to_csv(csv, index=False,  encoding='utf-16')

def create_sql(cnx, osmid, kwargs):
    cursor = cnx.cursor()
    q=f"""INSERT INTO main.clinics(element_type, geometry, osmid,
                                    latitude, longitude, name, `healthcare:speciality`
                ) VALUES(
                'node', '{str(shapely.Point(kwargs["latitude-form"], kwargs["longitude-form"]))}', {osmid}, {kwargs['latitude-form']}, {kwargs['longitude-form']},
                 '{kwargs['name-form']}', '{kwargs['specialities-form']}'); """
    cursor.execute(q)
    cnx.commit()

def sql_to_gpd(cnx, table):
    curs = cnx.cursor(dictionary=True)
    r_curs = cnx.cursor()
    q = f'SELECT * FROM main.{table};'
    curs.execute(q)
    df = pd.DataFrame.from_dict(curs.fetchall())
    if len(df.index)==0:
        r_curs.execute(f'SHOW COLUMNS FROM main.{table};')
        columns = r_curs.fetchall()
        df = pd.DataFrame(columns=[i[0] for i in columns])
    df['geometry'] = df['geometry'].apply(wkt.loads)
    gdf = gpd.GeoDataFrame(df, crs='epsg:4326')
    curs.close()
    return gdf

def handle_gdf(gdf):
    if 'latitude' not in gdf.columns:
        gdf['latitude'] = ''
    if 'longitude' not in gdf.columns: 
        gdf['longitude'] = ''
    
    for index, row in gdf.iterrows():
        gdf.at[index, 'latitude'] = gdf.iloc[index]['geometry'].coords[:][0][1]
        gdf.at[index, 'longitude'] = gdf.iloc[index]['geometry'].coords[:][0][0]
        if gdf.iloc[index]['element_type'] == 'way' or gdf.iloc[index]['element_type'] == 'relation':
            gdf.at[index, 'geometry'] = gdf.iloc[index]['geometry'].centroid
            gdf.at[index, 'element_type'] = 'node'
            gdf.at[index, 'nodes'] = ''
    return gdf
    


if __name__=='__main__':
    import mysql.connector as connector
    config = {'host':'45.95.202.187', 'port':8080, 'user':'root', 'password':'1048576power'}
    cnx = connector.connect(**config)
    gdf = sql_to_gpd(cnx)
    cnx.close()
    gdf = handle_gdf(gdf)
    gdf.to_csv(dir_path+'/'+csv, index=False, encoding='utf-16')
    
    
    mini_df(gdf).to_file(dir_path+'/'+'mini_russia.json', driver="GeoJSON")  
    
    area = ox.geocode_to_gdf(place)
    base = area.plot(color='white', edgecolor='black')
    gdf.plot(ax=base, color='red', figsize=5, alpha=0.02)
    


    plt.show()