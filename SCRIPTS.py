### Librerias necesarias:

#pip install requests
import csv as csv
import pandas as pd
import numpy as np
#pip install logging
#pip install logging_config
import os 
os.system('pip install psycopg2')
import requests
from logging.config import logging
import logging as log
from datetime import date
#pip install sqlalchemy
#pip install python-decouple
#pip install psycopg2-binary
import sqlalchemy
from sqlalchemy import create_engine, Date, Integer, String
import decouple 
from decouple import config
#pip install config
from config import Config
#pip install utils
from utils import *

# Importamos las variables necesarias

from URLS import *
from env.py import *


# Guarda fecha de descarga de csv
hoy = date.today()

def descargar_csv(url, categoria):
    '''
    Descarga el csv desde la url, guarda en la ruta de archivo indicada a su categoría y fecha
    '''
    try:
        # Nombro la carpeta de almacenamiento
        carpeta = categoria + '/' + str(hoy.year) + '-' + str(hoy.month)
        
        # Si no existe la creo
        if not os.path.exists(carpeta):
                os.makedirs(carpeta)
        
        # Creo la ruta deseada de archivo 
        carpetafinal = carpeta + '/' + categoria + '-' + str(hoy.day) + '-' + str(hoy.month) + '-' + str(hoy.year) + '.csv'

        # Descargo el archivo y lo guardo
        req = requests.get(url)
        url_contenido = req.content
        csv_file = open(carpetafinal, 'wb')
        csv_file.write(url_contenido)
        csv_file.close()

        log.info('Se realizó la descarga de los archivos correctamente')
        
    except Exception as e:
        log.error('No se pudo completar la descarga de los archivos correctamente')
        print(f'Error al realizar la descarga: {e}')

if __name__ == "__main__":
    descargar_csv(BIBLIOTECAS, 'bibliotecas')
    descargar_csv(MUSEOS, 'museos')
    descargar_csv(CINES, 'cines')


    
# Almaceno la fecha de descarga de los archivos
hoy = date.today()

def modificar_datos():
    '''
    Normaliza toda la información de creando una tabla con todos los datos juntos.
    Procesa la tabla de datos conjuntos y crea una tabla con la cantidad de registros por categoría, fuente y provincia-categoría.
    Procesa la información de cines y crea una tabla con la cantidad de butacas, pantallas y espacios INCAA por provincia.
    '''
    try:
        # Cargo los datos de bibliotecas
        carpeta = 'bibliotecas' + '/' + str(hoy.year) + '-' + str(hoy.month)
        df_bibliotecas = pd.read_csv(carpeta + '/' + 'bibliotecas' + '-' + str(hoy.day) + '-' + str(hoy.month) + '-' + str(hoy.year) + '.csv', encoding='UTF-8')

        # Me quedo con las columnas de necesarias
        df_bibliotecas = df_bibliotecas[['Cod_Loc', 'IdProvincia', 'IdDepartamento', 'Categoría', 'Provincia', 'Localidad',
                                        'Nombre', 'Domicilio', 'CP', 'Teléfono', 'Mail', 'Web', 'Fuente']]

        # Nombro las columnas
        df_bibliotecas.rename(
                            columns={'Cod_Loc':'cod_localidad', 'IdProvincia':'id_provincia', 'IdDepartamento':'id_departamento', 'Categoría':'categoría',
                            'Provincia':'provincia', 'Localidad':'localidad', 'Nombre':'nombre', 'Domicilio':'domicilio', 'CP':'código postal',
                            'Teléfono':'número de teléfono', 'Mail':'mail', 'Web':'web', 'Fuente':'fuente'}, inplace=True)

        # Cargo los datos de salas de cine
        carpeta = 'cines' + '/' + str(hoy.year) + '-' + str(hoy.month)
        df_cines = pd.read_csv(carpeta + '/' + 'cines' + '-' + str(hoy.day) + '-' + str(hoy.month) + '-' + str(hoy.year) + '.csv', encoding='UTF-8')

        # Creo un dataframe con la información a necesitar 
        df_salas_de_cine = df_cines[['Provincia', 'Pantallas', 'Butacas', 'espacio_INCAA']].copy()

        df_salas_de_cine.rename(columns={'Provincia':'provincia', 'Pantallas':'pantallas', 'Butacas':'butacas'}, inplace=True)
 
        # Los valores que dicen 0 los tomo como null porque no son espacio INCAA.
        df_salas_de_cine.replace('0', np.nan, inplace=True)

        # Proceso los datos para generar la tercera tabla
        df_salas_de_cine = df_salas_de_cine.groupby('provincia').aggregate({'pantallas': 'sum', 'butacas':'sum', 'espacio_INCAA':'count'}).reset_index()

        #Agrego la fecha de carga
        df_salas_de_cine = df_salas_de_cine.assign(fecha_carga=hoy)

        # Lo guardo en un nuevo csv
        df_salas_de_cine.to_csv('df_cines.csv',index=False, encoding='UTF-8')

        # Me quedo con las columnas de interés
        df_cines = df_cines[['Cod_Loc', 'IdProvincia', 'IdDepartamento', 'Categoría', 'Provincia', 'Localidad',
                                        'Nombre', 'Dirección', 'CP', 'Teléfono', 'Mail', 'Web', 'Fuente']]

        # Nombro las columnas
        df_cines.rename(
                        columns={'Cod_Loc':'cod_localidad', 'IdProvincia':'id_provincia', 'IdDepartamento':'id_departamento', 'Categoría':'categoría',
                        'Provincia':'provincia', 'Localidad':'localidad', 'Nombre':'nombre', 'Dirección':'domicilio', 'CP':'código postal',
                        'Teléfono':'número de teléfono', 'Mail':'mail', 'Web':'web', 'Fuente':'fuente'}, inplace=True)


        # Cargo los datos de museos
        carpeta = 'museos' + '/' + str(hoy.year) + '-' + str(hoy.month)
        df_museos = pd.read_csv(carpeta + '/' + 'museos' + '-' + str(hoy.day) + '-' + str(hoy.month) + '-' + str(hoy.year) + '.csv', encoding='UTF-8')

        # Me quedo con las columnas necesarias
        df_museos = df_museos[['Cod_Loc', 'IdProvincia', 'IdDepartamento', 'categoria', 'provincia', 'localidad',
                                'nombre', 'direccion', 'CP', 'telefono', 'Mail', 'Web', 'fuente']]

        # Nombro las columnas
        df_museos.rename(
                        columns={'Cod_Loc':'cod_localidad', 'IdProvincia':'id_provincia', 'IdDepartamento':'id_departamento', 'dirección':'domicilio',
                        'categoria':'categoría', 'CP':'código postal','telefono':'número de teléfono', 'Mail':'mail', 'Web':'web'}, inplace=True)

        # Creo un dataframe con la información conjunta que acabo de procesar
        df_unido = pd.concat([df_bibliotecas, df_cines, df_museos])

        # Agrego la columna correspondiente a la fecha de carga 
        df_unido = df_unido.assign(fecha_carga=hoy)

        # Reemplazo los valores sin datos ("s/d") por null
        df_unido = df_unido.replace('s/d', np.nan)

        # Proceso los datos conjuntos para generar la segunda tabla  
        df_categoria = df_unido.value_counts('categoría').reset_index(name='total por categoría')
        df_fuente = df_unido.value_counts('fuente').reset_index(name='total por fuente')
        df_provincia = df_unido.value_counts(['categoría', 'provincia']).reset_index(name='total por provincia y categoría')
        df_provincia.insert(0, 'provincia y categoría', df_provincia['provincia'] + "/" + df_provincia['categoría'])
        df_provincia.drop(['categoría', 'provincia'], axis=1, inplace=True)

        # Genero la segunda tabla 
        df_registros = pd.concat([df_categoria, df_fuente, df_provincia], axis=1)

        # Agrego la fecha de carga
        df_registros = df_registros.assign(fecha_carga=hoy)

        # Guardo en un nuevo csv 
        df_registros.to_csv('df_cantidad_registros.csv', index=False, encoding='UTF-8')

        # Elimino la columna fuente ya que solo la agregué para generar la segunda tabla
        df_unido.drop('fuente', axis=1, inplace=True)

        # Guardo en un nuevo csv los datos conjuntos
        df_unido.to_csv('df_conjunto.csv', index=False, encoding='UTF-8')

        log.info('Los datos fueron procesados con éxito')

    except Exception as e:
        log.error('No pudieron procesarse correctamente los datos')
        print(f'Error al procesar los datos: {e}')

if __name__ == '__main__':
    modificar_datos()


    
# Conectamos con postgres SQL
engine = create_engine('postgresql://' + DB_USER + ':' + DB_PASSWORD + '@' + DB_HOST + ':' + DB_PORT + '/'+ DB_NAME)


def cargar_tablas():
    '''
    Conecta con la base de datos y la actualiza con las tablas creadas
    '''
    try:
        # Obtengo la tabla conjunta
        df_conjunta = pd.read_csv('df_conjunto.csv')

        # Subo la tabla a la base de datos
        df_conjunta.to_sql('datos_conjuntos', con=engine, if_exists='replace', index=False, dtype={
            'cod_localidad':String, 'id_provincia':String, 'id_departamento':String, 'categoria':String,
            'provincia':String, 'localidad':String, 'nombre':String, 'domicilio':String, 'código postal':String,
            'número de teléfono':String, 'mail':String, 'web':String, 'fecha_carga':Date})

        # Obtengo la tabla de cantidad de registros
        df_registros = pd.read_csv('df_cantidad_registros.csv')

        # Subo la tabla a la base de datos
        df_registros.to_sql('cantidad_registros', con=engine, if_exists='replace', index=False, dtype={
            'categoría':String, 'total por categoría':Integer, 'fuente':String, 'total por fuente':Integer, 
            'provincia':String, 'categorías por provincia':Integer, 'fecha_carga':Date})

        # Obtengo la tabla de salas de cine
        df_cines = pd.read_csv('df_cines.csv')

        # Subo la tabla a la base de datos
        df_cines.to_sql('info_cines', con=engine, if_exists='replace', index=False, dtype={
            'provincia':String, 'pantallas':Integer, 'butacas':Integer, 'espacios_INCAA':Integer, 'fecha_carga':Date})
        
        log.info('Se actualizó la base de datos con éxito')

    except Exception as e:
        log.error('No se pudieron subir las tablas')
        print(f'Error al subir las tablas: {e}')

if __name__ == '__main__':
    cargar_tablas()


# Configuración para crear logs
log.basicConfig(level=log.INFO,
                filename='debug.log',
                filemode= 'w',
                format='%(asctime)s: %(levelname)s [%(filename)s:%(lineno)s] %(message)s',
                )
