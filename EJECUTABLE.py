import download
import connect
from concurrent.futures import process
from logging_config import logging as log
from utils import *
from SCRIPTS import descargar_csv, modificar_datos, cargar_tablas
from URLS import BIBLIOTECAS, MUSEOS, CINES


log.info('Descargando archivos fuente')

descargar_csv(BIBLIOTECAS, 'bibliotecas')
descargar_csv(MUSEOS, 'museos')
descargar_csv(CINES, 'cines')

log.info('Procesando datos y creando tablas')
modificar_datos()

log.info('Conectando con base de datos y subiendo tablas')
cargar_tablas()

log.info('Ejecución finalizada con éxito')