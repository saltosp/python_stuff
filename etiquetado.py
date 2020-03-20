import io
import time
import os
import datetime
import logging
from pymongo import MongoClient
from bson.objectid import ObjectId
from model import PythiaDemo
from googletrans import Translator
import math


class GetConn(object):

    def __init__(self, host, username, password, authSource, coleccion_imagenes, coleccion_tweets):
        self.client = MongoClient(
            host=host, username=username, password=password,
            authSource='admin'
        )
        self.database = authSource
        self.db = self.client[authSource]
        self.coleccion_imagenes = self.db[coleccion_imagenes]
        self.coleccion_tweets = self.db[coleccion_tweets]

    def contar_imagenes_no_etiquetadas(self):
        return (self.coleccion_imagenes.find({
            'etiquetado': {"$exists": False}
        }).count())

    def obtener_imagenes_no_etiquetadas(self):
        return (i for i in self.coleccion_imagenes.find({
            'etiquetado': {"$exists": False}
        }).sort('_id', 1))

    def obtener_twwet_original(self, id_tweet):
        return self.coleccion_tweets.find_one({
            '_id': ObjectId(id_tweet)
        })

    def actualizar_tweet_original(self, id_original, nuevos_datos):
        self.coleccion_tweets.update_one({'_id': id_original}, {"$set": nuevos_datos})

    def actualizar_imagen(self, id_original, nuevos_datos):
        self.coleccion_imagenes.update_one({'_id': id_original}, {"$set": nuevos_datos})


if __name__ == "__main__":
    logging.basicConfig(filename='procesamiento.log', level=logging.INFO)
    # ubicacion_directorios = 'd:\\WorkFiles\\LINK\\Imagenes_tweeter\\'
    ubicacion_directorios = '/home/pablo/Desktop/imagenes_tweets/'
    ptia = PythiaDemo()
    '''connection = GetConn(
        "pruebas2.espaciolink.com", "scrap_PU", "9JPDs67uxSep8de",
        'scrap_scrap', 'virusImageHash', 'virusRevisar'
    )'''
    connection = GetConn(
        "ec2-54-234-246-199.compute-1.amazonaws.com", "covid", "1q2w3_e4r5t*+{}",
        'covid', 'virusImageHash', 'virusRevisar'
    )

    translator = Translator()
    cuantas = 0
    while True:
        cuantas += 1
        logging.info(f'Inicio vuelta {cuantas}: {datetime.datetime.now()}')
        # leer los no procesados de la coleccion mongo, que no tengan el campo etiquetado
        total_registros = connection.contar_imagenes_no_etiquetadas()

        contador = 0
        for la_imagen in connection.obtener_imagenes_no_etiquetadas():
            tokens = None
            contador += 1
            print(f'procesando {contador} de {total_registros}')
            # cojo el path
            imagen_path = la_imagen["path"]
            # imagen_path = '1583640000/5e39573cf6b274d43ddb0e52_1.jpg'
            id_coleccion_imagen = la_imagen['_id']
            # id_coleccion_imagen = ObjectId("5e6528c3e6ac72fd54894900")

            path_imagen_completo = ubicacion_directorios + imagen_path
            # leer si el archivo existe
            if os.path.isfile(path_imagen_completo):
                print('Archivo encontrado')
                etiqueta_ingles = ''
                etiqueta_espaniol = ''

                try:
                    tokens = ptia.predict(path_imagen_completo)
                    etiqueta = ptia.caption_processor(tokens.tolist()[0])["caption"]
                    # etiquetar imagen
                    # TODO:
                    etiqueta_ingles = etiqueta
                    etiqueta_espaniol = translator.translate(etiqueta, dest='es', src='en').text  # .encode('utf8')
                    print(path_imagen_completo)
                    print(etiqueta_ingles)
                    print(etiqueta_espaniol)
                except Exception as exception:
                    print(f'Error al obtener la etiqueta {exception}')
                id_mongo_tweet = ''
                item_etiqueta = {
                    'etiqueta_ingles': etiqueta_ingles,
                    'etiqueta_espaniol': etiqueta_espaniol,
                    'etiquetado': int(datetime.datetime.now().timestamp()),
                    'tweet_id': ''
                }
                if etiqueta_ingles is not "":
                    # busco el tweet original
                    pedazos = imagen_path.split('/')
                    if pedazos[1] is not None:
                        pedazos_1 = pedazos[1].split('_')
                        id_mongo_tweet = pedazos_1[0]

                if id_mongo_tweet != '':
                    # busco el tweet original
                    tweet_original = connection.obtener_twwet_original(id_mongo_tweet)
                    if tweet_original is not None:
                        # actualizar con la etiqueta, fecha de etiquetado
                        item_etiqueta['tweet_id'] = tweet_original['id_str']
                        connection.actualizar_tweet_original(tweet_original['_id'], item_etiqueta)
                        print("guardo tweet")

                if etiqueta_ingles is not "":
                    # en el registro de donde se leyo la imagen tambien se actualiza con la etiqueta y fecha de etiquetado
                    connection.actualizar_imagen(id_coleccion_imagen, item_etiqueta)
                    print("guardo etiqueta")
            else:
                item_etiqueta = {
                    'etiqueta_ingles': '',
                    'etiqueta_espaniol': '',
                    'etiquetado': int(datetime.datetime.now().timestamp()),
                    'tweet_id': '',
                    'error': 'File not found'
                }
                connection.actualizar_imagen(id_coleccion_imagen, item_etiqueta)

            if contador == 50000:
                break

        break
        time.sleep(3)
