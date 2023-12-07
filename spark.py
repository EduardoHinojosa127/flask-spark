from pyspark.sql import SparkSession
from queue import Queue
import threading
import numpy as np
import json

class SparkLogic:
    def __init__(self, data_queue):
        self.data_queue = data_queue
        self.nearest_neighbor_info = None
        self.spark = SparkSession.builder \
            .master("spark://44.216.72.16:7077") \
            .appName("MiApp") \
            .config("spark.executor.memory", "2g") \
            .config("spark.executor.cores", "2") \
            .getOrCreate()
        self.spark_thread = threading.Thread(target=self.run_spark_logic)
        self.spark_thread.daemon = True
        self.spark_thread.start()

    def read_csv(self, file_path):
        df = self.spark.read.csv(file_path, header=True, inferSchema=True)
        return df
    
    def convert_user_data(self, user_data):
        user_ratings = [int(user_data[f'pelicula{i}']) for i in range(1, 6)]
        return user_ratings

    def calculate_nearest_neighbor(self, user_ratings, data):
        min_distance = float('inf')
        nearest_neighbor = None

        for row in data.collect():
            other_user_ratings = [row[f'pelicula{i}'] for i in range(1, 6)]
            distance = np.linalg.norm(np.array(user_ratings) - np.array(other_user_ratings))

            if distance < min_distance:
                min_distance = distance
                nearest_neighbor = (row['usuario'], distance)
                vecino_peliculas = dict(zip(data.columns[-5:], other_user_ratings))

        # Encontrar la película con la calificación más alta entre las últimas 5 películas
        pelicula_recomendada = min(vecino_peliculas, key=vecino_peliculas.get)

        return nearest_neighbor, pelicula_recomendada


    def run_spark_logic(self):
        archivo_csv = "data.csv"  # Reemplaza con la ruta correcta
        data = self.read_csv(archivo_csv)

        while True:
            if not self.data_queue.empty():
                user_data = self.data_queue.get()

                if isinstance(user_data, str):
                    try:
                        user_data = json.loads(user_data)
                    except json.JSONDecodeError:
                        print("Error al decodificar los datos JSON.")
                        continue

                user_ratings = self.convert_user_data(user_data)

                nearest_neighbor = self.calculate_nearest_neighbor(user_ratings, data)

                self.nearest_neighbor_info = nearest_neighbor

                print("Data from Flask:", user_data)
                print("Usuario más cercano:", nearest_neighbor)
                print("Datos han llegado a Spark")

    def get_nearest_neighbor_info(self):
        return self.nearest_neighbor_info

if __name__ == "__main__":
    data_queue = Queue()
    spark_logic = SparkLogic(data_queue)
    while True:
        pass
