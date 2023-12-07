from pyspark.sql import SparkSession
from queue import Queue
import threading
import numpy as np
import json

class SparkLogic:
    def __init__(self, data_queue):
        self.data_queue = data_queue
        self.nearest_neighbor_info = None
        self.pelicula_recomendada_info = None
        self.spark_running = True
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
            other_user_ratings = [row[f'pelicula{i}'] for i in range(1, 6)]  # Obtener las primeras 5 películas
            distance = np.linalg.norm(np.array(user_ratings) - np.array(other_user_ratings))

            if distance < min_distance:
                min_distance = distance
                nearest_neighbor = row['usuario']

        if nearest_neighbor:

            # Obtener todas las películas del vecino más cercano
            all_movies = self.get_all_movies_of_neighbor(nearest_neighbor, data)
            print(f"Todas las películas del vecino más cercano ({nearest_neighbor}):")
            for pelicula, calificacion in all_movies.items():
                print(f"{pelicula}: {calificacion}")

            # Encontrar la película con la calificación más alta entre las últimas 5 películas
            ultimas_5_peliculas = {f'pelicula{i}': all_movies[f'pelicula{i}'] for i in range(6, 11)}
            pelicula_recomendada = max(ultimas_5_peliculas, key=ultimas_5_peliculas.get)
            self.pelicula_recomendada_info = pelicula_recomendada
            print(f"Película recomendada de las últimas 5: {pelicula_recomendada}")

            return nearest_neighbor
        else:
            return "No hay vecinos cercanos"


    def get_all_movies_of_neighbor(self, nearest_neighbor, data):
        row = data.filter(data.usuario == nearest_neighbor).first()
        if row:
            vecino_peliculas = dict(zip(data.columns[1:], row[1:]))  # Obtener todas las películas del vecino
            return vecino_peliculas
        else:
            return "Vecino no encontrado en el CSV"


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

                if nearest_neighbor:
                    break

        print("Hilo de Spark terminado.")

    def get_nearest_neighbor_info(self):
        return self.nearest_neighbor_info
    
    def get_pelicula_recomendada_info(self):
        return self.pelicula_recomendada_info
    
    def stop_spark_thread(self):
        self.spark_running = False
        self.spark_thread.join()
    
    def restart_spark_thread(self):
        self.spark_thread.join()
        self.spark_thread = threading.Thread(target=self.run_spark_logic)
        self.spark_thread.daemon = True
        self.spark_thread.start()

if __name__ == "__main__":
    data_queue = Queue()
    spark_logic = SparkLogic(data_queue)
    while True:
        pass
