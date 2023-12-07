from flask import Flask, request, jsonify
from queue import Queue
from spark import SparkLogic

app = Flask(__name__)

# Crear una cola para compartir datos entre la aplicación Flask y Spark
data_queue = Queue()
spark_logic = SparkLogic(data_queue)

@app.route('/procesar', methods=['POST'])
def receive_data():
    data = request.get_json()
    print("Received data:", data)
    # Coloca los datos en la cola
    data_queue.put(data)
    spark_logic.restart_spark_thread()
    vecino = spark_logic.get_nearest_neighbor_info()
    pelicula = spark_logic.get_pelicula_recomendada_info()
    respuesta = {
        'vecino_mas_cercano': vecino,
        'pelicula_recomendada': pelicula,
	    'usuario_recibido': data['usuario'],
    }

    return jsonify(respuesta)

if __name__ == "__main__":
    app.run(debug=True, port=5000)
