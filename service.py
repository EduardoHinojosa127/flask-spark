from flask import Flask, request, jsonify
from queue import Queue
from spark import SparkLogic

app = Flask(__name__)

# Crear una cola para compartir datos entre la aplicación Flask y Spark
data_queue = Queue()
spark_logic = SparkLogic(data_queue)

@app.route('/receive_data', methods=['POST'])
def receive_data():
    data = request.get_data().decode('utf-8')
    print("Received data:", data)
    # Coloca los datos en la cola
    data_queue.put(data)
    # Devuelve la respuesta como un JSON
    return "data enviada"

@app.route('/get_nearest_neighbor_info', methods=['GET'])
def get_nearest_neighbor_info():
    nearest_neighbor_info = spark_logic.get_nearest_neighbor_info()
    return jsonify(nearest_neighbor_info)

if __name__ == "__main__":
    app.run(debug=True, port=5001)
