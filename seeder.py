import csv
import random

# Número de usuarios
num_users = 100000

# Generar datos de ejemplo y escribir al archivo CSV
with open('data.csv', 'w', newline='') as csvfile:
    fieldnames = ['usuario', 'pelicula1', 'pelicula2', 'pelicula3', 'pelicula4', 'pelicula5', 'pelicula6', 'pelicula7', 'pelicula8', 'pelicula9', 'pelicula10']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    # Escribir encabezados
    writer.writeheader()

    # Generar datos aleatorios
    for user_id in range(1, num_users + 1):
        row_data = {
            'usuario': f'User{user_id}',
            'pelicula1': random.randint(1, 100),
            'pelicula2': random.randint(1, 100),
            'pelicula3': random.randint(1, 100),
            'pelicula4': random.randint(1, 100),
            'pelicula5': random.randint(1, 100),
            'pelicula6': random.randint(1, 100),
            'pelicula7': random.randint(1, 100),
            'pelicula8': random.randint(1, 100),
            'pelicula9': random.randint(1, 100),
            'pelicula10': random.randint(1, 100)
        }
        writer.writerow(row_data)

print('Archivo CSV generado con éxito: data.csv')