# ProyectoEnergia
# Proyecto de Interpretación de Datos de Excel en Python

Este proyecto en Python se encarga de interpretar datos de archivos Excel y cargarlos en una base de datos MySQL. Proporciona una solución para descomprimir archivos ZIP, filtrar y transformar datos de Excel, y subirlos a una base de datos para su posterior análisis y consulta.

## Características principales

- Descomprime archivos ZIP: El proyecto incluye funciones para descomprimir archivos ZIP, tanto uno inicial como los archivos ZIP de cada casa en una carpeta específica.

- Filtrado de datos: Permite filtrar los datos de los archivos CSV según el mes especificado en la carpeta de descompresión. Esto garantiza que solo se suban a la base de datos los datos correspondientes al mes deseado.

- Carga en la base de datos: Utiliza consultas SQL para cargar los datos filtrados en una base de datos MySQL. Los datos se organizan en tablas específicas para cada casa e intervalo de tiempo.

- Manejo de datos climáticos: El proyecto también incluye funciones para procesar y cargar datos climáticos en la base de datos. Los archivos de clima se encuentran en una carpeta específica y se cargan en una tabla dedicada.

- Rutas relativas: Se utilizan rutas relativas para asegurar la portabilidad y flexibilidad del proyecto. Esto permite que el código funcione independientemente de la ubicación del repositorio en diferentes entornos.

## Requisitos previos

- Python 3.x
- Pandas
- mysql-connector-python
- python-dateutil

## Configuración de la base de datos

Antes de utilizar este proyecto, asegúrate de configurar correctamente la conexión a tu base de datos MySQL. Debes crear un archivo `conectionBD.py` en el mismo directorio que contenga los detalles de configuración de tu base de datos, como el nombre de usuario, contraseña, host y nombre de la base de datos. Asegúrate de mantener este archivo seguro y no compartirlo públicamente.

## Uso

1. Clona el repositorio a tu máquina local.

git clone https://github.com/tu-usuario/repo.git

2. Asegúrate de cumplir con los requisitos previos mencionados.

3. Ejecuta el archivo `main.py

