# Metricas de Geoportales

## Descripción

Este proyecto está enfocado en la lectura, limpieza y análisis de archivos de logs que provienen de geoportales uruguayos. Utilizando la biblioteca Polars en Python, procesamos un archivo de logs para filtrar información específica y realizar análisis de las solicitudes HTTP realizadas al servidor.

## Archivos

- **access.log**: Archivo de logs web original.
- **archivo.csv**: Archivo CSV generado a partir del archivo de logs web.
- **process_logs.py**: Este script convierte un archivo de logs web en un archivo CSV.

## Requisitos

- Python 3.8 o superior
- Bibliotecas Python: Polars, CSV

Para instalar las bibliotecas necesarias para correr este proyecto, pueden usar:

```bash
pip install polars
pip install kaggle