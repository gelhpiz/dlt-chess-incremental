# Chess.com ETL con dlt (incremental)

Este proyecto demuestra un pipeline ETL incremental usando la librería **dlt** (data loading tool).  
Extrae partidas de ajedrez de la API de Chess.com para dos jugadores (Magnus Carlsen y R. Praggnanandhaa) y las carga en una base de datos DuckDB.

## Características
- Extracción desde API REST.
- **Carga incremental** basada en el campo `url` (cada URL corresponde a un mes).
- Almacenamiento en DuckDB.
- Uso de `merge` para evitar duplicados (clave primaria `game_id`).

## Requisitos
- Python 3.9+
- Instalar dependencias:
  ```bash
  pip install -r requirements.txt