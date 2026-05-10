import dlt
from src.pipeline import player_games_data

def main():
    pipeline = dlt.pipeline(
        pipeline_name='chess_incremental_pipeline',
        destination='duckdb',
        dataset_name='chess_data'
    )

    print("--- PRIMERA EJECUCIÓN (carga completa) ---")
    load_info = pipeline.run(player_games_data())
    print(load_info)

    print("\n--- ESTADO DEL PIPELINE ---")
    state = pipeline.state
    resource_state = state.get('resources', {}).get('player_games_data', {})
    last_url = resource_state.get('incremental', {}).get('url', {}).get('last_value')
    print(f"Última URL procesada: {last_url}")

    print("\n--- DATOS ALMACENADOS ---")
    with pipeline.sql_client() as client:
        result = client.execute_sql(
            "SELECT player, month, COUNT(*) as game_count FROM player_games_data GROUP BY player, month ORDER BY month;"
        )
        for row in result:
            print(row)

if __name__ == "__main__":
    main()