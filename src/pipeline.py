import dlt
from dlt.sources.helpers import requests
from datetime import datetime
import time

@dlt.source
def chess_source(players: list = None):
    if players is None:
        players = ['magnuscarlsen', 'rpragchess']

    @dlt.resource
    def player_games_urls():
        base_url = "https://api.chess.com/pub/player/{player}/games/archives"
        for player in players:
            response = requests.get(base_url.format(player=player))
            response.raise_for_status()
            archives = response.json().get('archives', [])
            for url in archives:
                yield {
                    'player': player,
                    'url': url,
                    'month': url.split('/')[-1]
                }
    return player_games_urls

@dlt.resource(
    primary_key='game_id',
    write_disposition='merge'
)
def player_games_data(incremental_urls=dlt.sources.incremental('url')):
    source = chess_source()
    for url_data in source.player_games_urls:
        if incremental_urls.last_value is not None and url_data['url'] <= incremental_urls.last_value:
            print(f"Saltando URL ya procesada: {url_data['url']}")
            continue

        print(f"Procesando nueva URL: {url_data['url']} para {url_data['player']}")
        response = requests.get(url_data['url'])
        response.raise_for_status()
        games = response.json().get('games', [])

        for game in games:
            white = game.get('white', {})
            black = game.get('black', {})
            white_player = white.get('username')
            black_player = black.get('username')

            result = None
            if white_player == url_data['player']:
                result = white.get('result')
            elif black_player == url_data['player']:
                result = black.get('result')

            end_time = None
            if game.get('end_time'):
                end_time = datetime.fromtimestamp(game['end_time'])

            yield {
                'game_id': game.get('url', '').split('/')[-1] if game.get('url') else None,
                'player': url_data['player'],
                'url': url_data['url'],
                'month': url_data['month'],
                'white_player': white_player,
                'black_player': black_player,
                'result': result,
                'pgn': game.get('pgn', 'N/A'),
                'time_control': game.get('time_control', 'N/A'),
                'end_time': end_time,
                'processed_at': datetime.now()
            }
        time.sleep(1)