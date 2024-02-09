# Importando as bibliotecas para a extração
from dotenv import load_dotenv
import os
import base64
from requests import post, get
import json
import pandas as pd


# Função para gerar o token de acesso para a api do Spotify
def get_token(client_id, client_secret):
    auth_string = client_id + ":" + client_secret
    auth_bytes = auth_string.encode("ascii")
    auth_base64 = base64.b64encode(auth_bytes)
    base64_string = auth_base64.decode("ascii")

    url = "https://accounts.spotify.com/api/token"
    headers = {
        "Authorization": "Basic " + base64_string,
        "Content-Type": "application/x-www-form-urlencoded"
    }
    data = {"grant_type": "client_credentials"}
    result = post(url, headers=headers, data=data)
    token = result.json()['access_token']

    return token


def get_auth_header(token):
    return {"Authorization": "Bearer " + token}


def get_playlist_data(token, playlist_id):
    url_playlist_data = f'https://api.spotify.com/v1/playlists/{playlist_id}/tracks?market=BR'
    url_playlist_name = f'https://api.spotify.com/v1/playlists/{playlist_id}'
    headers = get_auth_header(token)

    result_playlist_data = get(url_playlist_data, headers=headers)
    json_result = json.loads(result_playlist_data.content)
    
    result_playlist_name = get(url_playlist_name, headers=headers)
    playlist_name = json.loads(result_playlist_name.content)['name']
    
    return json_result, playlist_name


# Chamada da API para buscar infomações das características de áudios
def get_audio_features(token, playlist_data):
    # Salvando os IDs das músicas em string para chamada da API
    music_ids = []

    for item in playlist_data["items"]:
        music_ids.append(item["track"]["id"])

    music_ids_string = ','.join(music_ids)
    
    # Chamando a API
    url = "https://api.spotify.com/v1/audio-features?ids="
    headers = get_auth_header(token)

    query_url = url + music_ids_string
    result = get(query_url, headers=headers)
    json_result = json.loads(result.content)

    return json_result


# Chamada da API para buscar infomações dos artistas
def get_artist_data(token, playlist_data):
    headers = get_auth_header(token)
    ids_str_list = []
    count = 50
    
    for i in range(0, len(playlist_data['items']), count):
        ids_str = ','.join(str(id["track"]["artists"][0]["id"]) for id in playlist_data['items'][i:i+count])
        ids_str_list.append(ids_str)
    
    artist_data = []
    
    for ids_str in ids_str_list:
        url = f'https://api.spotify.com/v1/artists?ids={ids_str}'
        result = get(url, headers=headers)
        json_result = json.loads(result.content)['artists']
        
        artist_data.extend(json_result)
        
    return artist_data


def clean_playlist_data(playlist_data, playlist_name):
    list = []
    dict = {}

    for item in playlist_data["items"]:
        dict["playlist_name"] = playlist_name
        dict["music_name"] = item["track"]["name"]
        dict["music_id"] = item["track"]["id"]
        dict["music_explicit"] = item["track"]["explicit"]
        dict["music_popularity"] = item["track"]["popularity"]
        dict["album_name"] = item["track"]["album"]["name"]
        dict["album_id"] = item["track"]["album"]["id"]
        dict["album_image"] = item["track"]["album"]["images"][0]["url"]
        dict["album_release_date"] = item["track"]["album"]["release_date"]
        dict["artist_name"] = item["track"]["artists"][0]["name"]
        dict["artist_id"] = item["track"]["artists"][0]["id"]

        dict_copy = dict.copy()
        list.append(dict_copy)

    return list


# Função para salvar apenas informações desejadas das músicas
def clean_audio_features(audio_features):
    list = []
    dict = {}

    for item in audio_features["audio_features"]:
        dict["music_id"] = item["id"]
        dict["music_duration"] = item["duration_ms"]
        dict["music_key"] = item["key"]
        dict["music_acousticness"] = item["acousticness"]
        dict["music_danceability"] = item["danceability"]
        dict["music_energy"] = item["energy"]
        dict["music_instrumentalness"] = item["instrumentalness"]
        dict["music_liveness"] = item["liveness"]
        dict["music_loudness"] = item["loudness"]
        dict["music_mode"] = item["mode"]
        dict["music_speechness"] = item["speechiness"]
        dict["music_valence"] = item["valence"]
        dict["music_tempo"] = item["tempo"]

        dict_copy = dict.copy()
        list.append(dict_copy)

    return list


# Função para salvar apenas informações desejadas dos artistas
def clean_artist_data(artist_data):
    list = []
    dict = {}

    for item in artist_data:
        dict["artist_id"] = item["id"]
        dict["artist_genres"] = ', '.join(item["genres"])

        dict_copy = dict.copy()
        list.append(dict_copy)

    return list


def merge_data(clean_playlist_data, clean_audio_features, clean_artist_data):
    df_playlist = pd.DataFrame(clean_playlist_data)
    df_audio_features = pd.DataFrame(clean_audio_features)
    df_artist = pd.DataFrame(clean_artist_data)

    df_final = pd.merge(df_playlist, df_audio_features, left_on='music_id', right_on='music_id')
    df_final = pd.merge(df_final, df_artist, left_on='artist_id', right_on='artist_id')

    df_final.drop_duplicates(subset=['music_id'], keep='first', inplace=True, ignore_index=True)
    
    return df_final


def main():
    # Definindo as secret keys
    load_dotenv()
    client_id = os.getenv("CLIENT_ID")
    client_secret = os.getenv("CLIENT_SECRET")

    # Gerando token de acesso
    token = get_token(client_id, client_secret)

    # Input do ID da playlist
    playlist_ids = ['https://open.spotify.com/playlist/37i9dQZEVXbM8SIrkERIYl',
                    'https://open.spotify.com/playlist/37i9dQZEVXbJPcfkRz0wJ0',
                    'https://open.spotify.com/playlist/37i9dQZEVXbMMy2roB9myp',
                    'https://open.spotify.com/playlist/37i9dQZEVXbOa2lmxNORXQ',
                    'https://open.spotify.com/playlist/37i9dQZEVXbKj23U1GF4IR',
                    'https://open.spotify.com/playlist/37i9dQZEVXbO3qyFxbkOE1',
                    'https://open.spotify.com/playlist/37i9dQZEVXbLRQDuF5jeBp',
                    'https://open.spotify.com/playlist/37i9dQZEVXbNFJfN1Vw8d9',
                    'https://open.spotify.com/playlist/37i9dQZEVXbIQnj7RRhdSX',
                    'https://open.spotify.com/playlist/37i9dQZEVXbIPWwFssbupI',
                    'https://open.spotify.com/playlist/37i9dQZEVXbLnolsZ8PSNw',
                    'https://open.spotify.com/playlist/37i9dQZEVXbJiZcmkrIHGU',
                    'https://open.spotify.com/playlist/37i9dQZEVXbMH2jvi6jvjk',
                    'https://open.spotify.com/playlist/37i9dQZEVXbLn7RQmT5Xv2',
                    'https://open.spotify.com/playlist/37i9dQZEVXbKY7jLzlJ11V',
                    'https://open.spotify.com/playlist/37i9dQZEVXbNBz9cRCSFkY',
                    'https://open.spotify.com/playlist/37i9dQZEVXbKXQ4mDTEBXq',
                    'https://open.spotify.com/playlist/37i9dQZEVXbJkgIdfsJyTw',
                    'https://open.spotify.com/playlist/37i9dQZEVXbObFQZ3JLcXt',
                    'https://open.spotify.com/playlist/37i9dQZEVXbLZ52XmnySJg',
                    'https://open.spotify.com/playlist/37i9dQZEVXbMXbN3EUUhlg']
    
    for playlist_id in playlist_ids:

        playlist_id = playlist_id.split('/')[-1]
        
        # Extraindo informações da playlist
        playlist_data, playlist_name = get_playlist_data(token, playlist_id)
        
        # Extraindo informações das músicas
        audio_features = get_audio_features(token, playlist_data)

        # Extraindo informações dos artistas
        artist_data = get_artist_data(token, playlist_data)

        # Transformando informações desejadas da playlist
        cleaned_playlist_data = clean_playlist_data(playlist_data, playlist_name)

        # Transformando informações desejadas das músicas
        cleaned_audio_features = clean_audio_features(audio_features)

        # Transformando informações desejadas dos artistas
        cleaned_artist_data = clean_artist_data(artist_data)

        # Agrupando dados em único dataset
        final_data = merge_data(cleaned_playlist_data, cleaned_audio_features, cleaned_artist_data)

        # Salvando dataset em arquivo csv
        final_data.to_csv('./datasets/' + playlist_name + '.csv', encoding='utf-8', index=False)

        if playlist_data:
            print('Playlist:', playlist_name)
            print("Dados baixados com sucesso")
            print('---')
        else:
            print("Dados não baixados")


if __name__ == "__main__":
    main()
    
    
''' 

alterar para colar o link e não apenas o id


'''