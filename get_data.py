import pandas as pd
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import concurrent.futures
import time
import threading

# Replace with your Spotify Developer credentials
SPOTIPY_CLIENT_ID = '64634682c8344306a9dde37511f65673'
SPOTIPY_CLIENT_SECRET = 'ced7da0971274e11a220f77ccc3dea3c'

df = pd.read_csv("final_songs_with_ids.csv")

# Set up authentication
sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(client_id=SPOTIPY_CLIENT_ID,
                                                           client_secret=SPOTIPY_CLIENT_SECRET))

# Memo cache dictionary
memo_cache = {}

# Lock for thread-safe memo cache updates
lock = threading.Lock()

# Counter for consecutive 429 responses
consecutive_429 = 0

# Function to get audio features for a song ID
def get_audio_features(song_id):
    global consecutive_429
    try:
        audio_features = sp.audio_features([song_id])[0]
        if audio_features is None:
            return None
        # Filter out unwanted keys
        keys_to_exclude = {"id", "analysis_url", "track_href", "type", "uri"}
        filtered_features = {k: v for k, v in audio_features.items() if k not in keys_to_exclude}
        consecutive_429 = 0  # Reset the counter on successful response
        return song_id, filtered_features
    except spotipy.exceptions.SpotifyException as e:
        if e.http_status == 429:
            consecutive_429 += 1
            if consecutive_429 >= 40:
                raise RuntimeError("Too many consecutive 429 errors. Exiting...")
        else:
            consecutive_429 = 0
        return song_id, None

# Function to process rows with rate limiting
def process_row_with_rate_limit(song_id):
    while True:
        try:
            result = get_audio_features(song_id)
            time.sleep(0.4)  # 2.5 requests per second
            return result
        except RuntimeError as e:
            # Stop sending requests and save progress
            with lock:
                df.to_csv('audio_features_partial.csv', index=False)
            raise e

count = 1
# Use ThreadPoolExecutor to send API requests concurrently
with concurrent.futures.ThreadPoolExecutor() as executor:
    futures = {executor.submit(process_row_with_rate_limit, song_id): song_id for song_id in df["Song ID"]}
    results = []
    for future in concurrent.futures.as_completed(futures):
        song_id, audio_features = future.result()
        if audio_features:
            with lock:
                memo_cache[song_id] = audio_features
                df.loc[df['Song ID'] == song_id, audio_features.keys()] = audio_features.values()
                print(f"Processed: {count} / {len(df)}")
                count += 1

# Save the final dataframe with audio features
df.to_csv("audio_features.csv", index=False)

print(df)
