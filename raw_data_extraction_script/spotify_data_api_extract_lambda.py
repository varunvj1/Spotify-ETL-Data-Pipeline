import json
import os
from spotipy.oauth2 import SpotifyClientCredentials
import boto3
from datetime import datetime
import pandas as pd


def lambda_handler(event, context):
    # Fetched required id(s) stored in the environment variables
    api_client_id = os.environ.get("client_id")
    api_client_secret = os.environ.get("client_secret")

    # Authentication
    client_credentials_manager = SpotifyClientCredentials(
        client_id=api_client_id,
        client_secret=api_client_secret,
    )

    # Authorization
    sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

    # Link to Global Top Songs playlist in Spotify
    playlist_link = "https://open.spotify.com/playlist/37i9dQZEVXbNG2KDcFcKOF"

    # Extracted the path parameter from the URI
    playlist_URI = playlist_link.split("/")[4]

    # playlist_tracks is a method avaliable in the "sp" object that we created
    # Stored this data in a variable
    playlist_data = sp.playlist_tracks(playlist_URI)

    # Created the file name that will be used to store the data in S3
    filename = "spotify_raw_" + str(datetime.now()) + ".json"

    # Stored the data in S3 (in json format)
    client = boto3.client("s3")
    client.put_object(
        Bucket="spotify_etl_pipeline_varun",
        Key="raw_data/source_data/" + filename,
        Body=json.dumps(playlist_data),
    )
