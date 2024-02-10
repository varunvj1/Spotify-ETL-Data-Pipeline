import json
import boto3
from datetime import datetime
from io import StringIO
import pandas as pd


def album(data):
    # List to store the album related information
    album_list = []

    # Extract the album information
    for item in data["items"]:
        id = item["track"]["album"]["artists"][0]["id"]
        # artist_name = item['track']['album']['artists'][0]['name']
        album_name = item["track"]["album"]["name"]
        release_date = item["track"]["album"]["release_date"]
        total_tracks = item["track"]["album"]["total_tracks"]
        ext_url = item["track"]["external_urls"]["spotify"]

        # Fetched all the artist names who created that album
        artist_names_list = []
        for artist in item["track"]["artists"]:
            artist_name = artist["name"]
            artist_names_list.append(artist_name)

        all_artist_names = ", ".join(artist_names_list)

        # Stored the album details in a dictionary
        album_elements = {
            "album_id": id,
            "album_name": album_name,
            "artist_name": all_artist_names,
            "release_date": release_date,
            "total_tracks": total_tracks,
            "External URL": ext_url,
        }

        # Appended the disctionary in the list
        album_list.append(album_elements)
    return album_list


def artist(data):
    # Fetched the artist information
    artist_list = []

    for item in data["items"]:
        for artist in item["track"]["artists"]:
            artist_name = artist["name"]
            artist_id = artist["id"]
            artist_url = artist["href"]

            artist_dict = {
                "artist_id": artist_id,
                "artist_name": artist_name,
                "artist_url": artist_url,
            }

            artist_list.append(artist_dict)
        return artist_list


def songs(data):
    # Fetch the song information
    song_list = []

    for item in data["items"]:
        song_id = item["track"]["id"]
        song_name = item["track"]["name"]
        song_popularity = item["track"]["popularity"]
        song_duration = item["track"]["duration_ms"]
        song_url = item["track"]["external_urls"]["spotify"]
        song_added = item["added_at"]
        album_id = item["track"]["album"]["id"]
        artist_id = item["track"]["external_urls"]["spotify"]

        for artist in item["track"]["artists"]:
            artist_id = artist["id"]
            songs_dict = {
                "song_id": song_id,
                "song_name": song_name,
                "song_popularity": song_popularity,
                "song_duration": song_duration,
                "song_url": song_url,
                "song_added": song_added,
                "album_id": album_id,
                "artist_id": artist_id,
            }

            song_list.append(songs_dict)
    return song_list


def lambda_handler(event, context):
    # Create object to access the S3 files
    s3 = boto3.client("s3")

    Bucket = "spotify_etl_pipeline_varun"
    Key = "raw_data/source_data/"

    ############ Fetched the S3 files in the "raw_data" bucket, and stored it in a list ############
    # Contains data
    spotify_data = []

    # Contains file name
    spotify_keys = []
    for file in s3.list.objects(Bucket=Bucket, Prefix=Key)["Contents"]:
        file_key = file["Key"]
        if file_key.split(".")[-1] == "json":
            # Retrieved the object (file information) from Amazon S3
            response = s3.get_object(Bucket=Bucket, Key=file_key)

            # Fetched the data from that file
            content = response["Body"]

            # Read the data (valid json string) and converted it into a Python Dictionary
            jsonObject = json.loads(content.read())

            # Stored the data in the list
            spotify_data.append(jsonObject)

            # Stored the keys (file names with path in the source_data folder) in the list
            spotify_keys.append(file_key)

    # Created the list for albums, artists, songs for each file respectively
    for data in spotify_data:
        album_list = album(data)
        artist_list = artist(data)
        song_list = songs(data)

        # Created dataframe from the dictionaries
        album_df = pd.DataFrame.from_dict(album_list)
        artist_df = pd.DataFrame.from_dict(artist_list)
        song_df = pd.DataFrame.from_dict(song_list)

        # Removed the duplicates
        album_df = album_df.drop_duplicates()
        artist_df = artist_df.drop_duplicates()
        song_df = song_df.drop_duplicates()

        # Converted the "release_date" column to Date/Time format for album_df
        album_df["release_date"] = pd.to_datetime(album_df["release_date"])

        # Converted the "song_added" column to Date/Time format for song_df
        song_df["song_added"] = pd.to_datetime(song_df["song_added"])

        ############### Stored the data to the target location in S3 (transformed data bucket) ###############
        # Created the file name that will be used to store the data in S3
        filename = "spotify_transformed_" + str(datetime.now()) + ".csv"

        ### Songs data ###
        # Created a StringIO object
        song_buffer = StringIO()
        # Converted the dataframe to csv
        song_df.to_csv(song_buffer, index=False)
        # Retrieved the entire contents of the object
        song_content = song_buffer.getvalue()

        s3.put_object(
            Bucket="spotify_etl_pipeline_varun",
            Key="transformed_data/song_data/" + filename,
            Body=song_content,
        )

        ### Album data ###
        album_buffer = StringIO()
        album_df.to_csv(album_buffer, index=False)
        album_content = album_buffer.getvalue()

        s3.put_object(
            Bucket="spotify_etl_pipeline_varun",
            Key="transformed_data/album_data/" + filename,
            Body=album_content,
        )

        ### Artist data ###
        artist_buffer = StringIO()
        artist_df.to_csv(artist_buffer, index=False)
        artist_content = artist_buffer.getvalue()

        s3.put_object(
            Bucket="spotify_etl_pipeline_varun",
            Key="transformed_data/artist_data/" + filename,
            Body=artist_content,
        )

    ### Copy data from "source data" folder to the "processed" folder in S3 (then delete it from source folder) ###
    # 1. Copy data from source -> processed
    s3_resource = boto3.resource("s3")

    # spotify_keys contain a list of all file path names present in the source_data folder in S3
    for key in spotify_keys:
        copy_source = {"Bucket": Bucket, "Key": Key}

        """
            s3_resource.meta.client.copy( 
                <Object having source bucket and file path name>,
                <Target bucket name>,
                <Target file path name> 
            )
        """
        s3_resource.meta.client.copy(
            copy_source, Bucket, "raw_data/processed" + key.split("/")[-1]
        )

        # Delete the json file from "source_data" folder after copying it to "processed" folder
        # This will delete the "source_data" folder
        s3_resource.Object(Bucket, key).delete()
