import streamlit as st
from googleapiclient.discovery import build
import pandas as pd
import mysql.connector
from mysql.connector import Error
from datetime import datetime
import re

# Function to get YouTube channel data
def get_youtube_data(channel_id):
    api_key = "AIzaSyB4vLW1698ssn3sU_Lh3CpmZpk0zEoL2k8" 
    youtube = build('youtube', 'v3', developerKey=api_key)
    
    # Fetch channel details
    channel_request = youtube.channels().list(part="snippet,statistics,contentDetails", id=channel_id)
    channel_response = channel_request.execute()
    channel_data = channel_response['items'][0]
    
    channel_info = {
        "Channel_Name": channel_data["snippet"]["title"],
        "Channel_Id": channel_data["id"],
        "Subscription_Count": channel_data["statistics"]["subscriberCount"],
        "Channel_Views": channel_data["statistics"]["viewCount"],
        "Channel_Description": channel_data["snippet"]["description"],
        "Playlist_Id": channel_data["contentDetails"]["relatedPlaylists"]["uploads"]
    }
    
    # Initialize the final structured data
    structured_data = {"Channel_Name": channel_info,}
    
    # Fetch playlists
    playlists_request = youtube.playlists().list(part="snippet,contentDetails", channelId=channel_id)
    playlists_response = playlists_request.execute()

    # Process playlists
    for playlist in playlists_response['items']:
        playlist_id = playlist["id"]
        playlist_name = playlist["snippet"]["title"]

        # Add playlist info to the structured data
        structured_data[f"Playlist_{playlist_id}"] = {
            "Playlist_Id": playlist_id,
            "Playlist_Name": playlist_name
        }

        # Fetch playlist videos
        playlist_request = youtube.playlistItems().list(part="snippet,contentDetails", playlistId=playlist_id, maxResults=10)
        playlist_response = playlist_request.execute()

    for index, item in enumerate(playlist_response['items']):
        video_id = item["contentDetails"]["videoId"]
        
        # Fetch video details
        video_request = youtube.videos().list(part="snippet,statistics,contentDetails", id=video_id)
        video_response = video_request.execute()
        video_data = video_response['items'][0]
     
         # Convert the publishedAt datetime format
        published_at = video_data["snippet"]["publishedAt"]
        formatted_published_at = datetime.strptime(published_at, "%Y-%m-%dT%H:%M:%SZ").strftime("%Y-%m-%d %H:%M:%S")

        # Format video details
        video_info = {
            "Video_Id": video_data["id"],
            "Video_Name": video_data["snippet"]["title"],
            "Video_Description": video_data["snippet"]["description"],
            "Tags": video_data["snippet"].get("tags", []),
            "PublishedAt": formatted_published_at,
            "View_Count": video_data["statistics"].get("viewCount", 0),
            "Like_Count": video_data["statistics"].get("likeCount", 0),
            "Dislike_Count": video_data["statistics"].get("dislikeCount", 0),
            "Favorite_Count": video_data["statistics"].get("favoriteCount", 0),
            "Comment_Count": video_data["statistics"].get("commentCount", 0),
           "Duration": video_data["contentDetails"].get("duration", "Not Available"),
            "Thumbnail": video_data["snippet"]["thumbnails"]["default"]["url"],
            "Caption_Status": video_data["contentDetails"].get("caption", "Not Available"),
            "Comments": {}
        }
        
        # Fetch comments for the video
        try:
            comments_request = youtube.commentThreads().list(part="snippet", videoId=video_id, maxResults=5)
            comments_response = comments_request.execute()
            
            for comment_item in comments_response["items"]:
                comment_id = comment_item["id"]
                comment_snippet = comment_item["snippet"]["topLevelComment"]["snippet"]
                
                comment_info = {
                    "Comment_Id": comment_id,
                    "Comment_Text": comment_snippet["textDisplay"],
                    "Comment_Author": comment_snippet["authorDisplayName"],
                    "Comment_PublishedAt": comment_snippet["publishedAt"]
                }
                
                video_info["Comments"][comment_id] = comment_info
        except Exception as e:
            st.write(f"Comments are disabled for video {video_id}. Skipping comments fetching.")

        # Add video info to the structured data
        structured_data[f"Video_Id_{index + 1}"] = video_info
    
    return structured_data

# Function to connect to MySQL
def connect_to_mysql():
    try:
        conn = mysql.connector.connect(
            host='localhost',
            user='root', 
            password='12345678',
            database='Youtube_data'
        )

        if conn.is_connected():
            return conn
    except Error as e:
        st.write(f"Error: {e}")
        return None

# function to add Duration
def parse_duration(duration):
    """Parse ISO 8601 duration format to total seconds."""
    pattern = re.compile(
        r'^P(?:(\d+)Y)?(?:(\d+)M)?(?:(\d+)D)?(?:T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?)?$'
    )
    match = pattern.match(duration)
    if match:
        years, months, days, hours, minutes, seconds = [int(x) if x else 0 for x in match.groups()]
        total_seconds = (
            seconds
            + minutes * 60
            + hours * 3600
            + days * 86400
            + months * 2592000  
            + years * 31536000  
        )
        return total_seconds
    else:
        return None


# Function to create tables if they don't exist
def create_tables():
    conn = connect_to_mysql()
    if conn:
        cursor = conn.cursor()
        create_channel_table = """
        CREATE TABLE IF NOT EXISTS Channels (
            channel_id VARCHAR(255) PRIMARY KEY,
            channel_name VARCHAR(255),
            subscription_count BIGINT,
            channel_views BIGINT,
            channel_description TEXT,
            channel_status VARCHAR(50)  
        );
        """
        create_playlist_table = """
        CREATE TABLE IF NOT EXISTS Playlists (
            playlist_id VARCHAR(255) PRIMARY KEY,
             playlist_name VARCHAR(255),
            channel_id VARCHAR(255),
            FOREIGN KEY (channel_id) REFERENCES Channels(channel_id)
        );
        """
        create_video_table = """
        CREATE TABLE IF NOT EXISTS Videos (
            video_id VARCHAR(255) PRIMARY KEY,
            playlist_id VARCHAR(255),
            video_name VARCHAR(255),
            video_description TEXT,
            tags TEXT,
            published_at DATETIME,
            view_count BIGINT,
            like_count BIGINT,
            dislike_count BIGINT,
            favorite_count BIGINT,
            comment_count BIGINT,
            duration INT,
            thumbnail VARCHAR(255),
            caption_status VARCHAR(50),
            FOREIGN KEY (playlist_id) REFERENCES Playlists(playlist_id)
        );
        """
        create_comment_table = """
        CREATE TABLE IF NOT EXISTS Comments (
            comment_id VARCHAR(255) PRIMARY KEY,
            video_id VARCHAR(255),
            comment_text TEXT,
            comment_author VARCHAR(255),
            comment_published_at DATETIME,
            FOREIGN KEY (video_id) REFERENCES Videos(video_id)
        );
        """

        cursor.execute(create_channel_table)
        cursor.execute(create_playlist_table)
        cursor.execute(create_video_table)
        cursor.execute(create_comment_table)
        conn.commit()
        cursor.close()
        conn.close()

def check_exists(table, key_column, key_value):
    conn = connect_to_mysql()
    if conn:
        cursor = conn.cursor()
        query = f"SELECT COUNT(*) FROM {table} WHERE {key_column} = %s"
        cursor.execute(query, (key_value,))
        result = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        return result > 0
    return False


 # Function to store data in respective tables

def store_data(channel_data):
    conn = connect_to_mysql()
    if conn:
        cursor = conn.cursor()
        channel_id = channel_data['Channel_Name']["Channel_Id"]
        channel_name = channel_data['Channel_Name']["Channel_Name"]
        subscription_count = channel_data['Channel_Name']["Subscription_Count"]
        channel_views = channel_data['Channel_Name']["Channel_Views"]
        channel_description = channel_data['Channel_Name']["Channel_Description"]
        channel_status = "Active"

        # Check if channel data exists
        exists = check_exists('Channels', 'channel_id', channel_id)
        if exists:
            st.write(f"Channel data with ID {channel_id} already exists.")
        else:
            st.write(f"Inserting new channel data with ID {channel_id}.")
            # Insert or update channel data
            channel_insert_query = """
            INSERT INTO Channels (channel_id, channel_name, subscription_count, channel_views, channel_description, channel_status)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE 
            channel_name=VALUES(channel_name), 
            subscription_count=VALUES(subscription_count), 
            channel_views=VALUES(channel_views), 
            channel_description=VALUES(channel_description),
            channel_status=VALUES(channel_status);
            """
            channel_values = (channel_id, channel_name, subscription_count, channel_views, channel_description, channel_status)
            cursor.execute(channel_insert_query, channel_values)

            for key, playlist_data in channel_data.items():
                if key.startswith('Playlist_'):
                    playlist_insert_query = """
                    INSERT INTO Playlists (playlist_id, playlist_name, channel_id)
                    VALUES (%s, %s, %s)
                    ON DUPLICATE KEY UPDATE 
                    playlist_name=VALUES(playlist_name), 
                    channel_id=VALUES(channel_id);
                    """
                    playlist_values = (playlist_data["Playlist_Id"], playlist_data["Playlist_Name"], channel_id)
                    cursor.execute(playlist_insert_query, playlist_values)

                    playlist_id = playlist_data["Playlist_Id"]

                    for key, video_data in channel_data.items():
                        if key.startswith('Video_Id_'):
                            video_insert_query = """
                            INSERT INTO Videos (video_id, playlist_id, video_name, video_description, tags, published_at, 
                            view_count, like_count, dislike_count, favorite_count, comment_count, duration, 
                            thumbnail, caption_status)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON DUPLICATE KEY UPDATE 
                            video_name=VALUES(video_name),
                            video_description=VALUES(video_description),
                            tags=VALUES(tags),
                            published_at=VALUES(published_at),
                            view_count=VALUES(view_count),
                            like_count=VALUES(like_count),
                            dislike_count=VALUES(dislike_count),
                            favorite_count=VALUES(favorite_count),
                            comment_count=VALUES(comment_count),
                            duration=VALUES(duration),
                            thumbnail=VALUES(thumbnail),
                            caption_status=VALUES(caption_status);
                            """

                            duration_seconds = parse_duration(video_data["Duration"])
                            try:
                                formatted_published_at = datetime.strptime(video_data["PublishedAt"], "%Y-%m-%dT%H:%M:%SZ").strftime("%Y-%m-%d %H:%M:%S")
                            except ValueError:
                                formatted_published_at = datetime.strptime(video_data["PublishedAt"], "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%d %H:%M:%S")

                            video_values = (
                                video_data["Video_Id"],
                                playlist_id,
                                video_data["Video_Name"],
                                video_data["Video_Description"],
                                ','.join(video_data["Tags"]),
                                formatted_published_at,
                                video_data["View_Count"],
                                video_data["Like_Count"],
                                video_data["Dislike_Count"],
                                video_data["Favorite_Count"],
                                video_data["Comment_Count"],
                                # video_data["Duration"],
                                duration_seconds,
                                video_data["Thumbnail"],
                                video_data["Caption_Status"]
                            )
                            cursor.execute(video_insert_query, video_values)

                            for comment_id, comment_info in video_data["Comments"].items():
                                comment_insert_query = """
                                INSERT INTO Comments (comment_id, video_id, comment_text, comment_author, comment_published_at)
                                VALUES (%s, %s, %s, %s, %s)
                                ON DUPLICATE KEY UPDATE 
                                comment_text=VALUES(comment_text),
                                comment_author=VALUES(comment_author),
                                comment_published_at=VALUES(comment_published_at);
                                """
                                
                                try:
                                    formatted_comment_published_at = datetime.strptime(comment_info["Comment_PublishedAt"], "%Y-%m-%dT%H:%M:%SZ").strftime("%Y-%m-%d %H:%M:%S")
                                except ValueError:
                                    formatted_comment_published_at = datetime.strptime(comment_info["Comment_PublishedAt"], "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%d %H:%M:%S")

                                comment_values = (
                                    comment_info["Comment_Id"],
                                    video_data["Video_Id"],
                                    comment_info["Comment_Text"],
                                    comment_info["Comment_Author"],
                                    formatted_comment_published_at
                                )
                                cursor.execute(comment_insert_query, comment_values)

            st.write(f"Data for channel {channel_name} stored successfully.")
            conn.commit()
            cursor.close()
            conn.close()


# Function to query data from MySQL
def query_data(query):
    conn = connect_to_mysql()
    if conn:
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df
    else:
        return pd.DataFrame()

# Function to show all tables
def showtables():
    conn = connect_to_mysql()
    if conn:
        cursor = conn.cursor()
        cursor.execute("SHOW TABLES;")
        tables = cursor.fetchall()
        cursor.close()
        conn.close()
        return [table[0] for table in tables]
    return []

# Function to delete Table
def drop_table(tab):
    conn = connect_to_mysql()
    if conn:
        cursor = conn.cursor()
        query = f"DROP table if exists `{tab}`;"
        cursor.execute(query)
        conn.commit()
        cursor.close()
        conn.close()
        st.write(f" '{tab}' deleted from database successfully")


# Streamlit app UI
st.title("YouTube Data Harvesting and Warehousing")
channel_id = st.text_input("Enter YouTube Channel ID")
if st.button("Fetch Data"):
    create_tables()  # Ensure tables are created
    channel_data = get_youtube_data(channel_id)
    store_data(channel_data)
    

# Dropdown for table selection
selected_table = st.selectbox("Select Table to View", options=showtables())

# Button to fetch and display table data
if st.button("Fetch Table Data"):
    if selected_table:
        query = f"SELECT * FROM `{selected_table}`;"
        df = query_data(query)
        if not df.empty:
            st.write(f"Data from table: {selected_table}")
            st.dataframe(df)
        else:
            st.write(f"No data found in table: {selected_table}")
    else:
        st.write("Please select a table to view.")

if st.button("Show all Tables in Database"):
    tables = showtables()
    st.write("Tables in the Database:")
    st.write(tables)
tab = st.text_input("Enter Table name to remove from DB").strip()

if st.button("Delete Stored table in DB"):

    if tab:
        drop_table(tab)
    else:
        st.write("Please enter a table name.")


# Function to query data from the database
def query_data(query):
    conn = mysql.connector.connect(
            host='localhost',
            user='root', 
            password='12345678',
            database='Youtube_data'
        )
    # conn = mysql.connector.connect('Youtube_data.db')  # Replace with your database connection
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

# Function to display SQL query results as tables
def display_query_results(query, title):
    df = query_data(query)
    if not df.empty:
        st.write(title)
        st.dataframe(df)
    else:
        st.write(f"No data found for: {title}")


st.title("YouTube Data Analysis")

# Queries to fetch data
queries = {
    "1. Names of All Videos and Their Corresponding Channels": """
        SELECT v.video_name, c.channel_name
        FROM Videos v
        JOIN Playlists p ON v.playlist_id = p.playlist_id
        JOIN Channels c ON p.channel_id = c.channel_id;
    """,
    "2. Channels with the Most Number of Videos": """
        SELECT c.channel_name, COUNT(v.video_id) AS number_of_videos
        FROM Videos v
        JOIN Playlists p ON v.playlist_id = p.playlist_id
        JOIN Channels c ON p.channel_id = c.channel_id
        GROUP BY c.channel_name
        ORDER BY number_of_videos DESC;
    """,
    "3. Top 10 Most Viewed Videos and Their Respective Channels": """
        SELECT v.video_name, c.channel_name, v.view_count
        FROM Videos v
        JOIN Playlists p ON v.playlist_id = p.playlist_id
        JOIN Channels c ON p.channel_id = c.channel_id
        ORDER BY v.view_count DESC
        LIMIT 10;
    """,
    "4. Number of Comments Made on Each Video and Their Corresponding Video Names": """
        SELECT 
    video_name, 
    comment_count AS number_of_comments
FROM 
    Videos
ORDER BY 
    number_of_comments DESC;

    """,
    "5. Videos with the Highest Number of Likes and Their Corresponding Channel Names": """
        SELECT v.video_name, c.channel_name, v.like_count
        FROM Videos v
        JOIN Playlists p ON v.playlist_id = p.playlist_id
        JOIN Channels c ON p.channel_id = c.channel_id
        ORDER BY v.like_count DESC;
    """,
    "6. Total Number of Likes and Dislikes for Each Video and Their Corresponding Video Names": """
        SELECT v.video_name, (v.like_count) AS total_likes  ,( v.dislike_count) AS total_dislikes
        FROM Videos v;
    """,
    "7.Total Number of Views for Each Channel and Their Corresponding Channel Names": """
        SELECT c.channel_name, SUM(v.view_count) AS total_views
        FROM Videos v
        JOIN Playlists p ON v.playlist_id = p.playlist_id
        JOIN Channels c ON p.channel_id = c.channel_id
        GROUP BY c.channel_name;
    """,
    "8. Names of All Channels That Have Published Videos in the Year 2022": """
     SELECT DISTINCT c.channel_name
FROM Channels c
JOIN Playlists p ON c.channel_id = p.channel_id
JOIN Videos v ON p.playlist_id = v.playlist_id
WHERE YEAR(v.published_at) = 2022;

    """,
    "9. Average Duration of All Videos in Each Channel and Their Corresponding Channel Names": """
        SELECT c.channel_name, 
       AVG(TIME_TO_SEC(v.duration)) AS average_duration_seconds
FROM Videos v
JOIN Playlists p ON v.playlist_id = p.playlist_id
JOIN Channels c ON p.channel_id = c.channel_id
GROUP BY c.channel_name;
    """,
    "10. Videos with the Highest Number of Comments and Their Corresponding Channel Names": """
       SELECT v.video_name, ch.channel_name, v.comment_count AS number_of_comments
FROM Videos v
JOIN Playlists p ON v.playlist_id = p.playlist_id
JOIN Channels ch ON p.channel_id = ch.channel_id
ORDER BY v.comment_count DESC;

    """
}

# Display results for each query
for title, query in queries.items():
    display_query_results(query, title)



