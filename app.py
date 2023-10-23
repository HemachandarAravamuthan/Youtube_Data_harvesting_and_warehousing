#importing packages
import pymongo
import pandas as pd
import streamlit as st
import mysql.connector
from googleapiclient.discovery import build

#mongoDb setup
client = pymongo.MongoClient("mongodb+srv://Hemachandar:hema1234@cluster0.8rfch9i.mongodb.net/?retryWrites=true&w=majority")
db = client.Youtube_Data

#SQL server connection
mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="",
  database='youtube_data'
  
)

mycursor = mydb.cursor(buffered=True)

#api key connection
api_key = 'AIzaSyBGy_dk6_7Uwa1aZxmQuyn2lZc4RxeDJ4M'
youtube = build('youtube', 'v3', developerKey=api_key)

#function to get channel stats
def get_channel_stats(channel_id):
    request = youtube.channels().list(
        part='snippet,contentDetails,statistics',
        id=channel_id)
    response = request.execute()

    data = dict(Channel_id = channel_id,
                Channel_name = response['items'][0]['snippet']['title'],
                Description = response['items'][0]['snippet']['description'],
                Playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads'],
                Subscribers = response['items'][0]['statistics']['subscriberCount'],
                Views = response['items'][0]['statistics']['viewCount'],
                Total_videos = response['items'][0]['statistics']['videoCount'])

    return data

#function to get channel video ids
def get_channel_videos(channel_id):
    video_ids = []
    # get Uploads playlist id
    request = youtube.channels().list(
        part='snippet,contentDetails,statistics',
        id=channel_id)
    response = request.execute()
    playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None

    while True:
        response = youtube.playlistItems().list(playlistId=playlist_id,
                                           part='snippet',
                                           maxResults=50,
                                           pageToken=next_page_token).execute()

        for i in range(len(response['items'])):
            video_ids.append(response['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = response.get('nextPageToken')

        if next_page_token is None:
            break
    return video_ids

#function to convert delta time to mins
def duration_conversion(t):
      a = pd.Timedelta(t)
      b = str(a).split()[-1]
      return b

#function to get video stats
def get_video_detail(video_id):
    video_data=[]
    request = youtube.videos().list(
        part='snippet,contentDetails,statistics',
        id=video_id)
    response = request.execute()

    for video in response['items']:
        video_details=dict(Channel_name = video['snippet']['channelTitle'],
                        Video_id = video['id'],
                        Video_name = video['snippet']['title'],
                        Video_description = video['snippet']['description'],
                        Published_date = video['snippet']['publishedAt'],
                        View_count = video['statistics']['viewCount'],
                        Like_count = video['statistics']['likeCount'],
                        Favorite_count = video['statistics']['favoriteCount'],
                        Comments = video['statistics']['commentCount'],
                        Duration = duration_conversion(video['contentDetails']['duration']),
                        Thumbnail = video['snippet']['thumbnails']['default']['url'],
                        Caption_status = video['contentDetails']['caption']
                        )
        video_data.append(video_details)
    return (video_data)

#function to get comment details
def get_comments_details(v_id):
    comment_data = []
    try:
        next_page_token = None
        while True:
            response = youtube.commentThreads().list(part="snippet,replies",
                                                    videoId=v_id,
                                                    maxResults=100,
                                                    pageToken=next_page_token).execute()
            for cmt in response['items']:
                data = dict(Comment_id = cmt['id'],
                            Video_id = cmt['snippet']['videoId'],
                            Comment_text = cmt['snippet']['topLevelComment']['snippet']['textDisplay'],
                            Comment_author = cmt['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            Comment_posted_date = cmt['snippet']['topLevelComment']['snippet']['publishedAt'],
                            Like_count = cmt['snippet']['topLevelComment']['snippet']['likeCount'],
                            Reply_count = cmt['snippet']['totalReplyCount']
                           )
                comment_data.append(data)
            next_page_token = response.get('nextPageToken')
            if next_page_token is None:
                break
    except:
        pass
    return comment_data

#function to exctract data from youtubeapi and upload it to mongoDB
def extract_and_upload(id):
        records1=db.channel
        channel_detail=get_channel_stats(id)
        records1.insert_many([channel_detail])

        video_ids=get_channel_videos(id)
        for i in video_ids:
            data=get_video_detail(i)
            records2=db.video
            video_detail=data
            records2.insert_many(video_detail)

        for i in video_ids:
            comment_detail=get_comments_details(i)
            if comment_detail !=[]:
              records3=db.comment
              records3.insert_many(comment_detail)

#function to create table in sql
def sql_create_tables():
    mycursor.execute("CREATE TABLE if not exists channel(\
                                        channel_id 			varchar(255) primary key,\
                                        channel_name		varchar(255),\
                                        channel_description	text,\
                                        upload_id			varchar(255),\
                                        subscription_count	int,\
                                        channel_views		int,\
                                        total_videos        int)")

    mycursor.execute("CREATE TABLE if not exists video(\
                                        channel_name        varchar(255),\
                                        video_id			varchar(255),\
                                        video_name			varchar(255),\
                                        video_description	text,\
                                        published_date		date,\
                                        view_count			int,\
                                        like_count			int,\
                                        favourite_count		int,\
                                        comment_count		int,\
                                        duration			time,\
                                        thumbnail			varchar(255),\
                                        caption_status		varchar(255))")

    mycursor.execute("CREATE TABLE if not exists comment(\
                                        comment_id				varchar(255),\
                                        video_id				varchar(255),\
                                        comment_text			text,\
                                        comment_author			varchar(255),\
                                        comment_published_date	datetime,\
                                        like_count              int,\
                                        reply_count             int)")

    mydb.commit()

#function to insert values into channel table
def insert_into_channels(name):
                    records = db.channel
                    query = """INSERT INTO channel VALUES(%s,%s,%s,%s,%s,%s,%s)"""
                
                    for i in records.find({"Channel_name" : name},{'_id' : 0}):
                        mycursor.execute(query,tuple(i.values()))
                    mydb.commit()

#function to insert values into video table
def insert_into_videos(name):
            records1 = db.video
            query1 = """INSERT INTO video VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""

            for i in records1.find({"Channel_name" : name},{'_id' : 0}):
                mycursor.execute(query1, tuple(i.values()))
                mydb.commit()

#function to insert values into comment table
def insert_into_comments(name):
            records1 = db.video
            records2 = db.comment
            query2 = """INSERT INTO comment VALUES(%s,%s,%s,%s,%s,%s,%s)"""

            for vid in records1.find({"Channel_name" : name},{'_id' : 0}):
                for i in records2.find({'Video_id': vid['Video_id']},{'_id' : 0}):
                    mycursor.execute(query2,tuple(i.values()))
                    mydb.commit()

#function to get channel name from mongoDB
def channel_name():   
    ch_name = []
    for i in db.channel.find():
        ch_name.append(i['Channel_name'])
    return ch_name

#streamlit Page setup
st.title('YouTube Data Harvesting and Warehousing')
st.header('Work flow')
st.write('''Retrieving data from the YouTube API --> 
Store data to MongoDB --> 
Migrating data to a SQL data warehouse --> 
Data Analysis''')

#dropdown to select operation
list_options = ['Retrieving data from the YouTube API and dtore to mongoDB',
                'Migrating data to a SQL database', 'Data Analysis', 'Exit']
option = st.selectbox('Select One', list_options)

#Retrieving data from the YouTube API and dtore to mongoDB
if option=='Retrieving data from the YouTube API and dtore to mongoDB':
     if st.button('Channel_id ?'):
          st.write("View channel home page - Right click on the screen - select 'view page source' - search'?channel_id'")
     channel_id=st.text_input('Enter channel_id')
     if st.button('Enter'):
        extract_and_upload(channel_id)
        st.success('Successfully extracted and stored in MongoDB')

#Migrating data to a SQL database
elif option=='Migrating data to a SQL database':
     sql_create_tables()
     st.markdown("#   ")
     st.markdown("Select Channel to migrated to SQL")
     ch_names = channel_name()
     name = st.selectbox("Select channel",options= ch_names)
     if st.button("Submit"):
                try:
                    insert_into_channels(name)
                    insert_into_videos(name)
                    insert_into_comments(name)
                    st.success("Successfully migrated to SQL")
                except:
                     st.error('Data already Migrated')

#Data Analysis
elif option=='Data Analysis':
    #dropdown to select questions
    question = st.selectbox('Questions',
    ['1. What are the names of all the videos and their corresponding channels?',
    '2. Which channels have the most number of videos, and how many videos do they have?',
    '3. What are the top 10 most viewed videos and their respective channels?',
    '4. How many comments were made on each video, and what are their corresponding video names?',
    '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
    '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
    '7. What is the total number of views for each channel, and what are their corresponding channel names?',
    '8. What are the names of all the channels that have published videos in the year 2022?',
    '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
    '10. Which videos have the highest number of comments, and what are their corresponding channel names?'])
    if question == '1. What are the names of all the videos and their corresponding channels?':
        mycursor.execute("""SELECT video_name AS Video_Title, channel_name AS Channel_Name
                            FROM video
                            ORDER BY channel_name""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write("Names of all the videos and their corresponding channels")
        st.write(df)
            
    elif question == '2. Which channels have the most number of videos, and how many videos do they have?':
        mycursor.execute("""SELECT channel_name AS Channel_Name, total_videos AS Total_Videos
                            FROM channel
                            ORDER BY total_videos DESC""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write("Number of videos in each channel")
        st.write(df)
        
    elif question == '3. What are the top 10 most viewed videos and their respective channels?':
        mycursor.execute("""SELECT channel_name AS Channel_Name, video_name AS Video_Title, view_count AS Views 
                            FROM video
                            ORDER BY views DESC
                            LIMIT 10""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write("Top 10 most viewed videos")
        st.write(df)
        
    elif question == '4. How many comments were made on each video, and what are their corresponding video names?':
        mycursor.execute("""SELECT a.video_id AS Video_id, a.video_name AS Video_Title, a.Comment_count
                            FROM video AS a
                            LEFT JOIN (SELECT video_id,COUNT(comment_id) AS Total_Comments
                            FROM comment GROUP BY video_id) AS b
                            ON a.video_id = b.video_id
                            ORDER BY b.Total_Comments DESC""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write("Comments on video")
        st.write(df)
        
    elif question == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
        mycursor.execute("""SELECT channel_name AS Channel_Name,video_name AS Title,like_count AS Likes_Count 
                            FROM video
                            ORDER BY like_count DESC
                            LIMIT 10""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write("Highest liked videos from channel ")
        st.write(df)
        
    elif question == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
        mycursor.execute("""SELECT video_name AS Title, like_count AS Likes_Count
                            FROM video
                            ORDER BY like_count DESC""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write("Likes and dislikes of video")    
        st.write(df)
        
    elif question == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
        mycursor.execute("""SELECT channel_name AS Channel_Name, channel_views AS Views
                            FROM channel
                            ORDER BY channel_views DESC""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write("Channels vs Views")
        st.write(df)
    
    elif question == '8. What are the names of all the channels that have published videos in the year 2022?':
        mycursor.execute("""SELECT channel_name AS Channel_Name 
                            FROM video
                            WHERE published_date LIKE '2022%'
                            GROUP BY channel_name
                            ORDER BY channel_name""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write("Videos published in 2022")
        st.write(df)
        
    elif question == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
        mycursor.execute("""SELECT channel_name AS Channel_Name,
                            AVG(duration)/60 AS "Average_Video_Duration (mins)"
                            FROM video
                            GROUP BY channel_name
                            ORDER BY AVG(duration)/60 DESC""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write("Avg video duration for channels")
        st.write(df)
        
    elif question == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
        mycursor.execute("""SELECT channel_name AS Channel_Name,video_id AS Video_ID,comment_count AS Comments
                            FROM video
                            ORDER BY comment_count DESC
                            LIMIT 10""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write("Videos with most comments")
        st.write(df)

#exit
elif option=='Exit':
     st.write("Thank you")
