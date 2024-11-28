# from pyspark.sql.types import (
#     StructType,
#     StructField,
#     StringType,
#     IntegerType,
#     DoubleType,
#     LongType,
# )

# authentication_events_schema = StructType(
#     [
#         StructField("ts", LongType(), True),               
#         StructField("sessionId", LongType(), True),        
#         StructField("level", StringType(), True),          
#         StructField("itemInSession", LongType(), True),    
#         StructField("city", StringType(), True),           
#         StructField("zip", StringType(), True),           
#         StructField("state", StringType(), True),          
#         StructField("userAgent", StringType(), True),   
#         StructField("lon", DoubleType(), True),       
#         StructField("lat", DoubleType(), True),           
#         StructField("userId", LongType(), True),        
#         StructField("lastName", StringType(), True),     
#         StructField("firstName", StringType(), True),    
#         StructField("gender", StringType(), True),        
#         StructField("registration", LongType(), True),     
#         StructField("success", StringType(), True),       
#     ]
# )

# listening_events_schema = StructType(
#     [
#         StructField("artist", StringType(), True),          
#         StructField("song", StringType(), True),            
#         StructField("duration", DoubleType(), True),        
#         StructField("ts", LongType(), True),              
#         StructField("sessionId", LongType(), True),     
#         StructField("auth", StringType(), True),           
#         StructField("level", StringType(), True),        
#         StructField("itemInSession", LongType(), True),   
#         StructField("city", StringType(), True),          
#         StructField("zip", StringType(), True),            
#         StructField("state", StringType(), True),          
#         StructField("userAgent", StringType(), True),      
#         StructField("lon", DoubleType(), True),            
#         StructField("lat", DoubleType(), True),           
#         StructField("userId", LongType(), True),         
#         StructField("lastName", StringType(), True),      
#         StructField("firstName", StringType(), True),    
#         StructField("gender", StringType(), True),        
#         StructField("registration", LongType(), True),     
#     ]
# )

# page_view_events_schema = StructType(
#     [
#         StructField("ts", LongType(), True),               
#         StructField("sessionId", LongType(), True),         
#         StructField("page", StringType(), True),           
#         StructField("auth", StringType(), True),           
#         StructField("method", StringType(), True),         
#         StructField("status", IntegerType(), True),        
#         StructField("level", StringType(), True),          
#         StructField("itemInSession", LongType(), True),    
#         StructField("city", StringType(), True),          
#         StructField("zip", StringType(), True),           
#         StructField("state", StringType(), True),         
#         StructField("userAgent", StringType(), True),      
#         StructField("lon", DoubleType(), True),           
#         StructField("lat", DoubleType(), True),            
#         StructField("userId", LongType(), True),           
#         StructField("lastName", StringType(), True),       
#         StructField("firstName", StringType(), True),      
#         StructField("gender", StringType(), True),         
#         StructField("registration", LongType(), True),      
#         StructField("artist", StringType(), True),         
#         StructField("song", StringType(), True),            
#         StructField("duration", DoubleType(), True),   
#     ]
# )

# status_change_events_schema = StructType(
#     [
#         StructField("ts", LongType(), True),                
#         StructField("sessionId", LongType(), True),        
#         StructField("auth", StringType(), True),            
#         StructField("level", StringType(), True),           
#         StructField("itemInSession", LongType(), True),     
#         StructField("city", StringType(), True),            
#         StructField("zip", StringType(), True),             
#         StructField("state", StringType(), True),           
#         StructField("userAgent", StringType(), True),       
#         StructField("lon", DoubleType(), True),            
#         StructField("lat", DoubleType(), True),            
#         StructField("userId", LongType(), True),            
#         StructField("lastName", StringType(), True),        
#         StructField("firstName", StringType(), True),       
#         StructField("gender", StringType(), True),         
#         StructField("registration", LongType(), True),       
#     ]
# )
from pyspark.sql.types import (IntegerType,
                               StringType,
                               DoubleType,
                               StructField,
                               StructType,
                               LongType,
                               BooleanType,
                               TimestampType)

schema = {
    'listen_events': StructType([
        StructField("artist", StringType(), True),
        StructField("song", StringType(), True),
        StructField("duration", DoubleType(), True),
        StructField("ts", LongType(), True),
        StructField("sessionId", IntegerType(), True),
        StructField("auth", StringType(), True),
        StructField("level", StringType(), True),
        StructField("itemInSession", IntegerType(), True),
        StructField("city", StringType(), True),
        StructField("zip", StringType(), True), 
        StructField("state", StringType(), True),
        StructField("userAgent", StringType(), True),
        StructField("lon", DoubleType(), True),
        StructField("lat", DoubleType(), True),
        StructField("userId", LongType(), True),
        StructField("lastName", StringType(), True),
        StructField("firstName", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("registration", LongType(), True)
    ]),
    'page_view_events': StructType([
        StructField("ts", LongType(), True),
        StructField("sessionId", IntegerType(), True),
        StructField("page", StringType(), True),
        StructField("auth", StringType(), True),
        StructField("method", StringType(), True),
        StructField("status", IntegerType(), True),
        StructField("level", StringType(), True),
        StructField("itemInSession", IntegerType(), True),
        StructField("city", StringType(), True),
        StructField("zip", StringType(), True),
        StructField("state", StringType(), True),
        StructField("userAgent", StringType(), True),
        StructField("lon", DoubleType(), True),
        StructField("lat", DoubleType(), True),
        StructField("userId", IntegerType(), True),
        StructField("lastName", StringType(), True),
        StructField("firstName", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("registration", LongType(), True),
        StructField("artist", StringType(), True),
        StructField("song", StringType(), True),
        StructField("duration", DoubleType(), True)
    ]),
    'auth_events': StructType([
        StructField("ts", LongType(), True),
        StructField("sessionId", IntegerType(), True),
        StructField("level", StringType(), True),
        StructField("itemInSession", IntegerType(), True),
        StructField("city", StringType(), True),
        StructField("zip", StringType(), True),
        StructField("state", StringType(), True),
        StructField("userAgent", StringType(), True),
        StructField("lon", DoubleType(), True),
        StructField("lat", DoubleType(), True),
        StructField("userId", IntegerType(), True),
        StructField("lastName", StringType(), True),
        StructField("firstName", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("registration", LongType(), True),
        StructField("success", BooleanType(), True)
    ]),
    'status_change_events':StructType(
     [
         StructField("ts", LongType(), True),                
         StructField("sessionId", LongType(), True),        
         StructField("auth", StringType(), True),            
         StructField("level", StringType(), True),           
         StructField("itemInSession", LongType(), True),     
         StructField("city", StringType(), True),            
         StructField("zip", StringType(), True),             
         StructField("state", StringType(), True),           
         StructField("userAgent", StringType(), True),       
         StructField("lon", DoubleType(), True),            
         StructField("lat", DoubleType(), True),            
         StructField("userId", LongType(), True),            
         StructField("lastName", StringType(), True),        
         StructField("firstName", StringType(), True),       
         StructField("gender", StringType(), True),         
         StructField("registration", LongType(), True),       
     ]
),

#DWH model schemas
    'user_dim_schema': StructType([
    StructField("userId", LongType(), False),
    StructField("firstName", StringType(), True),
    StructField("lastName", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("level", StringType(), True),
    StructField("registration", LongType(), True),
    StructField("grouped", LongType(), True),
    StructField("rowActivationDate", TimestampType(), True),
    StructField("rowExpirationDate", TimestampType(), True),
    StructField("currRow", IntegerType(), True)
]),

    'artist_dim_schema': StructType(
    [
        StructField("artistId", LongType(), True),  
        StructField("artist", StringType(), True),  
        StructField("lat", DoubleType(), True), 
        StructField("lon", DoubleType(), True), 
        StructField("city", StringType(), True),  
        StructField("state", StringType(), True),  
        StructField("rowActivationDate", LongType(), True),
        StructField("rowExpirationDate", LongType(), True),
        StructField("currRow", IntegerType(), False),  
    ]
),

    'datetime_dim_schema': StructType([
        StructField("datetimeId", LongType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("year", IntegerType(), True),
        StructField("month", IntegerType(), True),
        StructField("day", IntegerType(), True),
        StructField("hour", IntegerType(), True),
        StructField("minute", IntegerType(), True),
        StructField("second", IntegerType(), True),
        StructField("rowActivationDate", TimestampType(), True),
        StructField("rowExpirationDate", TimestampType(), True),
        StructField("currRow", IntegerType(), False),  
]),

    'location_dim_schema': StructType(
    [
        StructField("locationId", LongType(), True),
        StructField("city", StringType(), True),
        StructField("zip",  IntegerType(), True),
        StructField("state", StringType(), True),
        StructField("lon", DoubleType(), True),
        StructField("lat", DoubleType(), True),
        StructField("rowActivationDate", TimestampType(), True),
        StructField("rowExpirationDate", TimestampType(), True),
        StructField("currRow", IntegerType(), False), 
    ]
),

    'song_dim_schema': StructType(
    [
        StructField("songId", LongType(), True),  
        StructField("song", StringType(), True),  
        StructField("artist", StringType(), True),  
        StructField("duration", DoubleType(), True),  
        StructField("rowActivationDate", TimestampType(), True),
        StructField("rowExpirationDate", TimestampType(), True),
        StructField("currRow", IntegerType(), False), 
    ]
),

    'session_dim_schema': StructType( 
    [
        StructField("sessionId", LongType(), True), 
        StructField("startTime", TimestampType(), True), 
        StructField("endTime", TimestampType(), True), 
        StructField("rowActivationDate", TimestampType(), True),
        StructField("rowExpirationDate", TimestampType(), True),
        StructField("currRow", IntegerType(), False), 
    ]
),

    'fact_streams_schema': StructType([
    StructField("UserIdSK", LongType(), True),
    StructField("ArtistIdSK", LongType(), True),
    StructField("SongSK", LongType(), True),
    StructField("LocationSK", LongType(), True),
    StructField("DateTimeSK", LongType(), True),
    StructField("SessionIdSK", LongType(), True),
    StructField("ts", LongType(), True),
    StructField("year", IntegerType(), True),
    StructField("month", IntegerType(), True),
    StructField("day", IntegerType(), True),
    StructField("hour", IntegerType(), True),
    StructField("minute", IntegerType(), True),
    StructField("second", IntegerType(), True),
    StructField("isMoving", IntegerType(), True), 
    StructField("isSongSkipped", IntegerType(), True), 
    StructField("isFirstListenEvent", IntegerType(), True),  
    StructField("isLastListenEvent", IntegerType(), True),  
    StructField("nextListenEventTimeGap", LongType(), True),
    StructField("consecutiveNoSong", IntegerType(), True),
    StructField("rowActivationDate", LongType(), True),
    StructField("rowExpirationDate", LongType(), True),
    StructField("currRow", IntegerType(), False), 
])
}