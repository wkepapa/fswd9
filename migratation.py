from pymongo import MongoClient
import psycopg2


mongo_uri = "mongodb+srv://2023shivamchaugule:Killjoycr@clustermongofswd.ebobtqh.mongodb.net/?retryWrites=true&w=majority&appName=Clustermongofswd"
mongo_client = MongoClient(mongo_uri)
mongo_db = mongo_client['movies']
mongo_collection = mongo_db['shivcollection']

pg_connection = psycopg2.connect(
    host="localhost",
    database="Library",
    user="postgres",
    password="postgres"
)
pg_cursor = pg_connection.cursor()


for doc in mongo_collection.find():
    title = doc.get('name')  
    year = int(doc.get('year')) if doc.get('year') else None
    rating = float(doc.get('rating')) if doc.get('rating') else None

    
    pg_cursor.execute(
        "INSERT INTO movies (title, year_published, rating) VALUES (%s, %s, %s)",
        (title, year, rating)
    )

pg_connection.commit()
pg_cursor.close()
pg_connection.close()
mongo_client.close()

print(" Data migration complete!")
