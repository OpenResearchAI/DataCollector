import os
from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.server_api import ServerApi

# Load environment variables from .env file
load_dotenv(dotenv_path='../.env')

# Get MongoDB credentials from environment variables
MONGODB_URI = os.getenv('MONGO_CONNECTION_STRING')
DB_NAME = os.getenv('DB_NAME')
COLLECTION_NAME = os.getenv('COLLECTION_NAME')

def check_mongo_connection():
    # Create a new client and connect to the server
    client = MongoClient(os.environ['MONGO_CONNECTION_STRING'], server_api=ServerApi('1'))
    # Send a ping to confirm a successful connection
    try:
        client.admin.command('ping')
        print("Pinged your deployment. You successfully connected to MongoDB!")
    except Exception as e:
        print(f"Failed to connect to MongoDB: {e}")

check_mongo_connection()

# Create a MongoDB client
client = MongoClient(MONGODB_URI)

# Get the database
db = client[DB_NAME]

# Get the collection
collection = db[COLLECTION_NAME]

# Create a text index on the title, authors, summary, and keywords fields
collection.create_index([
    ("title", "text"),
    ("authors", "text"),
    ("summary", "text"),
    ("keywords", "text")
])

print('Updated successfully')