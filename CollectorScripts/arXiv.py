import os
import requests
from pymongo import MongoClient
import PyPDF2
import io
from pymongo.server_api import ServerApi
from dotenv import load_dotenv
import xml.etree.ElementTree as ET
import json
import re
from summa import keywords

# Load environment variables from .env file
load_dotenv(dotenv_path='../.env')

TOPIC = 'astro-ph.SR'

def extract_keywords(text):
    text = re.sub('[^a-zA-Z]', ' ', text)

    keys = keywords.keywords(text)

    return keys

def check_mongo_connection():
    # Create a new client and connect to the server
    client = MongoClient(os.environ['MONGO_CONNECTION_STRING'], server_api=ServerApi('1'))
    # Send a ping to confirm a successful connection
    try:
        client.admin.command('ping')
        print("Pinged your deployment. You successfully connected to MongoDB!")
    except Exception as e:
        print(f"Failed to connect to MongoDB: {e}")

def get_mongo_collection():
    try:
        client = MongoClient(os.environ['MONGO_CONNECTION_STRING'])
        db = client[os.environ['DB_NAME']]
        collection = db[os.environ['COLLECTION_NAME']]
        return collection
    except Exception as e:
        print(f"Failed to get MongoDB collection: {e}")
        return None

def fetch_arxiv_data(start, max_results):
    try:
        response = requests.get(f'http://export.arxiv.org/api/query?search_query=cat:{TOPIC}&start={start}&max_results={max_results}')
        response.raise_for_status()
        
        # Parse the XML response
        root = ET.fromstring(response.content)

        # Create a list to store the entries
        entries = []

        # Iterate over each entry in the XML
        for xml_entry in root.findall('{http://www.w3.org/2005/Atom}entry'):
            # Create a dictionary to store the data for this entry
            entry = {}

            # Add data to the entry
            entry['id'] = xml_entry.find('{http://www.w3.org/2005/Atom}id').text
            entry['title'] = xml_entry.find('{http://www.w3.org/2005/Atom}title').text
            entry['published'] = xml_entry.find('{http://www.w3.org/2005/Atom}published').text
            entry['summary'] = xml_entry.find('{http://www.w3.org/2005/Atom}summary').text
            
            authors = xml_entry.findall('{http://www.w3.org/2005/Atom}author/{http://www.w3.org/2005/Atom}name')
            entry['authors'] = [author.text for author in authors]

            categories = xml_entry.findall('{http://www.w3.org/2005/Atom}category')
            entry['keywords'] = [category.attrib['term'] for category in categories]

            # Add the entry to the list of entries
            entries.append(entry)

        # Return the list of entries as JSON-like object
        return {'entries': entries}

    except requests.RequestException as e:
        print(f"Failed to fetch data from arXiv API: {e}")
        return None

def download_pdf(pdf_url):
    try:
        pdf_response = requests.get(pdf_url)
        pdf_response.raise_for_status()
        pdf_file = io.BytesIO(pdf_response.content)
        return pdf_file
    except requests.RequestException as e:
        print(f"Failed to download PDF: {e}")
        return None

def extract_text_from_pdf(pdf_file):
    try:
        pdf_reader = PyPDF2.PdfReader(pdf_file)
        text = ''
        for page_num in range(len(pdf_reader.pages)):
            page = pdf_reader.pages[page_num]
            text += page.extract_text()
        return text
    except PyPDF2.PdfReadError as e:
        print(f"Failed to read PDF: {e}")
        return ''

def fetch_citation_data(arxiv_id):
    url = f"https://inspirehep.net/api/literature?q=arxiv:{arxiv_id}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        if 'hits' in data and 'hits' in data['hits'] and len(data['hits']['hits']) > 0:
            return data['hits']['hits'][0]['metadata']['citation_count']
        else:
            print(f"No citation data found for arXiv ID: {arxiv_id}")
            return None
    except requests.RequestException as e:
        print(f"Failed to fetch citation data: {e}")
        return None

def main():
    collection = get_mongo_collection()
    if collection is None:
        return

    start = 0
    max_results = 10
    count = 0

    while True:
        data = fetch_arxiv_data(start, max_results)

        # print(json.dumps(data, indent=2))
        
        if data is None or not data['entries']:
            break

        for entry in data['entries']:
            pdf_url = entry['id'].replace('http://arxiv.org/abs/', 'http://arxiv.org/pdf/') + '.pdf'
            # pdf_file = download_pdf(pdf_url)
            
            # if pdf_file is None:
            #     continue
            
            # text = extract_text_from_pdf(pdf_file)

            doi = entry['id'].split('/')[-1]
            citation_data = fetch_citation_data(doi)

            keywords = extract_keywords(entry['summary'])

            # print(citation_data)

            paper = {
                'id': entry['id'],
                'title': entry['title'],
                'authors': entry['authors'],
                'published': entry['published'],
                'summary': entry['summary'],
                'keywords': keywords,
                # 'text': text,
                'doi': doi,
                'citations': citation_data,
            }
            
            try:
                collection.insert_one(paper)
                count += 1
                print(f"Total papers inserted so far: {count}")
            except Exception as e:
                print(f"Failed to insert paper into MongoDB: {e}")
            

        start += max_results

if __name__ == "__main__":
    main()
