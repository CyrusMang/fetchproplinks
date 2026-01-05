import os
from pymongo import MongoClient
from dotenv import load_dotenv
from openai import AzureOpenAI
from langchain_text_splitters import RecursiveCharacterTextSplitter

load_dotenv()

rating_threshold = 3.7
batch_size = 5

MONGODB_CONNECTION_STRING = os.getenv("MONGODB_CONNECTION_STRING")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_API_ENDPOINT = os.getenv("OPENAI_API_ENDPOINT")
OPENAI_API_VERSION = os.getenv("OPENAI_API_VERSION")

openai_client = AzureOpenAI(
    azure_endpoint = OPENAI_API_ENDPOINT, 
    api_key=OPENAI_API_KEY, 
    api_version=OPENAI_API_VERSION
)
embedding_model = 'text-embedding-3-large'

def get_embedding(text):
   embedding = openai_client.embeddings.create(input=[text], model=embedding_model).data[0].embedding
   return embedding

def text_split(text):
   text_splitter = RecursiveCharacterTextSplitter(
       chunk_size=500,
       chunk_overlap=50
   )
   return text_splitter.split_text(text)

def main():
   client = MongoClient(MONGODB_CONNECTION_STRING)
   db = client['prop_main']
   collection = db['props']
   props = collection.find({
      'summary': { '$exists': True },
   }).limit(batch_size)
   for prop in props:
      text = prop['summary']
      if not text or len(text) < 50:
         continue
      # chunks = text_split(text)
      # embeddings = []
      # for chunk in chunks:
      #    embedding = get_embedding(chunk)
      #    embeddings.append({ 'chunk': chunk, 'embedding': embedding })
      embedding = get_embedding(text)
      collection.update_one(
         { 'id': prop['id'] },
         { '$set': { 'embedding': embedding } }
      )
      print(f"Updated place {prop['id']} with embeddings.")
   client.close()

if __name__ == '__main__':
   main()