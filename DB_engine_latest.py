import pickle
import json
import os
import uuid

class Connection:
    def __init__(self, host_path=None):
        '''
            1. Connect to database instead of localhost port
            2. If localhost folder already exists:
                load the db.pkl file
               else: 
                creat an empty db.pkl file
            . Return "DocDatabase"
        '''
        self.db = {}
        # load db_list
        if host_path is None:
            if os.path.exists('localhost'):
                with open("./localhost/db.pkl", "rb") as f:
                    self.db_list = pickle.load(f)
            else:
                os.makedirs('localhost')
                self.db_list = []
                with open("./localhost/db.pkl", "wb") as f:
                    pickle.dump(self.db_list, f)
            self.host_path = os.path.normpath(os.path.join(os.getcwd(), "localhost"))
        else:
            self.host_path = host_path
            try:
                if os.path.exists(host_path):
                    if os.path.exists(self.db_pkl_path):
                        with open(self.db_pkl_path, "rb") as f:
                            self.db_list = pickle.load(f)
                    else:
                        print("The db.pkl does not exist.") 
                else:
                    print("The path for localhost does not exist.")
            except Exception as e:
                print(f"An error occurred: {e}")
        self.db_pkl_path = os.path.normpath(os.path.join(self.host_path, "db.pkl"))
        # Load db into connect as attributes
        for db_name in self.db_list:
            db_instance = DocDatabase(self.host_path, db_name)
            setattr(self, db_name, db_instance)
            self.db[db_name] = db_instance


    def __getitem__(self, key):
        return self.db[key]

    def __setitem__(self, key, value):
        self.db[key] = value

    def __delitem__(self, key):
        del self.db[key]

    def create_db(self, db_name):
        # Create a db folder
        db_path = os.path.join(self.host_path, db_name)
        if os.path.exists(db_path):
            raise FileExistsError("The database already exists!")
        else:
            os.mkdir(db_path)
        # Create an empty collection.pkl inside the folder
        collection_pkl_path = os.path.normpath(os.path.join(db_path, "collection.pkl"))
        with open(collection_pkl_path, "wb") as f:
            pickle.dump([], f)
        # Update db.pkl as record 
        with open(self.db_pkl_path, 'rb') as f:
            self.db_list = pickle.load(f)
        self.db_list.append(db_name)
        with open(self.db_pkl_path, 'wb') as f:
            pickle.dump(self.db_list, f)
        # Add to attribute
        db_instance = DocDatabase(self.host_path, db_name)
        setattr(self, db_name, db_instance)
        self.db[db_name] = db_instance

class DocDatabase:
    def __init__(self, host_path, db_name):
        # When init, search for DocDatabase pickle object or create a new one
        self.db_name = db_name
        self.host_path = host_path
        self.collection = {}
        self.collection_pkl_path = os.path.normpath(os.path.join(host_path, db_name, "collection.pkl"))
        with open(self.collection_pkl_path, 'rb') as f:
            self.collection_list = pickle.load(f)
        # Load collection into the DocDatabase instance
        for collection_name in self.collection_list:
            collection_instance = DocCollection(self.host_path, self.db_name, collection_name)
            setattr(self, collection_name, collection_instance)
            self.collection[collection_name] = collection_instance
            
    def __getitem__(self, key):
        return self.collection[key]

    def __setitem__(self, key, value):
        self.collection[key] = value

    def __delitem__(self, key):
        del self.collection[key]
            
    def create_collection(self, collection_name):
        json_name = collection_name + '.json'
        json_path = os.path.normpath(os.path.join(self.host_path, self.db_name, json_name))
        if os.path.exists(json_path):
            raise FileExistsError("The collection already exists!")
        # Initiate collection with empty list []
        with open(json_path, "w") as json_file:
            json.dump([], json_file)
            
        # Update collection.pkl as record 
        with open(self.collection_pkl_path, 'rb') as f:
            self.collection_list = pickle.load(f)
        self.collection_list.append(collection_name)
        with open(self.collection_pkl_path, 'wb') as f:
            pickle.dump(self.collection_list, f)
            
        # Add to attribute
        collection_instance = DocCollection(self.host_path, self.db_name, collection_name)
        setattr(self, collection_name, collection_instance)
        self.collection[collection_name] = collection_instance

class DocCollection:
    def __init__(self, host_path, db_name, collection_name):
        self.host_path = host_path
        self.db_name = db_name
        self.collection_name = collection_name
        
    def insert(self, jsonDocument):
        file_name = self.collection_name + '.json'
        file_path = os.path.normpath(os.path.join(self.host_path, self.db_name, file_name))
        doc_ids = []
        if isinstance(jsonDocument, str):
            jsonDocuments = json.loads(jsonDocument)
        elif isinstance(jsonDocument, dict):
            jsonDocuments = [jsonDocument]
        elif isinstance(jsonDocument, (list, dict)):
            jsonDocuments = jsonDocument    
        else:
            raise TypeError("unsupported types(s) for input")
            
        with open(file_path, 'r') as f:
            data = json.load(f)
        for doc in jsonDocuments:
            hash_key = str(uuid.uuid4())
            doc['_id'] = hash_key
            doc_ids.append(hash_key)
            data.append(doc)

        # Write the updated documents to the file
        with open(file_path, 'w') as f:
            json.dump(data, f)

        return doc_ids   