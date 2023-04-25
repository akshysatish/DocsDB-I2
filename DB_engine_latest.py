import pickle
import json
import os
import uuid
import utils
import updateUtils
import datetime

class Connection:
    def __init__(self, host_path=None):
        '''
            1. Connect to database instead of localhost port
            2. If localhost folder already exists:
                load the db.pkl file
               else: 
                create an empty db.pkl file
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
    
    def getCollectionNames(self):
        print(self.collection_list)
            
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
    
    def delete_collection(self, collection_name):
        json_name = collection_name + '.json'
        json_path = os.path.normpath(os.path.join(self.host_path, self.db_name, json_name))
        if not os.path.exists(json_path):
            raise FileExistsError("The collection does not exist!")
        # Delte .json file
        os.remove(json_path)
        # Update collection.pkl as record
        with open(self.collection_pkl_path, 'rb') as f:
            self.collection_list = pickle.load(f)
        self.collection_list.remove(collection_name)
        with open(self.collection_pkl_path, 'wb') as f:
            pickle.dump(self.collection_list, f)
        # Update list of attributes
        self.collection.pop(collection_name)

class DocCollection:
    def __init__(self, host_path, db_name, collection_name):
            self.host_path = host_path
            self.db_name = db_name
            self.collection_name = collection_name
      
    #Insert method - to insert documents          
    def insert(self, jsonDocument):
            file_name = self.collection_name + '.json'
            file_path = os.path.normpath(os.path.join(self.host_path, self.db_name, file_name))
            doc_ids = []
            jsonDocuments = utils.insertTypeCheck(jsonDocument)
                  
            with open(file_path, 'r') as f:
                  data = json.load(f)
            for doc in jsonDocuments:
                  hash_key = str(uuid.uuid4())
                  doc['_id'] = hash_key
                  for key, value in doc.items():
                          doc[key] = {"value": value, "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")}
                  doc_ids.append(hash_key)
                  data.append(doc)

            # Write the updated documents to the file
            with open(file_path, 'w') as f:
                  json.dump(data, f)

            self.createPrimaryIndex('_id', 3000)
            return doc_ids   
    
    #Delete method - to delete documents based on condition
    def delete(self, cjsonDocument):
            file_name = self.collection_name + '.json'
            file_path = os.path.normpath(os.path.join(self.host_path, self.db_name, file_name))

            jsonDocuments = utils.condTypeCheck(cjsonDocument)

            ddata = utils.getData(file_path)
            if jsonDocuments != {}:
                  del_doc_ids = utils.conditionEvaluation(jsonDocuments, ddata)
                  
            else:
                  print("No condition provided. Deletes all documents")
                  del_doc_ids = []
                  for doc in ddata:
                       del_doc_ids.append(doc["_id"])

            data = []
            for doc in ddata:
                  for i in del_doc_ids:
                        if doc['_id'] == i:
                              data.append(doc)
            
            for d in data:
                 ddata.remove(d)

            # Write the updated documents to the file
            with open(file_path, 'w') as f:
                  json.dump(ddata, f)

            return del_doc_ids

    #Delete method - to delete documents based on condition
    def indexdelete(self, cjsonDocument):
        self.delete(cjsonDocument=cjsonDocument)
        file_name = 'index.pkl'
        file_path = os.path.normpath(os.path.join(self.host_path, self.db_name, file_name))
        with open(file_path, "rb") as f:
                index_dict = pickle.load(f)
        index_list = index_dict[self.collection_name]
             
        jsonDocuments = utils.condTypeCheck(cjsonDocument)
        if jsonDocuments != {}:
                del_docs = self.indconditionEvaluation(jsonDocuments)        
        else:
                print("No condition provided.")
                del_docs = []

        fields = utils.getfields(jsonDocuments)
        for doc in del_docs:
            for d in doc:
                print(d)
                for key, value in d.items():
                    if key in index_list:
                        index_file_name = self.collection_name + '_' + key + '.pkl'
                        index_file_path = os.path.normpath(os.path.join(self.host_path, self.db_name, index_file_name))
                        with open(index_file_path, "rb") as f:
                            test = pickle.load(f)
                        if key in fields:
                            test.delete(fields[key], doc, jsonDocuments)
                        with open(index_file_path, "wb") as f:
                            pickle.dump(test, f)
                        index_list.remove(key)
                    for key in index_list:
                        index_file_name = self.collection_name + '_' + key + '.pkl'
                        index_file_path = os.path.normpath(os.path.join(self.host_path, self.db_name, index_file_name))
                        with open(index_file_path, "rb") as f:
                            test = pickle.load(f)
                        test.delete(d[key], doc)
                        with open(index_file_path, "wb") as f:
                            pickle.dump(test, f)

        return None
    
    #Update method - to update documents based on condition and update to be made
    def update(self, conditionalJsonDocument, toBeUpdatedJsonDocument):
            file_name = self.collection_name + '.json'
            file_path = os.path.normpath(os.path.join(self.host_path, self.db_name, file_name))

            doc_ids = []
            
            data = utils.getData(file_path)
            updated_docs = []
            cJsonDocuments = utils.condTypeCheck(conditionalJsonDocument)
            if cJsonDocuments != {}:
                  # ids = conditionEvaluation(cJsonDocuments, data)
                  # doc_ids = list(set([item for sublist in ids for item in sublist]))
                  doc_ids = utils.conditionEvaluation(cJsonDocuments, data)
                  for i in doc_ids:
                        for doc in data:
                              if doc['_id'] in i:
                                    updated_docs.append(doc)
            else:
                  updated_docs = data

            jsonDocuments = utils.insertTypeCheck(toBeUpdatedJsonDocument)
            if jsonDocuments != []:
                for jsonDocument in jsonDocuments:
                  for key, value in jsonDocument.items():
                        if updateUtils.updateConstraints(key):
                              updated_docs = updateUtils.updateData(key, updated_docs, value)
                for doc in updated_docs:
                     for key, value in doc.items():
                         if key != '_id':
                             doc[key]['latest_timestamp'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
            else:
                  print("No update provided.")

            # Write the updated documents to the file
            with open(file_path, 'w') as f:
                  json.dump(data, f)
            
                        
            return updated_docs
    
    def project(self, jsonDocument, data):
            documents = []
            jsonDocuments = utils.condTypeCheck(jsonDocument)
            for key, value in jsonDocuments.items():
                  if key == "$project":
                        try:
                              json.loads(json.dumps(value))
                              for doc in data:
                                    jsonObjectToBeReturned = {}
                                    for key2, value2 in value.items():
                                          if value2 == 1:
                                                jsonObjectToBeReturned[key2] = doc[key2]
                                          else:
                                                continue
                                    documents.append(jsonObjectToBeReturned)
                        
                        except: json.JSONDecodeError

            return documents
    
    #Read all method - to read all documents based on columns specified
    def readAll(self, jsonDocument):
            file_name = self.collection_name + '.json'
            file_path = os.path.normpath(os.path.join(self.host_path, self.db_name, file_name))
            data = utils.getData(file_path)
            jsonDocuments = utils.condTypeCheck(jsonDocument)
            if jsonDocuments == {}:
                  return data
            else:
                  docs = self.project(jsonDocuments, data)
                  return docs

    #Read specific method - to read documents based on columns specified and conditons matched
    def readSpecific(self, jsonDocument, conditionalJsonDocument):
            file_name = self.collection_name + '.json'
            file_path = os.path.normpath(os.path.join(self.host_path, self.db_name, file_name))
            doc_ids = []
            updated_docs = []
            data = utils.getData(file_path)
            cJsonDocuments = utils.condTypeCheck(conditionalJsonDocument)
            if cJsonDocuments != {}:
                doc_ids = utils.conditionEvaluation(cJsonDocuments, data)
                for doc in data:
                    if doc['_id'] in doc_ids:
                        for key, value in doc.items():
                            if key != '_id' and doc[key]['latest_timestamp'] < datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"):
                                updated_docs.append(doc)
            else:
                  updated_docs = data
            
            jsonDocuments = utils.condTypeCheck(jsonDocument)
            
            if jsonDocuments != {}:
                  final_docs = self.project(jsonDocument, updated_docs)
                  return final_docs
            else:
                  return updated_docs
    
    def indnestedconditionEvaluation(self, conditionalJsonDocument, nkey):
        docs = []
        for key, value in conditionalJsonDocument.items():
            if utils.conditionalConstraintCheck(key):
                index_file_name = self.collection_name + '_' + nkey + '.pkl'
                index_file_path = os.path.normpath(os.path.join(self.host_path, self.db_name, index_file_name))
                id_file_name = self.collection_name + '_' + '_id.pkl'
                id_file_path = os.path.normpath(os.path.join(self.host_path, self.db_name, id_file_name))
                if not os.path.exists(index_file_path):
                    self.createSecondaryIndex(nkey, 200)
                with open(index_file_path, "rb") as f:
                    test = pickle.load(f)
                docids = test.retrieve_new(value, key)
                with open(id_file_path, "rb") as f:
                    testid = pickle.load(f)
                for i in docids:
                    docs.append(testid.retrieve_new(i, '$eq'))

            elif utils.is_nested_dict(value):#
                nested_docs = self.indnestedconditionEvaluation(value, nkey)
                for i in nested_docs:
                    for d in i:
                        docs.append(d)
            elif key == "$and":
                docs.append(self.indandEvaluation(value))
            elif key == "$or":
                docs.append(self.indorEvaluation(value))
            else:
                #deal with it
                print('')

        return docs
    
    def indandEvaluation(self, jsonDocument):
        final_docs = []
        
        if isinstance(jsonDocument, str):
            jsonDocuments = json.loads(jsonDocument)
        elif isinstance(jsonDocument, dict):
            jsonDocuments = [jsonDocument]
        elif isinstance(jsonDocument, (list, dict)):
            jsonDocuments = jsonDocument    
        else:
            raise TypeError("unsupported types(s) for input")

        for index, doc in enumerate(jsonDocuments):
            for key, value in doc.items():
                docs = []
                if key == "$or":
                    temp_docs = []
                    or_docs = self.indorEvaluation(value)
                    for i in or_docs:
                        temp_docs.append(i)
                    docs = temp_docs
                
                elif utils.is_nested_dict(value):#
                    temp_docs = []
                    nested_docs = self.indnestedconditionEvaluation(value, key)
                    for i in nested_docs:
                        for d in i:
                            temp_docs.append(d)
                    docs = temp_docs
                
                else:
                    #create/use indexes
                    index_file_name = self.collection_name + '_' + key + '.pkl'
                    index_file_path = os.path.normpath(os.path.join(self.host_path, self.db_name, index_file_name))
                    id_file_name = self.collection_name + '_' + '_id.pkl'
                    id_file_path = os.path.normpath(os.path.join(self.host_path, self.db_name, id_file_name))
                    if not os.path.exists(index_file_path):
                        self.createSecondaryIndex(key, 200)
                    with open(index_file_path, "rb") as f:
                        test = pickle.load(f)
                    docids = test.retrieve_new(value, '$eq')
                    with open(id_file_path, "rb") as f:
                        testid = pickle.load(f)
                    for i in docids:
                         docs.append(testid.retrieve_new(i, '$eq'))
                         
                    
            final_docs.append(docs)
        
        common_dicts = []
        print(final_docs)
        for d in final_docs[0]:
            if all(d in sublist for sublist in final_docs[1:]):
                common_dicts.append(d)
        return common_dicts

    def indorEvaluation(self, jsonDocument):
        docs = []
        
        if isinstance(jsonDocument, str):
            jsonDocuments = json.loads(jsonDocument)
        elif isinstance(jsonDocument, dict):
            jsonDocuments = [jsonDocument]
        elif isinstance(jsonDocument, (list, dict)):
            jsonDocuments = jsonDocument    
        else:
            raise TypeError("unsupported types(s) for input")

        for index, doc in enumerate(jsonDocuments):
            for key, value in doc.items():
                if key == "$and":
                    temp_docs = []
                    or_docs = self.indandEvaluation(value)
                    for i in or_docs:
                        temp_docs.append(i)
                    docs.append(temp_docs)
                
                elif utils.is_nested_dict(value):#
                    temp_docs = []
                    nested_docs = self.indnestedconditionEvaluation(value, key)
                    for i in nested_docs:
                        for d in i:
                            docs.append(d)
                
                else:
                    #create/use indexes
                    index_file_name = self.collection_name + '_' + key + '.pkl'
                    index_file_path = os.path.normpath(os.path.join(self.host_path, self.db_name, index_file_name))
                    id_file_name = self.collection_name + '_' + '_id.pkl'
                    id_file_path = os.path.normpath(os.path.join(self.host_path, self.db_name, id_file_name))
                    if not os.path.exists(index_file_path):
                        self.createSecondaryIndex(key, 200)
                    with open(index_file_path, "rb") as f:
                        test = pickle.load(f)
                    docids = test.retrieve_new(value, '$eq')
                    with open(id_file_path, "rb") as f:
                        testid = pickle.load(f)
                    for i in docids:
                         docs.append(testid.retrieve_new(i, '$eq'))
        
        #flattened_doc_ids = list(set([item for sublist in doc_ids for item in sublist]))
        #return list(set(flattened_doc_ids))
        res_list = [i for n, i in enumerate(docs) if i not in docs[n + 1:]]
        return res_list
    
    def indconditionEvaluation(self, conditionalJsonDocument):
        docs = []
        for key, value in conditionalJsonDocument.items():
            if utils.is_nested_dict(value):#
                nested_docs = self.indnestedconditionEvaluation(value, key)
                #handle this
                for i in nested_docs:
                    docs.append(i)
            elif key == "$and":
                docs.append(self.indandEvaluation(value))
            elif key == "$or":
                docs.append(self.indorEvaluation(value))
            else:
                #create/use indexes
                index_file_name = self.collection_name + '_' + key + '.pkl'
                index_file_path = os.path.normpath(os.path.join(self.host_path, self.db_name, index_file_name))
                id_file_name = self.collection_name + '_' + '_id.pkl'
                id_file_path = os.path.normpath(os.path.join(self.host_path, self.db_name, id_file_name))
                if not os.path.exists(index_file_path):
                     self.createSecondaryIndex(key, 200)
                with open(index_file_path, "rb") as f:
                    test = pickle.load(f)
                docids = test.retrieve_new(value, '$eq')
                with open(id_file_path, "rb") as f:
                        testid = pickle.load(f)
                for i in docids:
                    docs.append(testid.retrieve_new(i, '$eq'))

        return docs
        
    def indexedreadSpecific(self, jsonDocument, conditionalJsonDocument):
            file_name = self.collection_name + '.json'
            file_path = os.path.normpath(os.path.join(self.host_path, self.db_name, file_name))
            ind_file_name = 'index.pkl'
            ind_file_path = os.path.normpath(os.path.join(self.host_path, self.db_name, ind_file_name))
            doc_ids = []
            updated_docs = []
            cJsonDocuments = utils.condTypeCheck(conditionalJsonDocument)
            if cJsonDocuments != {}:
                updated_docs = self.indconditionEvaluation(cJsonDocuments)
                '''
                for doc in data:
                    if doc['_id'] in doc_ids:
                        updated_docs.append(doc)
                '''
            else:
                ind_file_name = self.collection_name + '__id.pkl'
                ind_file_path = os.path.normpath(os.path.join(self.host_path, self.db_name, ind_file_name))
                with open(ind_file_path, "rb") as f:
                    test = pickle.load(f)
                updated_docs = test.retrieve_all()

            jsonDocuments = utils.condTypeCheck(jsonDocument)
            
            if jsonDocuments != {}:
                  final_docs = self.project(jsonDocument, updated_docs)
                  return final_docs
            else:
                  return updated_docs
    
    def join(self, localData, foreignData, localField, foreignField):
        right_dict = {}
        for row in foreignData:
            key = row[foreignField]
            if key not in right_dict:
                right_dict[key] = []
            right_dict[key].append(row)
        # Perform the join operation
        result = []
        for row in localData:
            key = row[localField]
            if key in right_dict:
                for right_row in right_dict[key]:
                    result.append({**row, **right_row})
        return result

    def lookup(self, db_name, collection_name, db_name2, collection_name2, localField, foreignField):
        coll1file = collection_name + '.json'
        coll1_path = os.path.normpath(os.path.join(self.host_path, db_name, coll1file))
        if not os.path.exists(coll1_path):
             raise FileNotFoundError(db_name + "\\" + collection_name + ".json does not exist")
        coll2file = collection_name2 + '.json'
        coll2_path = os.path.normpath(os.path.join(self.host_path, db_name2, coll2file))
        if not os.path.exists(coll2_path):
             raise FileNotFoundError(db_name2 + "\\" + collection_name2 + ".json does not exist")
        localData = utils.getData(coll1_path)
        foreignData = utils.getData(coll2_path)
        results = self.join(localData, foreignData, localField, foreignField)
        # Filtering/Querying conditions
        # result_filtered = [row for row in results if row.get('product') in ('Widget', 'Gadget')]
        # return result_filtered
        return results
            
    # Create index
    def createPrimaryIndex(self, field: str, order) -> None:
        file_name = 'index.pkl'
        file_path = os.path.normpath(os.path.join(self.host_path, self.db_name, file_name))
        if not os.path.exists(file_path):
            index_dict = {}
            index_dict[self.collection_name] = [field]
            with open(file_path, "wb") as f:
                pickle.dump(index_dict, f)
        else:
            with open(file_path, "rb") as f:
                index_dict = pickle.load(f)
            index_list = index_dict[self.collection_name]
            if field in index_list:
                raise FileExistsError("The index already exists!")
            else:
                index_list.append(field)
                index_dict[self.collection_name] = index_list
                with open(file_path, "wb") as f:
                    pickle.dump(index_dict, f)
        
        # Initiate B-plus tree structure
        index = utils.BplusTree(order)
        # Fetch all documents in the collection
        for doc in self.readAll('{}'):
            key = doc.get(field)
            if key:
                index.insert(key["value"], doc)
        # Export index
        index_file_name = self.collection_name + '_' + field + '.pkl'
        index_file_path = os.path.normpath(os.path.join(self.host_path, self.db_name, index_file_name))
        with open(index_file_path, "wb") as f:
            pickle.dump(index, f)
        print(f'Index: {field} under {self.collection_name} is created!')
        return None
    
    # Create index
    def createSecondaryIndex(self, field: str, order) -> None:
        file_name = 'index.pkl'
        file_path = os.path.normpath(os.path.join(self.host_path, self.db_name, file_name))
        if not os.path.exists(file_path):
            index_dict = {}
            index_dict[self.collection_name] = [field]
            with open(file_path, "wb") as f:
                pickle.dump(index_dict, f)
        else:
            with open(file_path, "rb") as f:
                index_dict = pickle.load(f)
            index_list = index_dict[self.collection_name]
            if field in index_list:
                raise FileExistsError("The index already exists!")
            else:
                index_list.append(field)
                index_dict[self.collection_name] = index_list
                with open(file_path, "wb") as f:
                    pickle.dump(index_dict, f)
        
        # Initiate B-plus tree structure
        index = utils.BplusTree(order)
        # Fetch all documents in the collection
        for doc in self.readAll('{}'):
            key = doc.get(field)
            if key:
                index.insert(key["value"], doc["_id"]['value'])
        # Export index
        index_file_name = self.collection_name + '_' + field + '.pkl'
        index_file_path = os.path.normpath(os.path.join(self.host_path, self.db_name, index_file_name))
        with open(index_file_path, "wb") as f:
            pickle.dump(index, f)
        print(f'Index: {field} under {self.collection_name} is created!')
        return None