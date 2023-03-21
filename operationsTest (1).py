import os
import json
import hashlib
import operations
import uuid

def insert(jsonDocument, db_name, collection_name):

    doc_ids = []

    jsonDocuments = json.loads(jsonDocument)

    # Generate a unique hash key for the document
    for doc in jsonDocuments:
        hash_key = str(uuid.uuid4())
        doc['_id'] = hash_key
        doc_ids.append(hash_key)
            
    # Create the folder if it does not exist
    if not os.path.exists(db_name):
        os.makedirs(db_name)

    # Create the file if it does not exist
    file_path = os.path.join(db_name, f'{collection_name}.json')
    if os.path.exists(file_path):
        pass 
    else:
        with open(file_path, 'w') as f:
            f.write('[]')

    # Load the existing documents from the file
    with open(file_path, 'r') as f:
            data = json.load(f)

    # Append the new documents to the existing data
    for d in jsonDocuments:
      data.append(d)
    
    print(data)
    # Write the updated documents to the file
    with open(file_path, 'w') as f:
            json.dump(data, f)

    return doc_ids


def getData(db_name, collection_name):
    # Check if the database exists
    if not os.path.exists(db_name):
        return []

    # Check if the collection exists
    file_path = os.path.join(db_name, f'{collection_name}.json')
    if not os.path.exists(file_path):
        return []

    # Load all documents from the collection file
    with open(file_path, 'r') as f:
        data = json.load(f)

    return data

def readAll(db_name, collection_name):
    data = getData(db_name, collection_name)
    return data


#gt
def gt(data, attributeKey, attributeValue):
    doc_ids = []
    for doc in data:
        if doc[attributeKey] > attributeValue:
            doc_ids.append(doc['_id'])

    return doc_ids

#lt
def lt(data, attributeKey, attributeValue):
    doc_ids = []
    for doc in data:
        if doc[attributeKey] < attributeValue:
            doc_ids.append(doc['_id'])

    return doc_ids

#lte
def lte(data, attributeKey, attributeValue):
    doc_ids = []
    for doc in data:
        if doc[attributeKey] <= attributeValue:
            doc_ids.append(doc['_id'])

    return doc_ids

#gte
def gte(data, attributeKey, attributeValue):
    doc_ids = []
    for doc in data:
        if doc[attributeKey] >= attributeValue:
            doc_ids.append(doc['_id'])

    return doc_ids

#eq
def eq(data, attributeKey, attributeValue):
    doc_ids = []
    for doc in data:
        if doc[attributeKey] == attributeValue:
            doc_ids.append(doc['_id'])

    return doc_ids

#ne
def ne(data, attributeKey, attributeValue):
    doc_ids = []
    for doc in data:
        if doc[attributeKey] != attributeValue:
            doc_ids.append(doc['_id'])

    return doc_ids

#in
def mongo_in(data, attributeKey, attributeValue):
    doc_ids = []
    for doc in data:
        if doc[attributeKey] == attributeValue:
            doc_ids.append(doc['_id'])
    return doc_ids



def checkKey(key, data, attributeKey, attributeValue):
    if key == "$in":
        return mongo_in(data, attributeKey, attributeValue)
    elif key == "$gt":
        return gt(data, attributeKey, attributeValue)
    elif key == "$lt":
        return lt(data, attributeKey, attributeValue)
    elif key == "$lte":
        return lte(data, attributeKey, attributeValue)
    elif key == "$gte":
        return gte(data, attributeKey, attributeValue)
    elif key == "$eq":
        return eq(data, attributeKey, attributeValue)
    elif key == "$ne":
        return ne(data, attributeKey, attributeValue)
    else:
        return ""


def conditionalConstraintCheck(key):
    if key == "$in":
        return True
    elif key == "$gt":
        return True
    elif key == "$lt":
        return True
    elif key == "$lte":
        return True
    elif key == "$gte":
        return True
    elif key == "$eq":
        return True
    elif key == "$ne":
        return True
    else:
        return False


def readSpecific(jsonDocument, db_name, collection_name):
    doc_ids = []
    data = getData(db_name, collection_name)
    jsonDocuments = json.loads(jsonDocument)
    for key, value in jsonDocuments.items():
        try:
            json.loads(json.dumps(value))
            for key2, value2 in value.items():
                if conditionalConstraintCheck(key2):
                    doc_ids = checkKey(key2, data, key, value2)
                else:
                    for doc in data:
                        if doc[key] == value:
                            doc_ids = doc['_id']
        except: json.JSONDecodeError

    documents = [] 
    for doc in data:
        for i in doc_ids:
            if doc['_id'] == i:
                documents.append(doc)

    return documents

def update(conditionalJsonDocument, toBeUpdatedJsonDocument, db_name, collection_name):
    doc_ids = []
    data = getData(db_name, collection_name)
    jsonDocuments = json.loads(conditionalJsonDocument)
    for key, value in jsonDocuments.items():
        try:
            json.loads(json.dumps(value))
            for key2, value2 in value.items():
                if conditionalConstraintCheck(key2):
                    doc_ids.append(checkKey(key2, data, key, value2))
                else:
                    for doc in data:
                        if doc[key] == value:
                            doc_ids.append(doc['_id'])
        except: json.JSONDecodeError

    flattened_doc_ids = list(set([item for sublist in doc_ids for item in sublist]))

    documentsToBeUpdated = json.loads(toBeUpdatedJsonDocument)
    for key, value in documentsToBeUpdated.items():
        if key == "$set":
            for doc in data:
                for key2, value2 in value.items():
                    for i in flattened_doc_ids:
                        if doc['_id'] == i:
                            doc[key2] = value2
        else:
            continue

    file_path = os.path.join(db_name, f'{collection_name}.json')

    # Write the updated documents to the file
    with open(file_path, 'w') as f:
        json.dump(data, f)
    
            
    return flattened_doc_ids


def delete(jsonDocument, db_name, collection_name):
    data = getData(db_name, collection_name)
    jsonDocuments = json.loads(jsonDocument)
    for key, value in jsonDocuments.items():
        try:
            json.loads(json.dumps(value))
            for key2, value2 in value.items():
                
                if conditionalConstraintCheck(key2):
                    doc_ids = checkKey(key2, data, jsonDocuments)
                
                else:
                    for doc in data:
                        if doc[key] == value:
                            doc_ids = doc['_id']
        
        except: json.JSONDecodeError

    for doc in data:
        for i in doc_ids:
            if doc['_id'] == i:
                data.remove(doc) #remove the document from the file ???

    file_path = os.path.join(db_name, f'{collection_name}.json')

    # Write the updated documents to the file
    with open(file_path, 'w') as f:
        json.dump(data, f)

    return "Deleted "+doc_ids
        


#docids = insert('[{"name": "ryan", "age": 24}]', "myDB", "myCollection")

#docids2 = insert('[{"name": "yash", "age": 24}]', "myDB", "myCollection")
#print(docids + docids2)
#read = readAll("myDB", "myCollection")
#print(read)

# read = readSpecific('{"age": {"$gt": 29}}', "myDB", "myCollection")
# print(read)


update = update('{"age": {"$lt": 25}}','{"$set": {"age": 25}}', "myDB", "myCollection")
print(update)

#delete = delete('{"age": {"$lt": 29}}', "myDB", "myCollection")
#print(delete)