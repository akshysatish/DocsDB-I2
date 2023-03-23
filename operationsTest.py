import os
import json
import hashlib
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

def readAll(jsonDocument, db_name, collection_name):
    data = getData(db_name, collection_name)
    jsonDocuments = json.loads(jsonDocument)
    if jsonDocuments == {}:
        return data
    else:
        docs = project(jsonDocuments, data)
        return docs

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

#inc
# def inc(data, attributeKey, attributeValue):
#     doc_ids = []
#     for doc in data:
#         newVal = doc[attributeKey] + attributeValue
#         doc[attributeKey] = newVal
#         doc_ids.append(doc['_id'])

#dec
# def dec(data, attributeKey, attributeValue):
#     doc_ids = []
#     for doc in data:
#         newVal = doc[attributeKey] - attributeValue
#         doc[attributeKey] = newVal
#         doc_ids.append(doc['_id'])


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
        return []


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


def readSpecific(jsonDocument, conditionalJsonDocument, db_name, collection_name):
    doc_ids = []
    updated_docs = []
    data = getData(db_name, collection_name)
    cJsonDocuments = json.loads(conditionalJsonDocument)
    if cJsonDocuments != {}:
        doc_ids = conditionEvaluation(cJsonDocuments, data)
        for i in doc_ids:
            for doc in data:
                if doc['_id'] in i:
                    updated_docs.append(doc)
    else:
        updated_docs = data
    
    jsonDocuments = json.loads(jsonDocument)
    
    if jsonDocuments != {}:
        final_docs = project(jsonDocument, updated_docs)
        return final_docs
    else:
        return updated_docs

            
        
def conditionEvaluation(conditionalJsonDocument, data):
    doc_ids = []
    for key, value in conditionalJsonDocument.items():
        try:
            if key == "$and":
                doc_ids.append(andEvaluation(value, data))
            elif key == "$or":
                doc_ids.append(orEvaluation(value, data))
            elif json.loads(json.dumps(value)):
                for key2, value2 in value.items():
                    doc_ids.append(checkKey(key2, data, key, value2))
        except:
            for doc in data:
                if doc[key] == value:
                    doc_ids.append(doc['_id'])

    return doc_ids
    
 
def andEvaluation(jsonDocument, data):
    final_ids = []
    
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
            doc_ids = []
            try:
                if key == "$or":
                    doc_ids.append(orEvaluation(value, data))
                
                elif json.loads(json.dumps(value)):
                    for key2, value2 in value.items():
                        doc_ids.append(checkKey(key2, data, key, value2))
            except:
                temp_ids = []
                for doc in data:
                    if doc[key] == value:
                        temp_ids.append(doc['_id'])
                doc_ids.append(temp_ids)

        flattened_doc_ids = list(set([item for sublist in doc_ids for item in sublist]))
        final_ids.append(flattened_doc_ids)
    
    set_ids = []
    for i in final_ids:
        set_ids.append(set(i))
    
    return set_ids[0].intersection(*set_ids[1:])

def orEvaluation(jsonDocument, data):
    doc_ids = []
    
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
            try:
                if key == "$and":
                    doc_ids.append(andEvaluation(value, data))
                
                elif json.loads(json.dumps(value)):
                    for key2, value2 in value.items():
                        doc_ids.append(checkKey(key2, data, key, value2))
            except:
                temp_ids = []
                for doc in data:
                    if doc[key] == value:
                        temp_ids.append(doc['_id'])
                doc_ids.append(temp_ids)
    
    flattened_doc_ids = list(set([item for sublist in doc_ids for item in sublist]))
    return list(set(flattened_doc_ids))



def update(conditionalJsonDocument, toBeUpdatedJsonDocument, db_name, collection_name):
    doc_ids = []
    data = getData(db_name, collection_name)
    updated_docs = []
    cJsonDocuments = json.loads(conditionalJsonDocument)
    if cJsonDocuments != {}:
        # ids = conditionEvaluation(cJsonDocuments, data)
        # doc_ids = list(set([item for sublist in ids for item in sublist]))
        doc_ids = conditionEvaluation(cJsonDocuments, data)
        for i in doc_ids:
            for doc in data:
                if doc['_id'] in i:
                    updated_docs.append(doc)
    else:
        updated_docs = data

    jsonDocuments = json.loads(toBeUpdatedJsonDocument)
    if jsonDocuments != {}:
        for key, value in jsonDocuments.items():
            if updateConstraints(key):
                updated_docs = updateData(key, updated_docs, value)
    else:
        print("No update provided.")
    
    file_path = os.path.join(db_name, f'{collection_name}.json')

    # Write the updated documents to the file
    with open(file_path, 'w') as f:
        json.dump(data, f)
    
            
    return doc_ids
    

def updateData(key, data, jsonDocument):
    if key == "$set":
        return update_set(data, jsonDocument)
    elif key == "$unset":
        return update_unset(data, jsonDocument)
    elif key == "$inc":
        return update_inc(data, jsonDocument)
    elif key == "$dec":
        return update_dec(data, jsonDocument)
    elif key == "$mul":
        return update_mul(data, jsonDocument)
    elif key == "$div":
        return update_div(data, jsonDocument)
    elif key == "$rename":
        return update_rename(data, jsonDocument)
    else:
        return data
    
def updateConstraints(key):
    if key == "$set":
        return True
    elif key == "$unset":
        return True
    elif key == "$inc":
        return True
    elif key == "$dec":
        return True
    elif key == "$mul":
        return True
    elif key == "$div":
        return True
    elif key == "$rename":
        return True
    else:
        return False

#Set
def update_set(data, jsonDocument):
    doc_ids = []
    for key, value in jsonDocument.items():
        for doc in data:
            doc[key] = value

    return data

#Unset
def update_unset(data, jsonDocument):
    # doc_ids = []
    # if isinstance(jsonDocument, str):
    #     jsonDocuments = json.loads(jsonDocument)
    # elif isinstance(jsonDocument, dict):
    #     jsonDocuments = [jsonDocument]
    # elif isinstance(jsonDocument, (list, dict)):
    #     jsonDocuments = jsonDocument    
    # else:
    #     raise TypeError("unsupported types(s) for input")

    # for index, doc in enumerate(jsonDocuments):
    #     for key, value in doc.items():
    #         try:
    #             if json.loads(json.dumps(value)):
    #                 for key, value in value.items():
    #                     for doc in data:
    #                         doc.pop(key, None)
    #         except: 
    #             for doc in data:
    #                 doc.pop(key, None)

    # return data
    doc_ids = []
    for key, value in jsonDocument.items():
        for doc in data:
            doc.pop(key, None)

    return data

#Increment
def update_inc(data, jsonDocument):
    # doc_ids = []
    # if isinstance(jsonDocument, str):
    #     jsonDocuments = json.loads(jsonDocument)
    # elif isinstance(jsonDocument, dict):
    #     jsonDocuments = [jsonDocument]
    # elif isinstance(jsonDocument, (list, dict)):
    #     jsonDocuments = jsonDocument    
    # else:
    #     raise TypeError("unsupported types(s) for input")

    # for index, doc in enumerate(jsonDocuments):
    #     for key, value in doc.items():
    #         try:
    #             if json.loads(json.dumps(value)):
    #                 for key, value in value.items():
    #                     for doc in data:
    #                         newValue = doc[key] + value
    #                         doc[key] = newValue
    #         except: 
    #             for doc in data:
    #                 newValue = doc[key] + value
    #                 doc[key] = newValue

    # return data
    doc_ids = []
    for key, value in jsonDocument.items():
        for doc in data:
            newValue = doc[key] + value
            doc[key] = newValue

    return data

#Decrement
def update_dec(data, jsonDocument):
    # doc_ids = []
    # if isinstance(jsonDocument, str):
    #     jsonDocuments = json.loads(jsonDocument)
    # elif isinstance(jsonDocument, dict):
    #     jsonDocuments = [jsonDocument]
    # elif isinstance(jsonDocument, (list, dict)):
    #     jsonDocuments = jsonDocument    
    # else:
    #     raise TypeError("unsupported types(s) for input")

    # for index, doc in enumerate(jsonDocuments):
    #     for key, value in doc.items():
    #         try:
    #             if json.loads(json.dumps(value)):
    #                 for key, value in value.items():
    #                     for doc in data:
    #                         newValue = doc[key] - value
    #                         doc[key] = newValue
    #         except: 
    #             for doc in data:
    #                 newValue = doc[key] - value
    #                 doc[key] = newValue

    # return data
    doc_ids = []
    for key, value in jsonDocument.items():
        for doc in data:
            newValue = doc[key] - value
            doc[key] = newValue

    return data


#Multiply
def update_mul(data, jsonDocument):
    # doc_ids = []
    # if isinstance(jsonDocument, str):
    #     jsonDocuments = json.loads(jsonDocument)
    # elif isinstance(jsonDocument, dict):
    #     jsonDocuments = [jsonDocument]
    # elif isinstance(jsonDocument, (list, dict)):
    #     jsonDocuments = jsonDocument    
    # else:
    #     raise TypeError("unsupported types(s) for input")

    # for index, doc in enumerate(jsonDocuments):
    #     for key, value in doc.items():
    #         try:
    #             if json.loads(json.dumps(value)):
    #                 for key, value in value.items():
    #                     for doc in data:
    #                         newValue = doc[key] * value
    #                         doc[key] = newValue
    #         except: 
    #             for doc in data:
    #                 newValue = doc[key] * value
    #                 doc[key] = newValue

    # return data
    doc_ids = []
    for key, value in jsonDocument.items():
        for doc in data:
            newValue = doc[key] * value
            doc[key] = newValue

    return data

#Divide
def update_div(data, jsonDocument):
    # doc_ids = []
    # if isinstance(jsonDocument, str):
    #     jsonDocuments = json.loads(jsonDocument)
    # elif isinstance(jsonDocument, dict):
    #     jsonDocuments = [jsonDocument]
    # elif isinstance(jsonDocument, (list, dict)):
    #     jsonDocuments = jsonDocument    
    # else:
    #     raise TypeError("unsupported types(s) for input")

    # for index, doc in enumerate(jsonDocuments):
    #     for key, value in doc.items():
    #         try:
    #             if json.loads(json.dumps(value)):
    #                 for key, value in value.items():
    #                     for doc in data:
    #                         if value != 0:
    #                             newValue = doc[key] / value
    #                             doc[key] = newValue
    #         except: 
    #             for doc in data:
    #                 if value != 0:
    #                     newValue = doc[key] / value
    #                     doc[key] = newValue

    # return data
    doc_ids = []
    for key, value in jsonDocument.items():
        for doc in data:
            if value != 0:
                newValue = doc[key] / value
                doc[key] = newValue

    return data

#Rename
def update_rename(data, jsonDocument):
    # doc_ids = []
    # if isinstance(jsonDocument, str):
    #     jsonDocuments = json.loads(jsonDocument)
    # elif isinstance(jsonDocument, dict):
    #     jsonDocuments = [jsonDocument]
    # elif isinstance(jsonDocument, (list, dict)):
    #     jsonDocuments = jsonDocument    
    # else:
    #     raise TypeError("unsupported types(s) for input")

    # for index, doc in enumerate(jsonDocuments):
    #     for key, value in doc.items():
    #         try:
    #             if json.loads(json.dumps(value)):
    #                 for key, value in value.items():
    #                     for doc in data:
    #                         doc[value] = doc.pop(key)
    #         except: 
    #             for doc in data:
    #                 doc[value] = doc.pop(key)

    # return data
    doc_ids = []
    for key, value in jsonDocument.items():
        for doc in data:
            doc[value] = doc.pop(key)

    return data


def delete(jsonDocument, db_name, collection_name):
    doc_ids = []
    data = getData(db_name, collection_name)
    if isinstance(jsonDocument, str):
            jsonDocuments = json.loads(jsonDocument)
    elif isinstance(jsonDocument, dict):
            jsonDocuments = jsonDocument   
    else:
            raise TypeError("unsupported types(s) for input")
    if jsonDocuments != {}:
        ids = conditionEvaluation(jsonDocuments, data)
        doc_ids = list(set([item for sublist in ids for item in sublist]))
        
    else:
        print("No condition provided.")

    #flattened_doc_ids = list(set([item for sublist in doc_ids for item in sublist]))
    for doc in data:
        for i in doc_ids:
            if doc['_id'] == i:
                data.remove(doc) 

    file_path = os.path.join(db_name, f'{collection_name}.json')

    # Write the updated documents to the file
    with open(file_path, 'w') as f:
        json.dump(data, f)

    return doc_ids


def project(jsonDocument, data):
    documents = []
    jsonDocuments = json.loads(jsonDocument)
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
        


#docids = insert('[{"name": "ryan", "age": 24}]', "myDB", "myCollection")

#docids2 = insert('[{"name": "yash", "age": 24}]', "myDB", "myCollection")
#print(docids + docids2)


# read = readAll('{"$project": {"name": 0, "age": 1}}',"myDB", "myCollection")
# print(read)

# read = readAll('{}',"myDB", "myCollection")
# print(read)


# read = readSpecific('{"$project": {"name": 0, "age": 1}}','{"age": {"$gt": 29}}', "myDB", "myCollection")
# print(read)

# read = readSpecific('{}','{"$and": [{"name": "sam"},{"age":{"$gte": 25}}]}', "myDB", "myCollection")
# print(read)

# read = readSpecific('{}','{"$or": [{"name": "sam"},{"age":{"$gt": 25}}]}', "myDB", "myCollection")
# print(read)

# read = readSpecific('{}','{"name": "sam"}', "myDB", "myCollection")
# print(read)

#project = project('{"$project": {"name": 1, "age": 1}}', "myDB", "myCollection")
#print(project)

# matchAnd = match('{"$and": [{"name": "ryan"}, {"$age":{"$gt": 29}}]}', "myDB", "myCollection")
# print(matchAnd)

# matchOr = match('{"$or": [{"name": "ryan"}, {"$age":{"$gt": 29}}]}', "myDB", "myCollection")
# print(matchOr)

# update = update('{"name": "sam"}','{"$rename": {"agee": "age"}}', "myDB", "myCollection")
# print(update)

delete = delete({"$or": [{"name": "sam"},{"age":{"$eq": 24}}]}, "myDB", "myCollection")
print(delete)