import pickle
import json
import os
import uuid

def insertTypeCheck(jsonDocument):
      if isinstance(jsonDocument, str):
            return json.loads(jsonDocument)
      elif isinstance(jsonDocument, dict):
            return [jsonDocument]
      elif isinstance(jsonDocument, (list, dict)):
            return jsonDocument    
      else:
            raise TypeError("unsupported types(s) for input")

def condTypeCheck(jsonDocument):
      if isinstance(jsonDocument, str):
            return json.loads(jsonDocument)
      elif isinstance(jsonDocument, dict):
            return jsonDocument  
      else:
            raise TypeError("unsupported types(s) for input")

def getData(file_path):
      # Check if the collection exists
      if not os.path.exists(file_path):
            return []

      # Load all documents from the collection file
      with open(file_path, 'r') as f:
            data = json.load(f)

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