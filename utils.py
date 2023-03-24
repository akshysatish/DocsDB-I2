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
        if is_nested_dict(value):#
            nested_doc_ids = nestedconditionEvaluation(value, data, key)
            for i in nested_doc_ids:
                doc_ids.append(i)
        elif key == "$and":
            doc_ids.append(andEvaluation(value, data))
        elif key == "$or":
            doc_ids.append(orEvaluation(value, data))
        else:
            for doc in data:
                if doc[key] == value:
                    doc_ids.append(doc['_id'])                
            

    return doc_ids

def nestedconditionEvaluation(conditionalJsonDocument, data, nkey):
    doc_ids = []
    for key, value in conditionalJsonDocument.items():
        if conditionalConstraintCheck(key):
            temp_ids = checkKey(key, data, nkey, value)
            for i in temp_ids:
                doc_ids.append(i)

        elif is_nested_dict(value):#
            nested_doc_ids = nestedconditionEvaluation(value, data, nkey)
            for i in nested_doc_ids:
                doc_ids.append(i)
        elif key == "$and":
            doc_ids.append(andEvaluation(value, data))
        elif key == "$or":
            doc_ids.append(orEvaluation(value, data))
        else:
            for doc in data:
                obj = json.loads(json.dumps(doc))
                if nkey in obj:
                    if obj[nkey][key] == value:
                        doc_ids.append(doc['_id'])
                else:
                    continue


    return doc_ids

def is_nested_dict(instance):
    """
    Check if an instance is a nested dictionary.
    """
    if not isinstance(instance, dict):
        return False
    for value in instance.values():
        if isinstance(value, dict):
            return is_nested_dict(value)
    return True   
    
 
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
            if key == "$or":
                temp_ids = []
                or_doc_ids = orEvaluation(value, data)
                for i in or_doc_ids:
                    temp_ids.append(i)
                doc_ids.append(temp_ids)
            
            elif is_nested_dict(value):#
                temp_ids = []
                nested_doc_ids = nestedconditionEvaluation(value, data, key)
                for i in nested_doc_ids:
                    temp_ids.append(i)
                doc_ids.append(temp_ids)
            
            else:
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
            if key == "$and":
                temp_ids = []
                or_doc_ids = orEvaluation(value, data)
                for i in or_doc_ids:
                    temp_ids.append(i)
                doc_ids.append(temp_ids)
            
            elif is_nested_dict(value):#
                temp_ids = []
                nested_doc_ids = nestedconditionEvaluation(value, data, key)
                for i in nested_doc_ids:
                    temp_ids.append(i)
                doc_ids.append(temp_ids)
            
            else:
                temp_ids = []
                for doc in data:
                    if doc[key] == value:
                        temp_ids.append(doc['_id'])
                doc_ids.append(temp_ids)
    
    flattened_doc_ids = list(set([item for sublist in doc_ids for item in sublist]))
    return list(set(flattened_doc_ids))