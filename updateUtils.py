#Functions needed for update operations
#Called in DB_engine_latest

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
    doc_ids = []
    for key, value in jsonDocument.items():
        for doc in data:
            doc.pop(key, None)

    return data

#Increment
def update_inc(data, jsonDocument):
    doc_ids = []
    for key, value in jsonDocument.items():
        for doc in data:
            newValue = doc[key] + value
            doc[key] = newValue

    return data

#Decrement
def update_dec(data, jsonDocument):
    doc_ids = []
    for key, value in jsonDocument.items():
        for doc in data:
            newValue = doc[key] - value
            doc[key] = newValue

    return data


#Multiply
def update_mul(data, jsonDocument):
    doc_ids = []
    for key, value in jsonDocument.items():
        for doc in data:
            newValue = doc[key] * value
            doc[key] = newValue

    return data

#Divide
def update_div(data, jsonDocument):
    doc_ids = []
    for key, value in jsonDocument.items():
        for doc in data:
            if value != 0:
                newValue = doc[key] / value
                doc[key] = newValue

    return data

#Rename
def update_rename(data, jsonDocument):
    doc_ids = []
    for key, value in jsonDocument.items():
        for doc in data:
            doc[value] = doc.pop(key)

    return data