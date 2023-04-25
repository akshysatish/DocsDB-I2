#All necessary functions called in DB_engine_latest

import json
import os
from bisect import bisect_left

def search_nested_dict(nested_dict, search_word):
    results = []
    if isinstance(nested_dict, list):
        for item in nested_dict:
            results.extend(search_nested_dict(item, search_word))
    elif isinstance(nested_dict, dict):
        for key, value in nested_dict.items():
            if isinstance(value, (dict, list)):
                if search_nested_dict(value, search_word):
                    results.append(nested_dict)
                    break
            elif isinstance(value, str) and search_word in value:
                results.append(nested_dict)
                break
    return results

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
        if doc[attributeKey]['value'] > attributeValue:
            doc_ids.append(doc['_id'])

    return doc_ids

#lt
def lt(data, attributeKey, attributeValue):
    doc_ids = []
    for doc in data:
        if doc[attributeKey]['value'] < attributeValue:
            doc_ids.append(doc['_id'])

    return doc_ids

#lte
def lte(data, attributeKey, attributeValue):
    doc_ids = []
    for doc in data:
        if doc[attributeKey]['value'] <= attributeValue:
            doc_ids.append(doc['_id'])

    return doc_ids

#gte
def gte(data, attributeKey, attributeValue):
    doc_ids = []
    for doc in data:
        if doc[attributeKey]['value'] >= attributeValue:
            doc_ids.append(doc['_id'])

    return doc_ids

#eq
def eq(data, attributeKey, attributeValue):
    doc_ids = []
    for doc in data:
        if doc[attributeKey]['value'] == attributeValue:
            doc_ids.append(doc['_id'])

    return doc_ids

#ne
def ne(data, attributeKey, attributeValue):
    doc_ids = []
    for doc in data:
        if doc[attributeKey]['value'] != attributeValue:
            doc_ids.append(doc['_id'])

    return doc_ids

#in
def mongo_in(data, attributeKey, attributeValue):
    doc_ids = []
    for doc in data:
        if doc[attributeKey]['value'] == attributeValue:
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

def cmpop(item, key, cmp):
    if cmp == "$gt":
        return item > key
    elif cmp == "$lt":
        return item < key
    elif cmp == "$lte":
        return item <= key
    elif cmp == "$gte":
        return item >= key
    elif cmp == "$ne":
        return item != key
    else:
        return False

def getfields(jsonDocument):
    fields = {}

    for key, value in jsonDocument.items():
        if key == "$or" or key == "$and":
            for item in value:
                for key, value in item.items():
                    if isinstance(value, dict):
                        for key1, value1 in value.items():
                            if conditionalConstraintCheck(key1):
                                fields[key] = value1
                    else:
                        fields[key] = value
        else:
            if isinstance(value, dict):
                for key1, value1 in value.items():
                    if conditionalConstraintCheck(key1):
                        fields[key] = value1
            else:        
                fields[key] = value
    return fields
        
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
                if doc[key]['value'] == value:
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
                    if doc[key]['value'] == value:
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
                    if doc[key]['value'] == value:
                        temp_ids.append(doc['_id'])
                doc_ids.append(temp_ids)
    
    flattened_doc_ids = list(set([item for sublist in doc_ids for item in sublist]))
    return list(set(flattened_doc_ids))

# B+ tree class and methods
class Node:
    def __init__(self, order):
        self.order = order
        self.values = []
        self.keys = []
        self.nextKey = None
        self.preKey = None
        self.parent = None
        self.check_leaf = False
        
    def insert_at_leaf(self, leaf, key, value):
        if (self.keys):
            temp1 = self.keys
            for i in range(len(temp1)):
                if (key == temp1[i]):
                    self.values[i].append(value)
                    break
                elif (key < temp1[i]):
                    self.keys = self.keys[:i] + [key] + self.keys[i:]
                    self.values = self.values[:i] + [[value]] + self.values[i:]
                    break
                elif (i + 1 == len(temp1)):
                    self.keys.append(key)
                    self.values.append([value])
                    break
        else:
            self.keys = [key]
            self.values = [[value]]

class BplusTree:
    def __init__(self, order):
        self.root = Node(order)
        self.root.check_leaf = True
        self.key_type = None

    # Insert operation
    def insert(self, key, value):
        old_node = self.search(key)
        old_node.insert_at_leaf(old_node, key, value)
        '''
        Splitting by mediam when full, creating two new child nodes:
        right_node inherited right parts of full leaf node
        left_node(old_node) inherited left parts of full leaf node
        old_node = fully inserted leaf node
        '''
        if (len(old_node.keys) == old_node.order):
            right_node = Node(old_node.order)
            right_node.check_leaf = True
            right_node.parent = old_node.parent
            mid = old_node.order//2
            # create a new right_node
            right_node.keys = old_node.keys[mid:]
            right_node.values = old_node.values[mid:]
            temp_right = old_node.nextKey
            right_node.nextKey = temp_right
            if temp_right:
                temp_right.preKey = right_node
            # update old_node as new left node
            old_node.values = old_node.values[:mid]
            old_node.keys = old_node.keys[:mid] 
            # Link sibling
            old_node.nextKey = right_node
            right_node.preKey = old_node
            self.insert_in_parent(old_node, right_node.keys[0], right_node)

    # Search toward leaf node
    def search(self, key):
        '''
        Search all the way down to leaf node, might not have the node with key
        but the node with the key fit in that range
        '''
        current_node = self.root
        while(current_node.check_leaf == False):
            temp2 = current_node.keys
            for i in range(len(temp2)):
                if (key == temp2[i]):
                    current_node = current_node.values[i + 1]
                    break
                elif (key < temp2[i]):
                    current_node = current_node.values[i]
                    break
                elif (i + 1 == len(current_node.keys)):
                    current_node = current_node.values[i+1]
                    break
        return current_node

    # Find the node
    def find(self, key, value):
        l = self.search(key)
        print('key')
        print(l.keys)
        for i, item in enumerate(l.keys):
            if item == key:
                if value in l.values[i]:
                    return True
                else:
                    return False
        return False

    # Inserting at the parent
    def insert_in_parent(self, n, key, ndash):
        if (self.root == n):
            rootNode = Node(n.order)
            rootNode.keys = [key]
            rootNode.values = [n, ndash]
            self.root = rootNode
            n.parent = rootNode
            ndash.parent = rootNode
            return

        parentNode = n.parent
        temp3 = parentNode.values
        for i in range(len(temp3)):
            if (temp3[i] == n):
                parentNode.keys = parentNode.keys[:i] + \
                    [key] + parentNode.keys[i:]
                parentNode.values = parentNode.values[:i +
                                                  1] + [ndash] + parentNode.values[i + 1:]
                if (len(parentNode.values) > parentNode.order):
                    parent_right_node = Node(parentNode.order)
                    parent_right_node.parent = parentNode.parent
                    mid = parentNode.order//2 -1 
                    parent_right_node.values = parentNode.values[mid + 1:]
                    parent_right_node.keys = parentNode.keys[mid + 1:]
                    # Link next
                    temp_parent_right = parentNode.nextKey
                    parent_right_node.nextKey = temp_parent_right
                    if temp_parent_right:
                        temp_parent_right.preKey = parent_right_node
                    key_ = parentNode.keys[mid]
                    if (mid == 0):
                        parentNode.keys = parentNode.keys[:mid + 1]
                    else:
                        parentNode.keys = parentNode.keys[:mid]
                    parentNode.values = parentNode.values[:mid + 1]
                    # Link sibilings
                    parentNode.nextKey = parent_right_node
                    parent_right_node.preKey = parentNode
                    # Link Parents
                    for j in parentNode.values:
                        j.parent = parentNode
                    for j in parent_right_node.values:
                        j.parent = parent_right_node
                    self.insert_in_parent(parentNode, key_, parent_right_node)

    def delete(self, key, value, jsonDocuments=None):
        leaf_node = self.search(key)
        temp = 0
        cmp = ''
        if is_nested_dict(jsonDocuments) and jsonDocuments != None:
            for k, v in jsonDocuments.items():
                for k1, v1 in v.items():
                    cmp = k1
        for i, item in enumerate(leaf_node.keys):
            if item == key or cmpop(item, key, cmp):
                temp = 1
                for j in range(len(value)):
                    if value[j] in leaf_node.values[i]:
                        # len(leaf_node.values[i]) for nested list structure
                        # If duplicate just remove
                        if len(leaf_node.values[i]) > 1:
                            print(leaf_node.values[i])
                            leaf_node.values[i].pop(leaf_node.values[i].index(value[j]))
                        elif leaf_node == self.root:
                            leaf_node.values.pop(i)
                            leaf_node.keys.pop(i)
                        # In case no duplicate, delete first then adjust
                        else:
                            leaf_node.values[i].pop(leaf_node.values[i].index(value[j]))
                            del leaf_node.values[i]
                            leaf_node.keys.pop(leaf_node.keys.index(key))
                            self.deleteEntry(leaf_node, key, value[j])
                    else:
                        print("Value not in Key")
                        return
        if temp == 0:
            print("Key not in Tree")
            return

    # Delete an entry
    def deleteEntry(self, node_, key, value):
        # If node is internal node, values={node}, keys={key_str}, value=child_node, key=child_key
        if not node_.check_leaf:
            for i, item in enumerate(node_.values):
                if item == value:
                    node_.values.pop(i)
                    break
            for i, item in enumerate(node_.keys):
                if item == key:
                    node_.keys.pop(i)
                    break

        if self.root == node_:
            if len(node_.values) == 1:
                self.root = node_.values[0]
                node_.values[0].parent = None
                del node_
            return
        
        elif (len(node_.values) < (node_.order//2 +1) and node_.check_leaf == False) or (len(node_.keys) < (node_.order//2) and node_.check_leaf == True):
            '''
            Adjust when 
            1. In internal node,  len(node_.values):number of child < (node_.order//2 +1)
            2. In leaf node, len(node_.keys):number of key < (node_.order//2)
            '''
            is_predecessor = 0
            parentNode = node_.parent
            PrevNode = -1
            NextNode = -1
            PrevK = -1
            NextK = -1
            # parentNode.values:[children_nodes], parentNode.keys:[key_str]
            for i, item in enumerate(parentNode.values):
                if item == node_:
                    if i > 0:
                        PrevNode = parentNode.values[i - 1]
                        PrevK = parentNode.keys[i - 1]

                    if i < len(parentNode.values) - 1:
                        NextNode = parentNode.values[i + 1]
                        NextK = parentNode.keys[i]
                        
            if (PrevNode == -1) and (NextNode == -1):
                if node_.nextKey:
                    ndash = node_.nextKey
                    key_ = node_.nextKey.keys[0]
                else:
                    ndash = node_.preKey
                    key_ = node_.preKey.keys[0]            
            elif PrevNode == -1:
                ndash = NextNode
                key_ = NextK
            elif NextNode == -1:
                is_predecessor = 1
                ndash = PrevNode
                key_ = PrevK
            else:
                if len(node_.keys) + len(NextNode.keys) < node_.order:
                    ndash = NextNode
                    key_ = NextK
                else:
                    is_predecessor = 1
                    ndash = PrevNode
                    key_ = PrevK
            if len(node_.keys) + len(ndash.keys) < node_.order:
                # Borrow from right
                if is_predecessor == 0:
                    temp_right = ndash.nextKey
                    temp_left = node_.preKey
                    node_, ndash = ndash, node_
                else:
                    temp_right = node_.nextKey
                    temp_left = ndash.preKey
                ndash.values += node_.values
                node_
                if not node_.check_leaf:
                    ndash.keys.append(key_)
                else:
                    ndash.nextKey = node_.nextKey
                    if temp_left:
                        ndash.preKey = temp_left
                    if temp_right:
                        temp_right.preKey = ndash
                ndash.keys += node_.keys

                if not ndash.check_leaf:
                    for j in ndash.values:
                        j.parent = ndash

                self.deleteEntry(node_.parent, key_, node_)
                del node_
            else:
                if is_predecessor == 1:
                    if not node_.check_leaf:
                        ndashpm = ndash.values.pop(-1)
                        ndashkm_1 = ndash.keys.pop(-1)
                        node_.values = [ndashpm] + node_.values
                        node_.keys = [key_] + node_.keys
                        parentNode = node_.parent
                        for i, item in enumerate(parentNode.keys):
                            if item == key_:
                                parentNode.keys[i] = ndashkm_1
                                break
                    else:
                        ndashpm = ndash.values.pop(-1)
                        ndashkm = ndash.keys.pop(-1)
                        node_.values = [ndashpm] + node_.values
                        node_.keys = [ndashkm] + node_.keys
                        parentNode = node_.parent
                        for i, item in enumerate(parentNode.keys):
                            if item == key_:
                                parentNode.keys[i] = ndashkm
                                break
                else:
                    if not node_.check_leaf:
                        ndashp0 = ndash.values.pop(0)
                        ndashk0 = ndash.keys.pop(0)
                        node_.values = node_.values + [ndashp0]
                        node_.keys = node_.keys + [key_]
                        parentNode = node_.parent
                        for i, item in enumerate(parentNode.keys):
                            if item == key_:
                                parentNode.keys[i] = ndashk0
                                break
                    else:
                        ndashp0 = ndash.values.pop(0)
                        ndashk0 = ndash.keys.pop(0)
                        node_.values = node_.values + [ndashp0]
                        node_.keys = node_.keys + [ndashk0]
                        parentNode = node_.parent
                        for i, item in enumerate(parentNode.keys):
                            if item == key_:
                                parentNode.keys[i] = ndash.keys[0]
                                break

                if not ndash.check_leaf:
                    for j in ndash.values:
                        j.parent = ndash
                if not node_.check_leaf:
                    for j in node_.values:
                        j.parent = node_
                if not parentNode.check_leaf:
                    for j in parentNode.values:
                        j.parent = parentNode


    # Use BFS to check B+tree structure
    def show(self):
        level = 0
        current_level_nodes = [self.root]
        while current_level_nodes:
            next_level_nodes = []
            print("Level", level, ": ")
            for idx, node in enumerate(current_level_nodes):
                print(f'{node.keys, node.values}', end=" ")
                if not node.check_leaf:
                    next_level_nodes += node.values
            if node.check_leaf:
                print()
                break
            print()
            current_level_nodes = next_level_nodes
            level += 1
        return None
    
    # Use BFS to check B+tree structure
    def retrieve_all(self):
        level = 0
        alldocs = []
        current_level_nodes = [self.root]
        while current_level_nodes:
            next_level_nodes = []
            for idx, node in enumerate(current_level_nodes):
                if not node.check_leaf:
                    next_level_nodes += node.values
                else:
                    alldocs.append(node.values)
            if node.check_leaf:
                break
            current_level_nodes = next_level_nodes
            level += 1
        finaldocs = []
        for i in alldocs:
            for d in i:
                finaldocs.append(d[0])
        return finaldocs

    def retrieve_new(self, key, cmp):
            leaf_node = self.search(key)
            leaf_node_keys = leaf_node.keys
            value_list = []
            if cmp == '$gt':
                # Handle target node
                if key in leaf_node_keys:
                    key_location = leaf_node_keys.index(key)
                    start_location = key_location+1
                else:
                    start_location = bisect_left(leaf_node_keys, key)
                value_list += leaf_node.values[start_location:]
                head = leaf_node.nextKey
                
                # Handle sibling
                while head:
                    value_list += head.values
                    head = head.nextKey
                # Flatten list
                return_list = [element for sublist in value_list for element in sublist]
                return return_list
            
            elif cmp == '$gte':
                # Handle target node
                if key in leaf_node_keys:
                    key_location = leaf_node_keys.index(key)
                    start_location = key_location
                else:
                    start_location = bisect_left(leaf_node_keys, key)
                value_list += leaf_node.values[start_location:]
                head = leaf_node.nextKey
                
                # Handle sibling
                while head:
                    value_list += head.values
                    head = head.nextKey
                # Flatten list
                return_list = [element for sublist in value_list for element in sublist]
                return return_list
            
            elif cmp == '$lt':
                # Handle target node
                if key in leaf_node_keys:
                    key_location = leaf_node_keys.index(key)
                    start_location = key_location
                else:
                    start_location = bisect_left(leaf_node_keys, key)
                value_list += leaf_node.values[:start_location]
                head = leaf_node.preKey
                
                # Handle sibling
                while head:
                    value_list += head.values
                    head = head.preKey
                # Flatten list
                return_list = [element for sublist in value_list for element in sublist]
                return return_list

            elif cmp == '$lte':
                # Handle target node
                if key in leaf_node_keys:
                    key_location = leaf_node_keys.index(key)
                    start_location = key_location+1
                else:
                    start_location = bisect_left(leaf_node_keys, key)
                value_list += leaf_node.values[:start_location]
                head = leaf_node.preKey
                
                # Handle sibling
                while head:
                    value_list += head.values
                    head = head.preKey
                # Flatten list
                return_list = [element for sublist in value_list for element in sublist]
                return return_list
            
            elif cmp == '$eq':
                '''
                leaf_node_values = leaf_node.values
                for i, item in enumerate(leaf_node_keys):
                    if item == key:
                        value_list += leaf_node_values[i]
                '''
                if key in leaf_node_keys:
                    key_location = leaf_node_keys.index(key)
                    value_list += leaf_node.values[key_location]
                return value_list
            elif cmp == '$ne':
                if key in leaf_node_keys:
                    key_location = leaf_node_keys.index(key)
                    del leaf_node.values[key_location]
                value_list += leaf_node.values
                
                # Handle sibling
                backward = forward = leaf_node
                while backward:
                    value_list += backward.values
                    backward = backward.preKey
                while forward:
                    value_list += forward.values
                    forward = forward.preKey    
                # Flatten list
                return_list = [element for sublist in value_list for element in sublist]
                return return_list
            elif cmp == '$in':
                for k in leaf_node_keys:
                    if k.find(key) != -1:
                        key_location = leaf_node_keys.index(k)
                        value_list += leaf_node.values[key_location]
                return value_list                
            else:
                raise('Not support in current function!')
    
    def traverse_and_aggregate(self, group_by_field, aggregate_field, operation):
        result = {}
        # Find head
        root = self.root
        while root:
            head = root
            if not root.check_leaf:
                root = root.values[0]
            else:
                root = None
        # Iterate head
        while head:
            keys = head.keys
            values = head.values
            for val in values:
                key_dict = val[0][group_by_field]
                if key_dict not in result:
                    result[key_dict] = []
                result[key_dict].append(val[0][aggregate_field])
            head = head.nextKey
        # Iterate dictionary
        return {key: operation(value) for key, value in result.items()}

    def retrieve(self, key):
        """Returns a value for a given key, and None if the key does not exist."""
        l = self.search(key)
        for i, item in enumerate(l.keys):
            if key == item:
                return l.values[i]

        return None