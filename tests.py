import utils

def join(localData, foreignData, localField, foreignField):
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

def lookup(db_name, collection_name, db_name2, collection_name2, localField, foreignField):
    coll1file = collection_name + '.json'
    coll1_path = "C:\\Users\\aksha\\Downloads\\DocsDB-I2\\DocsDB-I2\\" + coll1file
    coll2file = collection_name2 + '.json'
    coll2_path = "C:\\Users\\aksha\\Downloads\\DocsDB-I2\\DocsDB-I2\\" + coll2file
    localData = utils.getData(coll1_path)
    foreignData = utils.getData(coll2_path)
    results = join(localData, foreignData, localField, foreignField)
    # Filtering/Querying conditions
    # result_filtered = [row for row in results if row.get('product') in ('Widget', 'Gadget')]
    # return result_filtered
    return results

results = lookup("myDB", "customers", "myDB", "orders", "id", "customer_id")
print(results)