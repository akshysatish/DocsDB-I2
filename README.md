# DocsDB-I2
## DB setup
>**Connect to database** `client = DB.Connection()`

>**Create database** `client.create_db('dbname')`

>**Create collection** `client.dbname.create_collection('collname')`

## CRUD operations
>**Insert** `client.dbname.collname.insert(JSON document(s))`

>**Delete** - to delete documents based on condition  
`client.dbname.collname.indexdelete(condition JSON document)`  
Use 'and'/'or' to make multiple conditions and combine  
Ex: {"$or": [{"name": "sam"},{"age":{"$eq": 24}}]}, {"$and": [{"name": "sam"},{"age":{"$eq": 24}}]}

>**Update** - to update documents based on condition and update to be made  
`client.dbname.collname.update(condition JSON document, update to be made)`  
Ex: `client.db1.abc.update({"name": "ryan"},{"$rename": {"agee": "age"}, "$set": {"age":25}})`

>**readSpecific** - read/query all or some documents in a collection  
`client.dbname.collname.readSpecific(projection JSON document, condition JSON document)`  
Projection document to choose which fields to return  
Condition document to specify matching conditions  
Projection document example: {attr1 : 1, attr3: 1}  
Condition document example: {"salary":{"$gt": 1000}}  
Example: `client.db1.abc.readSpecific('{"$project": {"name": 0, "age": 1}}','{"age": {"$gt": 29}}')`
Example: `client.db2.col2.indexedreadSpecific({},{})` to read all documents with all attributes in collection

## Indexes
>**Create Secondary Indexes** - `client.db2.col2.createSecondaryIndex(<attribute>, <branching-factor: usually 200>)`

>**indexedreadSpecific** - to make the database build and use B+ tree indexes  
Example: `client.db1.abc.indexedreadSpecific('{"$project": {"name": 0, "age": 1}}','{"age": {"$gt": 29}}')`  
Builds indexes on attributes in conditional document