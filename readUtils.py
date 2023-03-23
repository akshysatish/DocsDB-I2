import json
import utils

def project(jsonDocument, data):
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