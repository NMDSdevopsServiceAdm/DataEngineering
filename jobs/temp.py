import hashlib


def hash_location_id(row_number, column_index):
    clean_string = datasetObj.cases[row_number, column_index][0].replace(" ", "")
    hashed_string = hashlib.sha256(clean_string.encode()).hexdigest()
    return hashed_string[:20]


def remove_spaces(input_string):
    return input_string.replace(" ", "")


locationid_index = datasetObj.varlist["locationid"].index
Identifier_index = datasetObj.varlist["Identifier"].index
rows = len(datasetObj.cases)

for i in range(rows):
    if datasetObj.cases[i, locationid_index][0][0] == "1":
        datasetObj.cases[i, Identifier_index] = hash_location_id(i, locationid_index)
