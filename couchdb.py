import couchdb

couch = couchdb.Server('http://couchdb:couchdb@localhost:5984')
#couch.delete('university')
#db = couch.create('university')
#couch.delete('university')
db = couch['student']

#couch.delete('university')
def write_student_data(doc_id, name, session_grade, average_grade):
    # Створюємо новий документ у базі даних
    doc = {
        "doc_id": doc_id,
        "name": name,
        "session_grade": session_grade,
        "average_grade": average_grade
    }
    db[doc_id] = doc
    print("Data with doc_id {}: {}".format(doc_id, doc))
def read_student_data(doc_id):
    try:
        # Шукаємо документ з заданим doc_id
        doc = db.get(doc_id)
        print("Data with doc_id {}: {}".format(doc_id, doc))
    except couchdb.http.ResourceNotFound:
        print("Data with doc_id {} not found".format(doc_id))

def delete_student_data(doc_id):
    try:
        # Шукаємо документ з заданим doc_id
        doc = db.get(doc_id)
        if doc is None:
            print("Document with doc_id {} not found".format(doc_id))
        else:
            # Видаляємо документ з бази даних
            db.delete(doc)
            print("Data with doc_id {} is deleted".format(doc_id))
    except couchdb.http.ResourceNotFound:
        print("Data with doc_id {} not found".format(doc_id))



# write some data to the database
write_student_data("s1", "John Smith", 75, 85)
write_student_data("s2", "Jane Doe", 80, 90)

# read data from the database
read_student_data("s1")

# delete data from the database
delete_student_data("s1")

