import couchdb

couch = couchdb.Server('http://couchdb:couchdb@localhost:5984')
couch.delete('university')
db = couch.create('university')
#couch.delete('university')
db = couch['university']

#couch.delete('university')
def write_student_data(doc_id, name, session_grade, average_grade, kolichestvo_ekzameniv):
    # Створюємо новий документ у базі даних
    doc = {
        "doc_id": doc_id,
        "name": name,
        "session_grade": session_grade,
        "average_grade": average_grade,
        "kolichestvo_ekzameniv" :kolichestvo_ekzameniv
    }
    db[doc_id] = doc
    #db.save(doc)
    print("Data with doc_id {}: {}".format(doc_id, doc))

def read_student_data(doc_id):
    try:
        # Шукаємо документ з заданим doc_id
        doc = db.get(doc_id)
        print("Data with doc_id {}: {}".format(doc_id, doc))
    except couchdb.http.ResourceNotFound:
        print("Data with doc_id {} not found".format(doc_id))

def copy_doc(doc_id):
    doc = db.get(doc_id)
    return doc

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

def view_all_data():
    # Get all documents in the database
    for doc_id in db:
        doc = db[doc_id]
        print("Data with doc_id {}: {}".format(doc_id, doc))
#view_all_data()

#write some data to the database
#write_student_data("1", "John Smith", 75, 85,0)
# write_student_data("2", "Jane Doe", 80, 90,0)
# docum = copy_doc("1")
# print(docum["doc_id"])
# print(docum)
# print(docum)
# read data from the database
#read_student_data("1")

#  delete data from the database
#delete_student_data("1")
