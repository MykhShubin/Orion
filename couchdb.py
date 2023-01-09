import couchdb # Імпортуємо бібліотеку для роботи з CouchDB

# Підключаємося до CouchDB
couch = couchdb.Server("http://localhost:5984/%22)

# Створюємо базу даних "Університет"
db = couch.create("university")

# Функція для запису даних про студента
def write_student_data(name, session_grade, average_grade):
    # Створюємо новий документ у базі даних
    doc = {
        "name": name,
        "session_grade": session_grade,
        "average_grade": average_grade
    }
    db.save(doc) # Зберігаємо документ у базі даних

# Функція для видалення даних про студента
def delete_student_data(doc_id):
    doc = db
    doc = db[doc_id]
    db.delete(doc)  # Видаляємо документ з бази даних
    # Виводимо інформацію про студента
    print("Name:", doc["name"])
    print("Session grade:", doc["session_grade"])
    print("Average grade:", doc["average_grade"])

# Функція для читання даних про студентів зі середньою оцінкою більшою за задане значення
def read_students_with_average_grade_greater_than(average_grade):
    # Виконуємо запит до бази даних
    results = db.view("students/by_average_grade", startkey=average_grade, inclusive_end=False)
    for result in results:
        doc = result.value # Отримуємо документ з результату
        # Виводимо інформацію про студента
        print("Name:", doc["name"])
        print("Session grade:", doc["session_grade"])
        print("Average grade:", doc["average_grade"])

def exit_database():
    couch.close()
