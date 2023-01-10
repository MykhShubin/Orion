from main import *

def test_create_db():
    postgreSQL.cursor.execute("select * from information_schema.tables where table_name=%s", ('students',))
    i = bool(postgreSQL.cursor.rowcount)
    assert i == True

def test_put_data():
    arr = array
    arr.append(student("Temp", 12, "DA-12", 122, 2, 0, True, {"os": 3, "algoritms": 4}, {"algoritms": 5}))
    postgreSQL.put_data(array[11].id, array[11].name, array[11].group, array[11].year, array[11].speciality, array[11].pub_work)
    postgreSQL.cursor.execute("SELECT * from students WHERE ID = %s", (array[0].id,))
    mlist = postgreSQL.cursor.fetchall()
    postgreSQL.delete_data(array[11].id)
    assert mlist[0][0] == array[0].id

def test_length_of_table():
    i = postgreSQL.get_number_of_rows()
    postgreSQL.cursor.execute("SELECT count(*) AS exact_count FROM students")
    mlist = postgreSQL.cursor.fetchall()
    assert i == mlist[0][0]

def test_fill_liv():
    i = True
    arr = array
    arr.append(student("Temp", 12, "DA-12", 122, 2, 0, True, {"os": 3, "algoritms": 4}, {"algoritms": 5}))
    postgreSQL.put_data(array[11].id, array[11].name, array[11].group, array[11].year, array[11].speciality,array[11].pub_work)
    postgreSQL.fill_liv(array[11].id,i)
    postgreSQL.cursor.execute("SELECT * from students WHERE ID = %s", (array[11].id,))
    mlist = postgreSQL.cursor.fetchall()
    postgreSQL.delete_data(array[11].id)
    assert mlist[0][7] == i


def test_add_student_class():
    arr = array
    arr.append(student("Temp", 12, "DA-12", 122, 2, 0, True, {"os": 3, "algoritms": 4}, {"algoritms": 5}))
    assert arr[11].name == "Temp"

def test_students_for_deduction():
    i = find_students_for_deduction(arr_studiens_hbase)
    assert i == 'Nastya'

def test_login(capsys):
    log_in()
    captured = capsys.readouterr()
    assert captured.out == ('Login successful!\n')

def test_menu(capsys):
    menu()
    captured = capsys.readouterr()
    assert captured.out == ('\n'
    '[1] Scolarships by group\n'
    '[2] List of students by group and their grades\n'
    '[3] List of students for deduction\n'
    '[4] List of group with middle number\n'
    '[5] List of all students by name\n'
    '[0] Exit the program\n'
    '\n')




