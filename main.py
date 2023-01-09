# import couchdb_kursova
import hbase_kursova
import postgreSQL
from collections import Counter

class student:
    def __init__(self, name: str, id: int, group: str, speciality: int, year: int, srednii_bal: float, pub_work: bool,
                 subjects: dict, subjects_sesiion: dict):
        self.name = name
        self.id = id
        self.group = group
        self.speciality = speciality
        self.year = year
        self.srednii_bal = srednii_bal
        self.pub_work = pub_work
        self.subjects = subjects
        self.subjects_sesiion = subjects_sesiion


class student_in_hbase:
    def __init__(self, id):
        self.id = id
        self.subjects = {}
        self.session_subjects = {}

    def add_subjects(self, subject, main_grade, session_grade):
        self.subjects[subject] = main_grade
        if session_grade != -1:
            self.session_subjects[subject] = session_grade


def read_data_hbase(arr_studiens_hbase):
    arr_hbase = hbase_kursova.conn.tables()
    for i in arr_hbase:
        stu = student_in_hbase(i.decode())
        array_of_subjectes = []
        hbase_kursova.read_data(i, array_of_subjectes)
        for k in array_of_subjectes:
            stu.add_subjects(k.name, k.main_grade, k.sesion_grade)
        arr_studiens_hbase.append(stu)


def delete_all_tables_hbase():
    for i in hbase_kursova.conn.tables():
        hbase_kursova.delete_table(i)


def creating_tables_and_adding_data(array):
    postgreSQL.create_db()
    postgreSQL.alter_table()
    for i in array:
        postgreSQL.put_data(i.id, i.name, i.group, i.year, i.speciality, i.pub_work)
        hbase_kursova.create_table(str(i.id), "grades")
        for main_subject, grage in i.subjects.items():
            hbase_kursova.put_data(str(i.id), main_subject, "grades", "main_grade", str(grage))
        for session_subject, grage in i.subjects_sesiion.items():
            hbase_kursova.put_data(str(i.id), session_subject, "grades", "session_grade", str(grage))


def put_academic_performance(arr_studiens_hbase):
    for i in arr_studiens_hbase:
        number = 0
        for value in i.subjects.values():
            number += value
        srednii_bal = number / len(i.subjects)
    # hbase_kursova.put_data(i.name,"srednii bal","grades","academic_performance",srednii_bal)
    # pushsql(srednii_bal) dobavlyem srednii bal v sql


def number_of_sesion_subject(arr_studiens_hbase):
    arr = []
    for i in arr_studiens_hbase:
        number_of_subjects = len(i.session_subjects)
        arr.append(number_of_subjects)
    num = max(arr)
    return num


def give_scholarship(arr_studiens_hbase):
    for stu in arr_studiens_hbase:
        type_of_scholarship = 0
        if(max(stu.subjects) == 5 and min(stu.subjects) == 5):
            type_of_scholarship = 2

        if(max(stu.subjects) == 5 and min(stu.subjects) == 4):
            type_of_scholarship = 1

        values = stu.subjects.values()
        counter = Counter(values)
        if (counter[3] == 1 and postgreSQL.get_data(stu.id)[5] == True):
            type_of_scholarship = 1

        postgreSQL.fill_scol(type_of_scholarship,stu.id)

def show_step_list():
    print("Enter name of group")
    group = 'DA-02'
    #group = input()
    print('Group '+ group + ' scholarships:')
    postgreSQL.show_step(group)


array = [student("Oleksandr", 1, "DA-02", 122, 3, 0, True, {"tik": 25, "sbd": 30}, {"sbd": 30}),
         student("Misha", 2, "DA-02", 122, 3, 0, True, {"tik": 38, "sbd": 23}, {"sbd": 80}),
         student("Andrii", 3, "DA-02", 122, 3, 0, True, {"tik": 49, "sbd": 22}, {"sbd": 67})]
delete_all_tables_hbase()
creating_tables_and_adding_data(array)
#postgreSQL.show_data()
#postgreSQL.exit()
arr_studiens_hbase = []
read_data_hbase(arr_studiens_hbase)
give_scholarship(arr_studiens_hbase)
postgreSQL.show_data()
show_step_list()

for i in arr_studiens_hbase:
    print("id is ", i.id, " dict 1 is ", i.subjects, " dict 2 is ", i.session_subjects)
print(hbase_kursova.conn.tables())
delete_all_tables_hbase()


