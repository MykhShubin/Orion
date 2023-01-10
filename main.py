import couchdb_kursova
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

class login:
    def __init__(self, log:str, password:str):
        self.log = log
        self.password = password

def creating_tables_and_adding_data(array):
    postgreSQL.create_db()
    postgreSQL.alter_table()

    for i in array:
        postgreSQL.put_data(i.id, i.name, i.group, i.year, i.speciality, i.pub_work)
        hbase_kursova.create_table(str(i.id), "grades")
        number = 0
        for key, value in i.subjects_sesiion.items():
            number = i.subjects_sesiion[key]
        couchdb_kursova.write_student_data(str(i.id), i.name, number, i.srednii_bal,0)
        for main_subject, grage in i.subjects.items():
            hbase_kursova.put_data(str(i.id), main_subject, "grades", "main_grade", str(grage))
        for session_subject, grage in i.subjects_sesiion.items():
            hbase_kursova.put_data(str(i.id), session_subject, "grades", "session_grade", str(grage))

def get_academic_performance(arr_studiens_hbase):
    srednii_bal = {}
    for i in arr_studiens_hbase:
        number = 0
        for value in i.subjects.values():
            number += value
        number = number / len(i.subjects)
        srednii_bal[i.id] = number
        docum = couchdb_kursova.copy_doc(str(i.id))
        couchdb_kursova.delete_student_data(str(i.id))
        couchdb_kursova.write_student_data(docum["doc_id"], docum["name"],
        docum["session_grade"], number, docum["kolichestvo_ekzameniv"])
    return srednii_bal


def number_of_sesion_subject(arr_studiens_hbase):
    arr = []
    for i in arr_studiens_hbase:
        number_of_subjects = len(i.session_subjects)
        postgreSQL.fill_ex_num(i.id, number_of_subjects)
        docum = couchdb_kursova.copy_doc(str(i.id))
        couchdb_kursova.delete_student_data(str(i.id))
        couchdb_kursova.write_student_data(docum["doc_id"], docum["name"],
        docum["session_grade"], docum["average_grade"], number_of_subjects)
        arr.append(number_of_subjects)
    num = max(arr)
    return num


def give_scholarship(arr_studiens_hbase):
    for stu in arr_studiens_hbase:
        type_of_scholarship = 0
        if (max(stu.subjects.values()) == 5 and min(stu.subjects.values()) == 5):
            type_of_scholarship = 2

        if (max(stu.subjects.values()) == 5 and min(stu.subjects.values()) == 4):
            type_of_scholarship = 1

        values = stu.subjects.values()
        counter = Counter(values)
        if (counter[3] == 1 and postgreSQL.get_data(stu.id)[5] == True):
            type_of_scholarship = 1
        postgreSQL.fill_scol(type_of_scholarship, stu.id)


def show_scol_list():
    print("Enter name of group")
    print("Choose group(Enter DA-02 or DA-01 or DA-12)")
    group = input()
    while(group != 'DA-01' and group != 'DA-02' and group != 'DA-12'):
        print('Incorrect input. Please, try again!')
        group = input()
    print('Group ' + group + ' scholarships:')
    postgreSQL.show_step(group)

def show_group_list():
    postgreSQL.show_group('DA-02')
    postgreSQL.show_group('DA-01')
    postgreSQL.show_group('DA-12')

def show_alph_by_group():
    print("List in alpabetical order by group")
    list = postgreSQL.order_alph()
    print("  DA-02:")
    for i in list:
        if(i[2] == 'DA-02'):
            print(i[1])
            print(hbase_kursova.read_data(str(i[0])).subjects)
    print("\n  DA-01:")
    for i in list:
        if (i[2] == 'DA-01'):
            print(i[1])
            print(hbase_kursova.read_data(str(i[0])).subjects)
    print("\n  DA-12:")
    for i in list:
        if (i[2] == 'DA-12'):
            print(i[1])
            print(hbase_kursova.read_data(str(i[0])).subjects)

def show_all_stu_names():
    for stu in arr_studiens_hbase:
        print(postgreSQL.get_data(stu.id)[1])


def find_students_for_deduction(arr_studiens_hbase):
    for stu in arr_studiens_hbase:
        values = stu.subjects.values()
        counter = Counter(values)
        if counter[2] > 2:
            print(postgreSQL.get_data(stu.id)[1])
            return postgreSQL.get_data(stu.id)[1]


def find_middle_grade_of_group(group, grades):
    srednii_bal_for_group = 0
    number_of_students = 0
    for i in grades.keys():
        if postgreSQL.get_data(i)[2] == group:
            srednii_bal_for_group += grades[i]
            number_of_students += 1
    srednii_bal_for_group /= number_of_students
    srednii_bal_for_group = format(srednii_bal_for_group, '.2f')
    return srednii_bal_for_group

def log_in():
    login_arr = [login("1@mail.com", "password1"),
                 login("2@mail.com", "password2"),
                 login("3@mail.com", "password3")]
    access = 0
    while access == 0 or access == 1:
        dat1 = str(input("Enter your login: "))
        dat2 = str(input("Enter your password: "))
        #dat1 = '1@mail.com';dat2 = 'password1'
        for i in login_arr:
            if dat1 == i.log and dat2 == i.password:
                print("Login successful!")
                access = 2
                break
            else:
                access = 1
                continue
        if access == 1:
            print("Try again!")


def menu():
    print("\n[1] Scolarships by group")
    print("[2] List of students by group and their grades")
    print("[3] List of students for deduction")
    print('[4] List of group with middle number')
    print("[5] List of all students by name")
    print("[0] Exit the program\n")


array = [student("Oleksandr", 1, "DA-02", 122, 3, 0, True, {"tik": 5, "sbd": 5, "web": 5, "grapics": 5, "english": 5},
                 {"sbd": 4}),
         student("Misha", 2, "DA-02", 122, 3, 0, True, {"tik": 5, "sbd": 4, "web": 4, "grapics": 5, "english": 4},
                 {"sbd": 4}),
         student("Andrii", 3, "DA-02", 122, 3, 0, True, {"tik": 3, "sbd": 4, "web": 4, "grapics": 4, "english": 5},
                 {"sbd": 5}),
         student("Danil", 4, "DA-01", 122, 3, 0, False, {"tik": 5, "sbd": 5, "web": 5, "grapics": 5, "english": 5},
                 {"sbd": 3}),
         student("Vadim", 5, "DA-01", 122, 3, 0, False, {"tik": 5, "sbd": 3, "web": 5, "grapics": 5, "english": 5},
                 {"sbd": 2}),
         student("Dasha", 6, "DA-01", 122, 3, 0, True, {"tik": 4, "sbd": 3, "web": 4, "grapics": 5, "english": 4},
                 {"sbd": 4}),
         student("Nastya", 7, "DA-01", 122, 3, 0, False, {"tik": 5, "sbd": 2, "web": 2, "grapics": 2, "english": 4},
                 {"sbd": 5}),
         student("Matvei", 8, "DA-12", 122, 2, 0, True, {"os": 3, "algoritms": 4}, {"algoritms": 5}),
         student("Oleksandr", 9, "DA-12", 122, 2, 0, True, {"os": 3, "algoritms": 4}, {"algoritms": 5}),
         student("Kiril", 10, "DA-12", 122, 2, 0, True, {"os": 3, "algoritms": 4}, {"algoritms": 5}),
         student("Anna", 11, "DA-12", 122, 2, 0, True, {"os": 3, "algoritms": 4}, {"algoritms": 5}),]

hbase_kursova.delete_all_tables()
creating_tables_and_adding_data(array)
arr_studiens_hbase = []
hbase_kursova.read_all_data(arr_studiens_hbase)
print(number_of_sesion_subject(arr_studiens_hbase))
#UI

log_in()
menu()
option = int(input("Enter your option: "))
#option = 4;
while option != 0:
    if option == 1:#[1] Scolarships by group
        give_scholarship(arr_studiens_hbase)
        show_scol_list()

    elif option == 2:#[2] List of students by group and their grades
        show_alph_by_group()

    elif option == 3:#[3] List of students for deduction
        print("Students for deduction:")
        find_students_for_deduction(arr_studiens_hbase)

    elif option == 4:#4] List of group with middle number
        srednii_bal = get_academic_performance(arr_studiens_hbase)
        print(srednii_bal)
        print('Middle grade for group DA-01 -', find_middle_grade_of_group("DA-01", srednii_bal))
        print('Middle grade for group DA-02 -', find_middle_grade_of_group("DA-02", srednii_bal))
        print('Middle grade for group DA-12 -', find_middle_grade_of_group("DA-12", srednii_bal))

    elif option == 5:#[5] List of all students by name
        show_all_stu_names()

    else:#[0] Exit the program
        print("Invalid option.")

    menu()
    #option = int(input("Enter your option: "))
    option = 0;

for i in range(1,12):
    couchdb_kursova.delete_student_data(str(i))
hbase_kursova.delete_all_tables()
postgreSQL.exit()
print("Thank you for using this program!")
