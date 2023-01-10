#import couchdb_kursova
import hbase_kursova
import postgreSQL
from collections import Counter


class student:
    def __init__(self,name:str,id:int,group:str,speciality:int,year:int,srednii_bal:float,pub_work:bool,subjects:dict,subjects_sesiion:dict):
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
    def __init__(self,id):
        self.id = id
        self.subjects = {}
        self.session_subjects = {}
    
    def add_subjects(self,subject,main_grade,session_grade):
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
            stu.add_subjects(k.name,k.main_grade,k.sesion_grade)
        arr_studiens_hbase.append(stu)

def delete_all_tables_hbase():
    for i in hbase_kursova.conn.tables():
        hbase_kursova.delete_table(i)

def creating_tables_and_adding_data(array):
    postgreSQL.create_db()
    postgreSQL.alter_table()
    for i in array:
        postgreSQL.put_data(i.id, i.name, i.group, i.year, i.speciality, i.pub_work)
        hbase_kursova.create_table(str(i.id),"grades")
        for main_subject,grage in i.subjects.items():
            hbase_kursova.put_data(str(i.id),main_subject,"grades","main_grade",str(grage))
        for session_subject,grage in i.subjects_sesiion.items():
            hbase_kursova.put_data(str(i.id),session_subject,"grades","session_grade",str(grage))


def get_academic_performance(arr_studiens_hbase):
    srednii_bal = {}
    for i in arr_studiens_hbase:
        number = 0
        for value in i.subjects.values():
            number += value
        number = number/len(i.subjects)
        srednii_bal[i.id] = number
    return srednii_bal


def number_of_sesion_subject(arr_studiens_hbase):
    arr = []
    for i in arr_studiens_hbase:
        number_of_subjects = len(i.session_subjects)
        postgreSQL.fill_ex_num(i.id,number_of_subjects)
        arr.append(number_of_subjects)
    num = max(arr)
    return num

def give_scholarship(arr_studiens_hbase,num_of_session):
    for stu in arr_studiens_hbase:
        type_of_scholarship = 0
        if(num_of_session != len(stu.session_subjects)):
            return "session is failed"

        if(max(stu.subjects.values()) == 5 and min(stu.subjects.values()) == 5):
            type_of_scholarship = 2
        
        if(max(stu.subjects.values()) == 5 and min(stu.subjects.values()) == 4):
            type_of_scholarship = 1
            
        values = stu.subjects.values()
        counter = Counter(values)
        if (counter[3] == 1 and postgreSQL.get_data(stu.id)[5] == True):
            type_of_scholarship = 1
        postgreSQL.fill_scol(type_of_scholarship,stu.id)

def show_step_list():
    print("Enter name of group")
    group = 'DA-02'
    # print("Choose group(Enter DA-02 or DA-01 or DA-12)")
    #group = input()
    print('Group '+ group + ' scholarships:')
    postgreSQL.show_step(group)

def find_students_for_deduction(arr_studiens_hbase):
    arr_of_id_for_deduction = []
    for stu in arr_studiens_hbase:
        values = stu.subjects.values()
        counter = Counter(values)
        if counter[2] > 2:
            arr_of_id_for_deduction.append(stu.id)
    return arr_of_id_for_deduction

def find_middle_grade_of_group(group,grades):
    srednii_bal_for_group = 0
    number_of_students = 0
    for i in grades.keys():
        if postgreSQL.get_data(i)[2] == group:
            srednii_bal_for_group += grades[i]
            number_of_students += 1
    srednii_bal_for_group /= number_of_students
    return srednii_bal_for_group
        

array = [student("Oleksandr",1,"DA-02",122,3,0,True,{"tik":5,"sbd":5,"web":5,"grapics":4,"english":4},{"sbd":4}),
        student("Misha",2,"DA-02",122,3,0,True,{"tik":5,"sbd":4,"web":4,"grapics":5,"english":4},{"sbd":4}),
        student("Andrii",3,"DA-02",122,3,0,True,{"tik":3,"sbd":4,"web":3,"grapics":4,"english":5},{"sbd":5}),
        student("Matvei",4,"DA-12",122,2,0,True,{"os":3,"algoritms":4},{"algoritms":5}),
        student("Danil",5,"DA-01",122,3,0,True,{"tik":3,"sbd":4,"web":5,"grapics":3,"english":4},{"sbd":3}),
        student("Vadim",6,"DA-01",122,3,0,True,{"tik":5,"sbd":4,"web":5,"grapics":5,"english":4},{"sbd":2}),
        student("Dasha",7,"DA-01",122,3,0,True,{"tik":4,"sbd":3,"web":3,"grapics":5,"english":4},{"sbd":4}),
        student("Nastya",8,"DA-01",122,3,0,True,{"tik":5,"sbd":2,"web":2,"grapics":2,"english":4},{"sbd":5})]
    
delete_all_tables_hbase()
creating_tables_and_adding_data(array)
# postgreSQL.show_data()
# postgreSQL.exit()
arr_studiens_hbase = []
read_data_hbase(arr_studiens_hbase)
for i in arr_studiens_hbase:
    print("id is ",i.id," dict 1 is ",i.subjects, " dict 2 is ", i.session_subjects)


print(get_academic_performance(arr_studiens_hbase))
#print(number_of_sesion_subject(arr_studiens_hbase))
number_of_exams = number_of_sesion_subject(arr_studiens_hbase)
give_scholarship(arr_studiens_hbase,number_of_exams)
show_step_list()
srednii_bal = get_academic_performance(arr_studiens_hbase)
print(srednii_bal)
for key,value in srednii_bal.items():
    postgreSQL.fill_scol(value,key)
#средний бал должен быть флоат, в массиве в мейне он флоат а уже в sql инт
#postgreSQL.show_data()

print("students for otchislenie ",find_students_for_deduction(arr_studiens_hbase))
print(find_middle_grade_of_group("DA-01",srednii_bal))
print(find_middle_grade_of_group("DA-02",srednii_bal))




delete_all_tables_hbase()

