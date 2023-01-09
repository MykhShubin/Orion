#import couchdb_kursova
import hbase_kursova
#import postgreSQL
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
    #postgreSQL.create_db()
    for i in array:
        #postgreSQL.put_data(i.id,i.name,i.group,i.year,i.speciality,i.srednii_bal,i.pub_work)
        hbase_kursova.create_table(str(i.id),"grades")
        for main_subject,grage in i.subjects.items():
            hbase_kursova.put_data(str(i.id),main_subject,"grades","main_grade",str(grage))
        for session_subject,grage in i.subjects_sesiion.items():
            hbase_kursova.put_data(str(i.id),session_subject,"grades","session_grade",str(grage))


def put_academic_performance(arr_studiens_hbase):
    for i in arr_studiens_hbase:
        number = 0
        for value in i.subjects.values():
            number += value
        srednii_bal = number/len(i.subjects)
    #hbase_kursova.put_data(i.name,"srednii bal","grades","academic_performance",srednii_bal)
    #pushsql(srednii_bal) dobavlyem srednii bal v sql

def put_number_of_sesion_subject(arr_studiens_hbase):
    arr=[]
    for i in arr_studiens_hbase:
        number_of_subjects = len(i.session_subjects)
        arr.append(number_of_subjects)
    num = max(arr)
    return num
    #putsql put(num)

def give_scholarship(stu,num_of_session,group):
    type_of_scholarship = 0
    if(num_of_session != len(stu.session_subjects)):
        return "session is failed"

    if(max(stu.subjects) == 5 and min(stu.subjects) == 5):
        type_of_scholarship = 2
       
    if(max(stu.subjects) == 5 and min(stu.subjects)==4):
        type_of_scholarship = 1
        
    values = stu.subjects.values()
    counter = Counter(values)
    if(counter[3] == 1 and find_sql(stu.id).public_work == True):
        type_of_scholarship = 1 

    sql_set_scholarship(type_of_scholarship)

    
    
    
    
    




array = [student("Oleksandr",1,"DA-02",122,3,0,True,{"tik":5,"sbd":3},{"sbd":4}),
        student("Misha",2,"DA-02",122,3,0,True,{"tik":5,"sbd":2},{"sbd":4}),
        student("Andrii",3,"DA-02",122,3,0,True,{"tik":3,"sbd":4},{"sbd":5}),
        student("Andrii",4,"DA-12",122,2,0,True,{"os":3,"algoritms":4},{"algoritms":5})]
    
delete_all_tables_hbase()
creating_tables_and_adding_data(array)
# postgreSQL.show_data()
# postgreSQL.exit()
arr_studiens_hbase =[]
read_data_hbase(arr_studiens_hbase)
for i in arr_studiens_hbase:
    print("id is ",i.id," dict 1 is ",i.subjects, " dict 2 is ", i.session_subjects)
print(hbase_kursova.conn.tables())



delete_all_tables_hbase()

