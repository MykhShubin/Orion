#import couchdb_kursova
import hbase_kursova
import postgreSQL


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
    def __init__(self,name,subjects = {},sesion_subjects={})-> None:
        self.name = name
        self.subjects = subjects
        self.session_subjects = sesion_subjects
    
    def add_subjects(self,name,main_grade,session_grade):
        self.subjects[name] = main_grade
        if session_grade != -1:
            self.session_subjects[name] = session_grade


def read_data_hbase(arr_studiens_hbase):
    arr_hbase = hbase_kursova.conn.tables()
    for i in arr_hbase:
        stu = student_in_hbase(i)
        array_of_subjectes = []
        hbase_kursova.read_data(i,array_of_subjectes)
        for k in array_of_subjectes:
            stu.add_subjects(k.name,k.main_grade,k.sesion_grade)
        arr_studiens_hbase.append(stu)

def print_data_hbase(arr_studiens_hbase):
    for i in arr_studiens_hbase:
        print("name is ",i.name," dict 1 is ",i.subjects, " dict 2 is ", i.session_subjects)

def delete_all_tables_hbase():
    for i in hbase_kursova.conn.tables():
        hbase_kursova.delete_table(i)

def creating_tables_and_adding_data(array):
    postgreSQL.create_db()
    for i in array:
        postgreSQL.put_data(i.id,i.name,i.group,i.year,i.speciality,i.srednii_bal,i.pub_work)
        hbase_kursova.create_table(i.name,"grades")
        for main_subject,grage in i.subjects.items():
            hbase_kursova.put_data(i.name,main_subject,"grades","main_grade",str(grage))
        for session_subject,grage in i.subjects_sesiion.items():
            hbase_kursova.put_data(i.name,session_subject,"grades","session_grade",str(grage))


array = [student("Oleksandr",1,"DA-02",122,3,0,True,{"tik":25,"sbd":30},{"sbd":30}),
        student("Misha",2,"DA-02",122,3,0,True,{"tik":38,"sbd":23},{"sbd":80}),
        student("Andrii",3,"DA-02",122,3,0,True,{"tik":49,"sbd":22},{"sbd":67})]
    

creating_tables_and_adding_data(array)
#postgreSQL.show_data()
#postgreSQL.exit()
arr_studiens_hbase =[]
read_data_hbase(arr_studiens_hbase)
print_data_hbase(arr_studiens_hbase)


print(hbase_kursova.conn.tables())

delete_all_tables_hbase()


