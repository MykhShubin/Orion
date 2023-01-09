#import couchdb_kursova
import hbase_kursova
#import postgreSQL
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

array = [student("Oleksandr",1,"DA-02",122,3,0,True,{"tik":25,"sbd":30},{"sbd":30}),
        student("Misha",2,"DA-02",122,3,0,True,{"tik":38,"sbd":23},{"sbd":80}),
        student("Andrii",3,"DA-02",122,3,0,True,{"tik":49,"sbd":22},{"sbd":67})]

#POSTGRES
postgreSQL.create_db()
for i in array:
    postgreSQL.put_data(i.id,i.name,i.group,i.year,i.speciality,i.srednii_bal,i.pub_work)
postgreSQL.show_data()
postgreSQL.exit()

#HBASE
for i in array:
    hbase_kursova.delete_table(i.name)

for i in array:
    hbase_kursova.create_table(i.name,"grades")
    for main_subject,grage in i.subjects.items():
        hbase_kursova.put_data(i.name,main_subject,"grades","main_grade",str(grage))
    for session_subject,grage in i.subjects_sesiion.items():
        hbase_kursova.put_data(i.name,session_subject,"grades","session_grade",str(grage))

for i in array:
    array_of_subjectes = []
    hbase_kursova.read_data(i.name,array_of_subjectes)
    for k in array_of_subjectes:
        print("name is ",i.name," subject is ", k.name, " main grade is ",k.main_grade," session grade is ", k.sesion_grade)

print(hbase_kursova.conn.tables())