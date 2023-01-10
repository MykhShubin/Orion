import happybase
conn = happybase.Connection()
conn.open()


class student:
    def __init__(self,id):
        self.id = int(id)
        self.subjects = {}
        self.session_subjects = {}
    
    def add_subjects(self,subject,main_grade,session_grade):
        self.subjects[subject] = main_grade
        if session_grade != -1:
            self.session_subjects[subject] = session_grade

def create_table(table_name,family1):
    conn.create_table(
        table_name,
        {
            family1:dict()
        }
    )

def put_data(table_name:str,key:str,family:str,columb:str,value:str):
    table = conn.table(table_name)
    data = family + ":"+ columb
    table.put(key, {data:value})

    
def read_data(table_name):
    table = conn.table(table_name)
    student1 = student(table_name)
    for key,data in table.scan():
        if(len(data)==2):
            student1.add_subjects(key.decode(),int(data[b'grades:main_grade'].decode()),int(data[b'grades:session_grade'].decode()))
        else:
            student1.add_subjects(key.decode(),int(data[b'grades:main_grade'].decode()),-1)
    return student1

def read_all_data(array):
    arr_hbase = conn.tables()
    for i in arr_hbase:
        student1 = read_data(i.decode())
        array.append(student1)
        

def delete_table(table_name):
    conn.disable_table(table_name)
    conn.delete_table(table_name)

def delete_data(table_name,key):
    table = conn.table(table_name)
    table.delete(key)

def delete_all_tables():
    for i in conn.tables():
        delete_table(i)

# delete_table("1")
# delete_table("2")
# create_table("1","grades")
# create_table("2","grades")
# #print(conn.tables())
# array_of_subjectes = []
# put_data("1","bd","grades","main_grade","70")
# put_data("1","tik","grades","main_grade","70")
# put_data("1","bd","grades","session_grade","86")
# put_data("2","bd","grades","main_grade","70")


# sstu = read_data("1")
# #print(sstu.id,sstu.subjects,sstu.session_subjects)
# # for i in array_of_subjectes:
# #     print("name is ", i.name, " main grade is ",i.main_grade," session grade is ", i.sesion_grade)
# array = []
# read_all_data(array)
# for i in array:
#     print(i.id,i.subjects,i.session_subjects)


# delete_table("1")
# delete_table("2")
