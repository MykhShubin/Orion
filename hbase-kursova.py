import happybase
conn = happybase.Connection()
conn.open()

class subject:
    def __init__(self,name:str,main_grade:int,sesion_grade:int):
        self.name = name
        self.main_grade = main_grade
        self.sesion_grade = sesion_grade

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

    
def read_data(table_name,array):
    table = conn.table(table_name)
    for key,data in table.scan():
        if(len(data)==2):
            subject1 = subject(key.decode(),int(data[b'grades:main_grade'].decode()),int(data[b'grades:session_grade'].decode()))
        else:
            subject1 = subject(key.decode(),int(data[b'grades:main_grade'].decode()),-1)
        array.append(subject1)
        #print(key,data)
        

def delete_table(table_name):
    conn.disable_table(table_name)
    conn.delete_table(table_name)

def delete_data(table_name,key):
    table = conn.table(table_name)
    table.delete(key)

#delete_table("student_name")
# create_table("student_name","grades")
# print(conn.tables())
# array_of_subjectes = []
# put_data("student_name","bd","grades","main_grade","70")
# put_data("student_name","bd","grades","session_grade","86")
# read_data("student_name",array_of_subjectes)
# for i in array_of_subjectes:
#     print("name is ", i.name, " main grade is ",i.main_grade," session grade is ", i.sesion_grade)

# delete_table("student_name")
