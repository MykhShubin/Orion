import psycopg2
from psycopg2.extensions import AsIs

# connection establishment
conn = psycopg2.connect(
    database='postgres',
    user='postgres',
    password='postgres',
    host='localhost',
    port='5432')
conn.autocommit = True

def exit():
    conn.commit()
    conn.close()

def create_db():
    cursor.execute('DROP TABLE IF EXISTS students')

    sql = '''CREATE TABLE students(  
      ID INT PRIMARY KEY ,
      FULL_NAME VARCHAR NOT NULL,
      ST_GROUP VARCHAR NOT NULL,
      ST_YEAR INT,
      SPECIALITY INT,
      PUB_WORK BOOLEAN NOT NULL
    )'''

    cursor.execute(sql)
    print("Table created successfully...")


def put_data(id:int, name:str, group:str, year:int, speciality:int,pub_work:bool):
    cursor.execute('''INSERT INTO students (ID,FULL_NAME,ST_GROUP,ST_YEAR,SPECIALITY,PUB_WORK) 
    VALUES (%s, %s,%s, %s, %s, %s)''',(AsIs(id),name,group,AsIs(year),AsIs(speciality),AsIs(pub_work)))

    #print('Table filled successfully...')


def delete_data(id:int):
    cursor.execute('DELETE FROM students WHERE ID = %s',(id,))
    print('Entry with id = ' + str(id) + ' deleted...')


def show_data():
    #cursor.execute('SELECT * FROM students')
    mlist = cursor.fetchall()
    print('Students list:')
    for entry in mlist:
        print("Id:" + str(entry[0]) + "; Full name:" + str(entry[1]) + "; Group:" + str(entry[2]) + "; Study year:"
              + str(entry[3]) + "; Specialty:" + str(entry[4]) + "; Public work:" + str(
            entry[5]) + "; Number of exams:" + str(entry[6]) + "; Living in dorm:" + str(entry[7]) + "; Scolarship:" + str(entry[8]))

def get_data(id:int):
    atr_list = []
    cursor.execute("SELECT * from students WHERE ID = %s",(id,))
    mlist = cursor.fetchall()
    for i in range(0,8):
        atr_list.append(mlist[0][i])
    return atr_list

def get_number_of_rows():
    cursor.execute("SELECT count(*) AS exact_count FROM students")
    mlist = cursor.fetchall()
    return mlist[0][0]

def alter_table():
    cursor.execute('''ALTER TABLE students
    ADD COLUMN EX_NUM INT,
    ADD COLUMN LIV BOOLEAN,
    ADD COLUMN SCOLARSHIP INT''')

def fill_ex_num(id:int,ex_num:int):
    cursor.execute('UPDATE students SET EX_NUM = %s WHERE ID = %s',(ex_num,id,))

def fill_liv(id:int,liv:bool):
    cursor.execute('UPDATE students SET LIV = %s WHERE ID = %s',(liv,id,))

def fill_scol(id:int,scol:int):
    cursor.execute('UPDATE students SET SCOLARSHIP = %s WHERE ID = %s', (scol, id,))

cursor = conn.cursor()
#create_db()
#put_data(1,'Andriy Dyniak','DA-02',3,122,True)
#put_data(2,'Mykhailo Shubin','DA-02',3,122,True)
#put_data(3,'Alexander Kovalenko','DA-02',3,122,True)
#get_number_of_rows()
#alter_table()
#show_data()
#delete_data(1)
#show_data()
#exit()

