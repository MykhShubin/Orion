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


def create_db():
    cursor.execute('DROP TABLE IF EXISTS students')

    sql = '''CREATE TABLE students(  
      ID SERIAL PRIMARY KEY ,
      FULL_NAME VARCHAR NOT NULL,
      ST_GROUP VARCHAR NOT NULL,
      ST_YEAR INT,
      SPECIALTY INT,
      GPA INT,
      PUB_WORK BOOLEAN NOT NULL
    )'''

    cursor.execute(sql)
    print("Table created successfully...")


def put_data(id:int, name:str, group:str, year:int, speciality:int,gpa:float,pub_work:bool):
    cursor.execute('''INSERT INTO students (ID,FULL_NAME,ST_GROUP,ST_YEAR,SPECIALITY,GPA,PUB_WORK) 
    VALUES (%s, %s,%s, %s, %s, %s, %s)''',(AsIs(id),name,group,AsIs(year),AsIs(speciality),AsIs(gpa),AsIs(pub_work)))

    #print('Table filled successfully...')


def delete_data():
    id_var = 2
    cursor.execute('DELETE FROM students WHERE ID = ' + str(id_var))
    print('Entry with id = ' + str(id_var) + ' deleted...')


def show_data():
    cursor.execute('SELECT * FROM students')
    mlist = cursor.fetchall()
    print('Students list:')
    for entry in mlist:
        print("Id:" + str(entry[0]) + "; Full name:" + str(entry[1]) + "; Group:" + str(entry[2]) + "; Study year:"
              + str(entry[3]) + "; Specialty:" + str(entry[4]) + "; GPA:" + str(entry[5]) + "; Public work:" + str(
            entry[6]))


cursor = conn.cursor()
create_db()
put_data(1,'Andriy Dyniak','DA-02',3,122,80,True)
show_data()
delete_data()
show_data()
conn.commit()
conn.close()
