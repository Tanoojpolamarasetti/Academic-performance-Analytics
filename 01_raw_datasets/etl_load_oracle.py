import pandas as pd
import oracledb

# --------------------------
# Step 1: Extract CSV files
# --------------------------
students = pd.read_csv('students.csv')
subjects = pd.read_csv('subjects.csv')
results = pd.read_csv('results.csv')

# --------------------------
# Step 2: Connect to Oracle XE as SYSDBA
# --------------------------
username = 'SYS'
password = 'Tanooj29'
host = 'localhost'
port = 1521
service_name = 'XE'

dsn = f"{host}:{port}/{service_name}"

connection = oracledb.connect(
    user=username,
    password=password,
    dsn=dsn,
    mode=oracledb.SYSDBA
)
cursor = connection.cursor()

# --------------------------
# Step 3: Create tables if not exist
# --------------------------
def create_table(sql):
    cursor.execute(f"""
    BEGIN
        EXECUTE IMMEDIATE '{sql}';
    EXCEPTION
        WHEN OTHERS THEN
            IF SQLCODE != -955 THEN
                RAISE;
            END IF;
    END;
    """)

create_table("""
CREATE TABLE students (
    student_id NUMBER PRIMARY KEY,
    name VARCHAR2(50),
    department VARCHAR2(50),
    year NUMBER
)
""")

create_table("""
CREATE TABLE subjects (
    subject_id NUMBER PRIMARY KEY,
    subject_name VARCHAR2(50),
    department VARCHAR2(50)
)
""")

create_table("""
CREATE TABLE results (
    result_id NUMBER PRIMARY KEY,
    student_id NUMBER REFERENCES students(student_id),
    subject_id NUMBER REFERENCES subjects(subject_id),
    marks NUMBER
)
""")
# Clear old data
cursor.execute("DELETE FROM results")
cursor.execute("DELETE FROM subjects")
cursor.execute("DELETE FROM students")

# --------------------------
# Step 4: Insert data with explicit int() conversion
# --------------------------
for _, row in students.iterrows():
    cursor.execute("""
        INSERT INTO students (student_id, name, department, year)
        VALUES (:1, :2, :3, :4)
    """, (
        int(row['student_id']),  # convert to Python int
        str(row['name']),
        str(row['department']),
        int(row['year'])
    ))

for _, row in subjects.iterrows():
    cursor.execute("""
        INSERT INTO subjects (subject_id, subject_name, department)
        VALUES (:1, :2, :3)
    """, (
        int(row['subject_id']),
        str(row['subject_name']),
        str(row['department'])
    ))

for _, row in results.iterrows():
    cursor.execute("""
        INSERT INTO results (result_id, student_id, subject_id, marks)
        VALUES (:1, :2, :3, :4)
    """, (
        int(row['result_id']),
        int(row['student_id']),
        int(row['subject_id']),
        int(row['marks'])
    ))

# --------------------------
# Step 5: Commit & Close
# --------------------------
connection.commit()
cursor.close()
connection.close()

print("ETL process completed successfully!")