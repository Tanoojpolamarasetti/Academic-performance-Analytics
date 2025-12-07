import pandas as pd
import oracledb

def extract_and_transform():

    # Extract
    print("Reading CSV files")
    students_df = pd.read_csv("students.csv")
    subjects_df = pd.read_csv("subjects.csv")
    results_df = pd.read_csv("results.csv")

    #check missing values
    print("Missing values in students:")
    print(students_df.isnull().sum())

    print("Missing values in subjects:")
    print(subjects_df.isnull().sum())

    print("Missing values in results:")
    print(results_df.isnull().sum())

    # Drop duplicates
    students_df.drop_duplicates(inplace=True)
    subjects_df.drop_duplicates(inplace=True)
    results_df.drop_duplicates(inplace=True)

    # Valid marks
    results_df = results_df[results_df["marks"].between(0, 100)]

    # Type casting
    students_df["student_id"] = students_df["student_id"].astype(int)
    students_df["year"] = students_df["year"].astype(int)

    subjects_df["subject_id"] = subjects_df["subject_id"].astype(int)

    results_df["result_id"] = results_df["result_id"].astype(int)
    results_df["student_id"] = results_df["student_id"].astype(int)
    results_df["subject_id"] = results_df["subject_id"].astype(int)
    results_df["marks"] = results_df["marks"].astype(int)

    # Transform
    df = results_df.merge(students_df, on="student_id", how="left")
    df = df.merge(subjects_df, on="subject_id", how="left")

    # Percentage
    df["percentage"] = df["marks"]

    # Grade
    def get_grade(m):
        if m >= 90: return "A+"
        if m >= 80: return "A"
        if m >= 70: return "B"
        if m >= 60: return "C"
        return "D"

    df["grade"] = df["marks"].apply(get_grade)

    print("First 10 rows of final data:")
    print(df.head(10))

    return df


# Run ETL
df = extract_and_transform()

# Oracle Connection

connection = oracledb.connect(
    user="SYSTEM",
    password="Tanooj29",
    dsn="localhost/XEPDB1"
)

cursor = connection.cursor()

# Create Table

create_table_sql = """
CREATE TABLE student_results (
    result_id NUMBER,
    student_id NUMBER,
    subject_id NUMBER,
    marks NUMBER,
    name VARCHAR2(50),
    department_x VARCHAR2(50),
    year NUMBER,
    subject_name VARCHAR2(50),
    department_y VARCHAR2(50),
    percentage NUMBER,
    grade VARCHAR2(5)
)
"""

try:
    cursor.execute(create_table_sql)
    print("Table created.")
except:
    print("Table already exists.")

# Prepare rows for insertion

rows = [tuple(x) for x in df.itertuples(index=False, name=None)]

# Correct INSERT SQL
insert_sql = """
INSERT INTO student_results (
    result_id, student_id, subject_id, marks,
    name, department_x, year, subject_name,
    department_y, percentage, grade
)
VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11)
"""

cursor.executemany(insert_sql, rows)
connection.commit()

print("Data inserted successfully into Oracle!")

cursor.close()
connection.close()