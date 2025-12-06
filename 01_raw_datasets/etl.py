import pandas as pd

# Simple ETL Project

def extract_and_transform():

    # Extract
    print("Reading CSV files")
    students_df = pd.read_csv("students.csv")
    subjects_df = pd.read_csv("subjects.csv")
    results_df = pd.read_csv("results.csv")

    # Check missing values
    print("Missing values in students:")
    print(students_df.isnull().sum())

    print("Missing values in subjects:")
    print(subjects_df.isnull().sum())

    print("Missing values in results:")
    print(results_df.isnull().sum())

    # Remove duplicates
    students_df.drop_duplicates(inplace=True)
    subjects_df.drop_duplicates(inplace=True)
    results_df.drop_duplicates(inplace=True)

    # Valid marks only
    results_df = results_df[results_df["marks"].between(0, 100)]

    # Type conversions
    students_df["student_id"] = students_df["student_id"].astype(int)
    students_df["year"] = students_df["year"].astype(int)

    subjects_df["subject_id"] = subjects_df["subject_id"].astype(int)

    results_df["result_id"] = results_df["result_id"].astype(int)
    results_df["student_id"] = results_df["student_id"].astype(int)
    results_df["subject_id"] = results_df["subject_id"].astype(int)
    results_df["marks"] = results_df["marks"].astype(int)

    # Transform – merge tables
    student_results_df = results_df.merge(students_df, on="student_id", how="left")
    student_results_df = student_results_df.merge(subjects_df, on="subject_id", how="left")

    # Percentage
    student_results_df["percentage"] = student_results_df["marks"]

    # Grade calculation
    grades = []
    for m in student_results_df["marks"]:
        if m >= 90:
            grades.append("A+")
        elif m >= 80:
            grades.append("A")
        elif m >= 70:
            grades.append("B")
        elif m >= 60:
            grades.append("C")
        else:
            grades.append("D")

    student_results_df["grade"] = grades

    print("First 10 rows of final data:")
    print(student_results_df.head(10))

    return students_df, subjects_df, results_df


# calling the function
extract_and_transform()