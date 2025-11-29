import pandas as pd

def extract_and_transform():
    # Extract
    students = pd.read_csv('students.csv')
    subjects = pd.read_csv('subjects.csv')
    results = pd.read_csv('results.csv')
    
    # Clean
    print(students.isnull().sum())
    print(subjects.isnull().sum())
    print(results.isnull().sum())

    students.drop_duplicates(inplace=True)
    subjects.drop_duplicates(inplace=True)
    results.drop_duplicates(inplace=True)

    results = results[(results["marks"] >= 0) & (results["marks"] <= 100)]

    # Convert datatypes (very important!!!)
    students['student_id'] = students['student_id'].astype(int)
    students['year'] = students['year'].astype(int)

    subjects['subject_id'] = subjects['subject_id'].astype(int)

    results['result_id'] = results['result_id'].astype(int)
    results['student_id'] = results['student_id'].astype(int)
    results['subject_id'] = results['subject_id'].astype(int)
    results['marks'] = results['marks'].astype(int)

    # Transform
    student_results = results.merge(students, on="student_id", how="left")
    student_results = student_results.merge(subjects, on="subject_id", how="left")

    student_results["percentage"] = student_results["marks"]

    def get_grade(x):
        if x >= 90: return "A+"
        elif x >= 80: return "A"
        elif x >= 70: return "B"
        elif x >= 60: return "C"
        else: return "D"

    student_results["grade"] = student_results["marks"].apply(get_grade)

    print("Transformed dataset (first 10 rows):")
    print(student_results.head(10))

    return students, subjects, results
if __name__ == "__main__":
    students, subjects, results = extract_and_transform()
    print("ETL Extract + Transform completed.")
