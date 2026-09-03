Student Analytics Project 

This project analyzes student academic performance using three CSV files and a simple ETL pipeline. It demonstrates data cleaning, transformation, and SQL-based analysis in an easy, beginner-friendly way.

Project Overview 

Input datasets:

students.csv – Student details

subjects.csv – Subjects

results.csv – Marks scored


The ETL script processes these files and loads the final data into an Oracle database.

ETL Process 

Extract: Read all three CSV files

Transform: Clean missing values, merge datasets, add grade column, standardize fields

Load: Insert the transformed data into Oracle tables


SQL Analysis 

Includes SQL queries for:

Top scorers

Failure rates

Subject-wise average marks


ER Diagram 

An ER diagram is provided to show the relationships between Students, Subjects, and Results.

## 📊 Power BI Dashboard

An interactive Power BI dashboard was created to analyze student academic performance.

### Dashboard Features

- Total Students
- Total Subjects
- Average Marks
- Highest Marks
- Pass Percentage
- Average Marks by Subject
- Average Marks by Department
- Average Marks by Year
- Student Performance Analysis
- Marks Distribution
- Detailed Student Performance Table

### Interactive Filters

- Department
- Year
- Subject

### Dashboard Preview

![Academic Performance Dashboard](https://github.com/Tanoojpolamarasetti/Academic-performance-Analytics/blob/main/Academic_performance_Analytics/academic-performance-dashboard.png?raw=true)

### Power BI File

The complete Power BI dashboard is available in:

`Academic_Performance_Analytics.pbix`
