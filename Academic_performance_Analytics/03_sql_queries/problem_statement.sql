-- Top Scores
select student_id,name,marks,subject_name
from student_results
order by marks desc
fetch first 10 rows only;

-- subject-wise Average marks
select subject_name,avg(marks) as avg_marks
from student_results
group by subject_name
order by avg_marks desc;

--failure rates
select count(*) as total_failures
from student_results
where marks<40;