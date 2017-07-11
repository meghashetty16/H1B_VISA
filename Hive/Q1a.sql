use h1bproject;

drop table h1b1a;

create table h1b1a (year string,total int )
row format delimited
fields terminated by '\t'
stored as textfile;

INSERT OVERWRITE table h1b1a select year,count(case_status) as total from h1b where job_title like '%DATA ENGINEER%' group by year order by year;


select a.year,a.total,(((b.total-a.total)*100)/a.total) as percentage from h1b1a a,h1b1a b where b.year=a.year+1;
