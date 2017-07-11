use h1bproject;

drop  table h1b2b;

create table h1b2b (year string,worksite string,count int )
row format delimited
fields terminated by '\t'
stored as textfile; 


--INSERT OVERWRITE DIRECTORY '/user/hive/warehouse/h1bproject.db/2bpartialout' ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' select year,split(worksite,'[,]')[1] as state, count(split(worksite,'[,]')[1]) as job_cnt from h1b where case_status LIKE 'CERTIFIED' group by split(worksite,'[,]')[1],year order by job_cnt DESC;

INSERT OVERWRITE TABLE h1b2b select year,split(worksite,'[,]')[1] as state, count(split(worksite,'[,]')[1]) as job_cnt from h1b where case_status LIKE 'CERTIFIED' group by split(worksite,'[,]')[1],year order by job_cnt DESC;

--LOAD DATA INPATH '/user/hive/warehouse/h1bproject.db/2bpartialout' OVERWRITE INTO TABLE h1b2b;

--select year ,max(struct(count,worksite)).col1 as name1,max(struct(count,worksite)).col2 as count1 from h1b2b group by year;

select * from(select year,worksite,count,RANK() OVER( partition by year order by count desc) as rank from h1b2b)t where rank<6;
