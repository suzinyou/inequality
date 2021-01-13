options symbolgen nosqlremerge;

/* CHANGE OUTPUT FOLDER NAME */
libname OUT '/userdata07/room285/data_out/output-mean_median';
libname STORE '/userdata07/room285/data_out/data_store';


%macro count_and_mean_group(dname, unit, group_var, second_group_var, earner,adult_age,savenamei, year_lb, year_ub);
%let where_expr=where input(STD_YYYY, 4.) between &year_lb and &year_ub;
%if &unit=adult %then %do;
	%let where_expr=&where_expr. and age >= &adult_age;
	%if &earner=1 %then %do;
		%let where_expr=&where_expr. and inc_tot > 0;
		%end;
	%end;
%else %if &unit=earner %then %do;
	%let where_expr=&where_expr. and inc_tot > 0;
	%end;

%if &group_var=is_working %then %do;
	%let group_select=case when inc_wage+inc_bus>0 then " 취업" else "미취업" end as group length=16;
	%let groupby=STD_YYYY, group;
	%end;
%else %if %sysevalf(%superq(group_var)=,boolean)=0 %then %do;
	%let group_select=put(cats(&group_var), $16.) as group length=16;
	%let groupby=STD_YYYY, &group_var;
	%end;
%else %do;
	%let group_select='' as group length=16;
	%let groupby=STD_YYYY;
	%end;

%if &second_group_var=age_group %then %do;
	%let second_group_select=case when age>=0 and age<=19 then "0-19" when age>=20 and age<=29 then "20-29" when age>=30 and age<=39 then "30-39" when age>=40 and age<=49 then "40-49" when age>=50 and age<=59 then "50-59" when age>=60 and age<=69 then "60-69" else "70+" end as group2 length=16;
	%let groupby=&groupby., group2;
	%end;
%else %do;
	%let second_group_select='' as group2 length=16;
	%end;

proc sql;
create table tmp as
select STD_YYYY
	, "&group_var" as group_var length=32
	, &group_select
	, &second_group_select
	, count(*) as num_adult20_inc_tot_earner
	, mean(inc_wage) as mean_inc_wage
	, mean(inc_bus) as mean_inc_bus
	, mean(inc_fin) as mean_inc_fin
	, mean(inc_othr) as mean_inc_othr
	, mean(inc_pnsn) as mean_inc_pnsn
	, mean(inc_tot) as mean_inc_tot
	, median(inc_tot) as median_inc_tot
from &dname
&where_expr
group by &groupby;

create table tmp_pop as
select STD_YYYY
	, "&group_var" as group_var length=32
	, &group_select
	, &second_group_select
	, count(*) as num_adult20
from &dname
&where_expr
group by &groupby;

create table &savenamei as
select a.*
	, b.num_adult20
	, a.num_adult20_inc_tot_earner / b.num_adult20 as frac_earner
from tmp as a
left join tmp_pop as b
on a.STD_YYYY=b.STD_YYYY and a.group=b.group and a.group2=b.group2;
%mend count_and_mean_group;

%macro count_and_mean(region, unit, group_vars, second_group_var=, earner=0, adult_age=20, year_lb=2006, year_ub=2018);
%let dname=store.&region;
/* Set savename */
%let savename=&region;
%if &unit=adult and &adult_age=. %then %do;
	%abort cancel;
	%end;
%else %if &unit=adult %then %do;
	%let savename=&savename.&adult_age;
	%if &earner=1 %then %do;
		%let savename=&savename._earner;
		%end;
	%end;

%if %sysevalf(%superq(group_vars)=,boolean) %then %do;
	%let savenamei=work.&savename._1;
	%count_and_mean_group(&dname,&unit,,&second_group_var,&earner,&adult_age,&savenamei, &year_lb, &year_ub);
	%end;
%else %do;
	%do i=1 %to %sysfunc(countw(&group_vars));
		%let group_var=%scan(&group_vars,&i);
		%let savenamei=work.&savename._&i;
		%count_and_mean_group(&dname,&unit,&group_var, &second_group_var,&earner,&adult_age,&savenamei, &year_lb, &year_ub);
		%end;
	%end;

/* CREATE OUTPUT DATASET */
%if %sysfunc(exist(out.&savename)) %then %do;
	data out.&savename;
	set 
		out.&savename
		work.&savename._:;
	run;
	%end;
%else
%do;
	data out.&savename;
	set work.&savename._:;
	run;
	%end;

/* DELETE TEMP DATASETS */
proc datasets lib=work nolist kill;
quit;
run;

proc export data=out.&savename
	outfile="/userdata07/room285/data_out/output-mean_median/mean_median-by_source.xlsx"
	DBMS=xlsx
	replace;
	sheet="&savename";
run;
%mend count_and_mean;

/* 성인 소득자만 */
/*%count_and_mean(seoul, unit=adult, group_vars=, earner=1, adult_age=20, year_lb=2006,year_ub=2018);*/
/*%count_and_mean(seoul, unit=adult, group_vars=is_working, earner=1, adult_age=20, year_lb=2018, year_ub=2018);*/
/*%count_and_mean(seoul, unit=adult, group_vars=sex_type, earner=1, adult_age=20, second_group_var=age_group, year_lb=2018, year_ub=2018);*/

/* 성인 전체 */
/*%count_and_mean(seoul, unit=adult, group_vars=, earner=0, adult_age=20, year_lb=2006,year_ub=2018);*/
/*%count_and_mean(seoul, unit=adult, group_vars=is_working, earner=0, adult_age=20, year_lb=2018, year_ub=2018);*/
/*%count_and_mean(seoul, unit=adult, group_vars=sex_type, earner=0, adult_age=20, second_group_var=age_group, year_lb=2018, year_ub=2018);*/

