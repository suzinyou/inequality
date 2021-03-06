/*OPTIONS NONOTES NOSOURCE NODATE NOCENTER LABEL NONUMBER LS=200 PS=MAX;*/
options symbolgen nosqlremerge;

libname OUT '/userdata07/room285/data_out/output-household';
libname STORE '/userdata07/room285/data_out/data_store';

/* CODE */
%macro count_and_mean_group(dname, group_var, second_group, where_expr, savenamei);
%let groupby=STD_YYYY, &group_var;
%if %sysevalf(%superq(second_group)=,boolean)=0 %then %do;
	%let groupby=&groupby., &second_group;
	%end;

proc sql;
create table &savenamei as
select &groupby
	, count(*) as count
	, mean(inc_tot) as mean_inc_tot
from &dname
&where_expr
group by &groupby;
quit;
%mend count_and_mean_group;

%macro count_and_mean(region, unit, group_vars, second_group, hh_size_cond='', earner=0, year_lb=2006, year_ub=2018);
%let dname=store.&region._&unit;
%let savename=&region._&unit;
%let where_expr=where input(STD_YYYY, 4.) between &year_lb and &year_ub;

%if &hh_size_cond='1' %then %do;
	%let savename=&savename._single;
	%let where_expr=&where_expr. and hh_size=1;
	%end;
%else %if &hh_size_cond='2+' %then %do;
	%let savename=&savename._multi;
	%let where_expr=&where_expr. and hh_size>=2;
	%end;

%if &earner=1 %then %do;
	%let savename=&savename._earner;
	%let where_expr=&where_expr. and inc_tot > 0;
	/*TODO: maybe also parametrize vname.. */
	%end;

%do i=1 %to %sysfunc(countw(&group_vars));
	%let group_var=%scan(&group_vars,&i);
	%let savenamei=work.&savename._&i;
	/*dname, group_var, second_group, where_expr, savenamei*/
	%count_and_mean_group(&dname, &group_var, &second_group, &where_expr, &savenamei);
%end;

/* CREATE OUTPUT DATASET */
%if %sysfunc(exist(out.&savename)) %then %do;
	data out.&savename;
	set 
		out.&savename
		work.&savename._:;
	run;
	%end;
%else %do;
	data out.&savename;
	set work.&savename._:;
	run;
	%end;

/* DELETE TEMP DATASETS */
proc datasets lib=work nolist kill;
quit;
run;

proc export data=out.&savename
	/* CHANGE OUTFILE PATH */
	outfile="/userdata07/room285/data_out/output-household/household.xlsx"
	DBMS=xlsx
	replace;
	sheet="&savename";
run;
%mend count_and_mean;

/*%count_and_mean(kr, hh1, group_vars=hh_size_group,second_group=,year_lb=2018,year_ub=2018);*/
/*%count_and_mean(kr, eq1, group_vars=hh_size_group age_group,second_group=,year_lb=2018,year_ub=2018);*/

/*%count_and_mean(seoul, hh1, group_vars=hh_size_group,second_group=,year_lb=2018,year_ub=2018);*/
/*%count_and_mean(seoul, hh2, group_vars=hh_size_group,second_group=,year_lb=2018,year_ub=2018);*/
/*%count_and_mean(seoul, eq1, group_vars=hh_size_group age_group,second_group=,year_lb=2018,year_ub=2018);*/
/*%count_and_mean(seoul, eq2, group_vars=hh_size_group age_group,second_group=,year_lb=2018,year_ub=2018);*/

/*%count_and_mean(seoul, hh2, group_vars=age_group,second_group=,hh_size_cond='1',year_lb=2006,year_ub=2018);*/
/*%count_and_mean(seoul, hh2, group_vars=age_group,second_group=sex_type,hh_size_cond='1',earner=1,year_lb=2018,year_ub=2018);*/

/* 2018년 서울, 소득자 비율 & 소득자 평균소득, 유재산자 비율 & 유재산자 평균재산 */
%macro earner_stat_var(dname, vname, i);
%let groupby_vars=age_group, sex_type;
proc sql;
create table stat as
select &groupby_vars
	, "&vname" as var
	, count(*) as count
	, mean(&vname) as mean
from &dname
where &vname > 0
group by &groupby_vars;
quit;

proc sql;
create table pop as
select &groupby_vars
	, count(*) as count
from &dname
group by &groupby_vars;
quit;

proc sql;
create table tmp&i as
select a.*
	, b.count as num_indi
	, a.count / b.count as frac_earners
from stat as a
left join pop as b
on a.age_group=b.age_group and a.sex_type=b.sex_type;
quit;
%mend;

%macro earner_stat(dname, savename);
%let vnames=prop_txbs_tot inc_tot;
%do i=1 %to 2;
	%let vname = %scan(&vnames, &i);
	%earner_stat_var(&dname, &vname, &i);
%end;

data out.&savename;
set work.tmp:;

/* DELETE TEMP DATASETS */
proc datasets lib=work nolist kill;
quit;
run;

proc export data=out.&savename
	outfile="/userdata07/room285/data_out/output-household/household.xlsx"
	DBMS=xlsx
	replace;
	sheet="&savename";
run;
%mend;

/* 2인이상 가구 가구주 데려오기 */
/*proc sql;*/
/*create table store.seoul_multi_hh_head_18 as*/
/*select a.inc_tot*/
/*	, a.prop_txbs_tot*/
/*	, a.sex_type*/
/*	, case*/
/*			when a.age=. then ''*/
/*			when a.age < 20 then "0-19"*/
/*			when a.age >= 20 and a.age < 30 then "20-29"*/
/*			when a.age >= 30 and a.age < 40 then "30-39"*/
/*			when a.age >= 40 and a.age < 50 then "40-49"*/
/*			when a.age >= 50 and a.age < 60 then "50-59"*/
/*			when a.age >= 60 and a.age < 70 then "60-69"*/
/*			when a.age >= 70 then "70+"*/
/*			else '' end*/
/*			as age_group */
/*from store.seoul as a*/
/*inner join store.seoul_hh2 as b*/
/*on a.STD_YYYY=b.STD_YYYY and a.INDI_DSCM_NO=b.HHRR_HEAD_INDI_DSCM_NO*/
/*where a.STD_YYYY="2018" and b.hh_size >= 2;*/
/*quit;*/

/* 1인 가구 데려오기 */
/*proc sql;*/
/*create table store.seoul_single_hh_head_18 as*/
/*select **/
/*from store.seoul_hh2*/
/*where STD_YYYY="2018" and hh_size=1;*/
/*quit;*/

/*%earner_stat(store.seoul_multi_hh_head_18, earner_stat_multi_hh_head_2018);*/
/*%earner_stat(store.seoul_single_hh_head_18, earner_stat_single_hh_head_2018);*/

/* 평균 세대/가구원수...  */
%MACRO mean_hh_size_dataset(i, region, unit, year_lb, year_ub);
proc sql;
create table tmp_&i as
select STD_YYYY
	, "&unit" as unit
	, mean(hh_size) as mean
from store.&region._&unit
where INPUT(STD_YYYY, 4.) between &year_lb and &year_ub
group by STD_YYYY;
quit;
%MEND mean_hh_size_dataset;

%macro mean_hh_size;
%let savename=mean_hh_size;
%mean_hh_size_dataset(1, SEOUL, HH1, 2006, 2018);
%mean_hh_size_dataset(2, SEOUL, HH2, 2006, 2018);
data out.&savename;
set work.tmp_:;

/* DELETE TEMP DATASETS */
proc datasets lib=work nolist kill;
quit;
run;

proc export data=out.&savename
	outfile="/userdata07/room285/data_out/output-household/household.xlsx"
	DBMS=xlsx
	replace;
	sheet="&savename";
run;
%mend;
/*%mean_hh_size;*/

/* 구별 의료급여수급률 (개인) */
%MACRO GAIBJA_TYPE_7_OR_8;
%LET savename=gaibja_type_7_or_8_seoul_18;
proc sql;
create table out.&savename as
select STD_YYYY
	, sigungu
	, sum(gaibja_type="7" or gaibja_type="8") as count
	, count(*) as num_indi
from store.SEOUL
where input(STD_YYYY, 4.) between 2006 and 2018
group by STD_YYYY, sigungu;
quit;

proc export data=out.&savename
	outfile="/userdata07/room285/data_out/output-household/household.xlsx"
	DBMS=xlsx
	replace;
	sheet="&savename";
run;
%MEND;
/*%GAIBJA_TYPE_7_OR_8;*/


%MACRO MONTHLY_HH_INC_DIST;
PROC SQL;
CREATE TABLE HH_INCOME_GROUP_18 AS
SELECT CASE when inc_tot/12>=0*10**4 and inc_tot/12<100*10**4 then "0만-100만"
		when inc_tot/12>=100*10**4 and inc_tot/12<200*10**4 then "100만-200만"
		when inc_tot/12>=200*10**4 and inc_tot/12<300*10**4 then "200만-300만"
		when inc_tot/12>=300*10**4 and inc_tot/12<400*10**4 then "300만-400만"
		when inc_tot/12>=400*10**4 and inc_tot/12<500*10**4 then "400만-500만"
		when inc_tot/12>=500*10**4 and inc_tot/12<600*10**4 then "500만-600만"
		when inc_tot/12>=600*10**4 and inc_tot/12<700*10**4 then "600만-700만"
		when inc_tot/12>=700*10**4 and inc_tot/12<800*10**4 then "700만-800만"
		when inc_tot/12>=800*10**4 and inc_tot/12<900*10**4 then "800만-900만"
		ELSE "900만+" END AS MONTHLY_INC_TOT
	, SEX_TYPE, HH_SIZE_GROUP, AGE_GROUP
FROM STORE.SEOUL_HH2
WHERE STD_YYYY="2018";
QUIT;

ods excel file="/userdata07/room285/data_out/output-household_validation/household-monthly_inc_tot_dist.xlsx"
	options(sheet_interval='none' sheet_name="monthly_hh_inc_tot_distribution");

PROC TABULATE DATA=WORK.HH_INCOME_GROUP_18;
CLASS MONTHLY_INC_TOT SEX_TYPE age_group hh_size_group;
TABLE MONTHLY_INC_TOT*N, (SEX_TYPE HH_SIZE_GROUP AGE_GROUP) / NOCELLMERGE;
RUN;

PROC TABULATE DATA=WORK.HH_INCOME_GROUP_18;
CLASS MONTHLY_INC_TOT SEX_TYPE age_group hh_size_group;
TABLE MONTHLY_INC_TOT*colpctn, (SEX_TYPE HH_SIZE_GROUP AGE_GROUP) / NOCELLMERGE;
RUN;

ods excel close;
%MEND;


/*%MONTHLY_HH_INC_DIST;*/
