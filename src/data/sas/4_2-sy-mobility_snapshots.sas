/* Social Mobility */
options symbolgen nosqlremerge;

libname OUT '/userdata07/room285/data_out/output-social_mobility';
libname STORE '/userdata07/room285/data_out/data_store';

%macro new_sheet(name=);
ods excel options(sheet_interval="TABLE" sheet_name=&name);
ods select none; data _null_; dcl odsout obj(); run; ods select all;
ods excel options(sheet_interval="NONE" sheet_name=&name);
ods select none; data _null_; dcl odsout obj(); run; ods select all;
%mend new_sheet;

%macro create_dataset(region, hhtype, vname, savename, year_lb, year_ub, multihh=0);
%if "&hhtype"="hh2" %then %do;
	%let id=HHRR_HEAD_INDI_DSCM_NO;
	%end;
%else %do;
	%let id=INDI_DSCM_NO;
	%end;

%if "&vname"="inc_tot" %then %do;
	%let var=inc;
	%end;
%else %do;
	%let var=prop;
	%end;

%if &multihh=1 %then %do;
	%let extra_where=and hh_size>=2;
	%end;
%else %do;
	%let extra_where=;
	%end;

%if &year_lb eq &year_ub %then %do;
/*	single year */
	%let year=&year_lb;
	proc sql;
	create table tmp_var as
		select &id, &vname
	from store.&region._&hhtype
	where STD_YYYY="&year_lb" &extra_where
	order by &vname;
	quit;
	%end;
%else %do;
/* average over multiple years */
	%let year=%substr(&year_lb,3,2)%substr(&year_ub,3,2);
	proc sql;
	create table tmp_var as
		select &id, mean(&vname) as &vname
	from store.&region._&hhtype
	where input(STD_YYYY, 4.) between &year_lb and &year_ub &extra_where
	group by &id;
	quit;
	proc sort data=tmp_var;
	by &vname;
	run;
	%end;

/* Assign decile groups to individuals in year(s) &year_lb ~ &year_ub*/
proc rank data=work.tmp_var groups=10 out=tmp ties=low;
	var &vname;
	ranks rnk;
run;

%let decile_savename=&region._&hhtype._&year._&var._decile;
%if &multihh=1 %then %do;
	%let decile_savename=&decile_savename._multi;
	%end;

/* Save decile info */
proc sql;
create table out.&decile_savename as
select rnk+1 as rank
	, count(*) as freq
	, min(&vname) as rank_min
	, max(&vname) as rank_max
	, sum(&vname) as rank_sum
from work.tmp
group by rnk;
quit;

%if "&hhtype"="hh2" and &year_lb eq &year_ub %then %do;
	/* 가구 기준으로 10분위 봤다면 각 개인이 어떤 가구, 즉 어떤 분위 해당하는지 파악*/
	proc sql;
	create table tmp2 as 
	select a.INDI_DSCM_NO, b.rnk
	from store.seoul_eq2 as a /* need to match new hh id */
	inner join tmp as b
	on a.new_hh_id=b.HHRR_HEAD_INDI_DSCM_NO
	where a.STD_YYYY="&year_lb";
	quit;
	%let merge_dname=tmp2;
	%end;
%else %do;
	%let merge_dname=tmp;
	%end;

/* Create dataset of people in 2018 */
proc sql;
create table store.&savename as
select a.INDI_DSCM_NO
	, b.rnk+1 as decile
	, a.STD_YYYY
	, a.inc_wage
	, a.inc_wage+a.inc_bus as inc_labor
	, a.sex_type
	, case 
		when a.age<15 then '' 
		when a.age>=15 and a.age<=19 then "15-19" 
		when a.age>=20 and a.age<=24 then "20-24" 
		when a.age>=25 and a.age<=29 then "25-29" 
		when a.age>=30 and a.age<=34 then "30-34" 
		when a.age>=35 and a.age<=39 then "35-39" 
		when a.age>=40 and a.age<=44 then "40-44" 
		when a.age>=45 and a.age<=49 then "45-49" 
		when a.age>=50 and a.age<=54 then "50-54" 
		when a.age>=55 and a.age<=59 then "55-59" 
		when a.age>=60 and a.age<=64 then "60-64" 
		else "65+" end 
	as age_group
	, case when a.INDI_DSCM_NO=a.new_hh_id then "head" else "dependent" end as head_or_dependent
from store.SEOULPANEL2 as a /*SEOULPANEL2는 재편된 가구id가 붙어있음 (new_hh_id)*/
inner join &merge_dname as b
on a.INDI_DSCM_NO=b.INDI_DSCM_NO
where STD_YYYY="2018";
quit;
%mend create_dataset;

%macro create_crosstab(sheet_name, savename, dependent=0, by_sex=0);
%if &dependent=1 %then %do;
	%let data=store.&savename(where=(head_or_dependent="dependent"));
	%end;
%else %do;
	%let data=store.&savename;
	%end;

%if &by_sex=1 %then %do;
	%let extra_class=sex_type;
	%let extra_table_dim=sex_type*;
	%end;
%else %do;
	%let extra_class=;
	%let extra_table_dim=;
	%end;

%new_sheet(name=&sheet_name);
proc tabulate data=&data;
class age_group decile &extra_class;
var inc_labor;
table age_group*&extra_table_dim.inc_labor*mean, decile / nocellmerge;
quit;

proc tabulate data=&data;
class decile &extra_class;
var inc_labor;
table &extra_table_dim.inc_labor*mean, decile / nocellmerge;
quit;
%mend create_crosstab;

/* III. 가구 소득(재산) 수준에 따른 개인 노동소득*/
/*%create_dataset(seoul, eq2, inc_tot, savename=inc_eq2_2018, year_lb=2018, year_ub=2018);*/
/*%create_dataset(seoul, hh2, prop_txbs_tot, savename=prop_hh2_2018, year_lb=2018, year_ub=2018);*/
/**/
/*%create_dataset(seoul, eq2, inc_tot, savename=inc_eq2_2006, year_lb=2006, year_ub=2006);*/
/*%create_dataset(seoul, hh2, prop_txbs_tot, savename=prop_hh2_2006, year_lb=2006, year_ub=2006);*/
/**/
/*%create_dataset(seoul, eq2, prop_txbs_tot, savename=prop_eq2_2018, year_lb=2018, year_ub=2018);*/
/*%create_dataset(seoul, eq2, inc_tot, savename=inc_eq2_2006_2008, year_lb=2006, year_ub=2008);*/
%create_dataset(seoul, eq2, prop_txbs_tot, savename=prop_eq2_2006_2008, year_lb=2006, year_ub=2008);

/* 가구주+가구원 모두 */
/*ods excel file="/userdata07/room285/data_out/output-social_mobility/social_mobility-3.xlsx"*/
/*	options(sheet_interval='none');*/
/**/
/*%create_crosstab('3.1) 2018 균등화소득10분위 2018 연령대별 평균 노동소득', inc_eq2_2018);*/
/*%create_crosstab('3.2) 2018 가구재산10분위 2018 연령대별 평균 노동소득', prop_hh2_2018);*/
/*%create_crosstab('3.3) 2006 균등화소득10분위 2018 연령대별 평균 노동소득', inc_eq2_2006);*/
/*%create_crosstab('3.4) 2006 가구재산10분위 2018 연령대별 평균 노동소득', prop_hh2_2006);*/
/*%create_crosstab('3.2b) 2018 균등화재산10분위 2018 연령대별 평균 노동소득', prop_eq2_2018);*/
/*%create_crosstab('3.3b) 2006-2008 평균 균등화소득10분위 2018 연령대별 평균 노동소득', inc_eq2_2006_2008);*/
/*%create_crosstab('3.4b) 2006-2008 평균 균등화재산10분위 2018 연령대별 평균 노동소득', prop_eq2_2006_2008);*/
/**/
/*ods excel close;*/
/**/
/*proc export data=out.seoul_eq2_2018_inc_decile*/
/*	outfile="/userdata07/room285/data_out/output-social_mobility/social_mobility-3.xlsx"*/
/*	DBMS=xlsx*/
/*	replace;*/
/*	sheet="3.1)seoul_eq2_2018_inc_decile";*/
/*run;*/
/**/
/*proc export data=out.seoul_hh2_2018_prop_decile*/
/*	outfile="/userdata07/room285/data_out/output-social_mobility/social_mobility-3.xlsx"*/
/*	DBMS=xlsx*/
/*	replace;*/
/*	sheet="3.2)seoul_hh2_2018_prop_decile";*/
/*run;*/
/**/
/*proc export data=out.seoul_eq2_2006_inc_decile*/
/*	outfile="/userdata07/room285/data_out/output-social_mobility/social_mobility-3.xlsx"*/
/*	DBMS=xlsx*/
/*	replace;*/
/*	sheet="3.3)seoul_eq2_2006_inc_decile";*/
/*run;*/
/**/
/*proc export data=out.seoul_hh2_2006_prop_decile*/
/*	outfile="/userdata07/room285/data_out/output-social_mobility/social_mobility-3.xlsx"*/
/*	DBMS=xlsx*/
/*	replace;*/
/*	sheet="3.4)seoul_hh2_2006_prop_decile";*/
/*run;*/
/**/
/**/
/*proc export data=out.seoul_eq2_2018_prop_decile*/
/*	outfile="/userdata07/room285/data_out/output-social_mobility/social_mobility-3.xlsx"*/
/*	DBMS=xlsx*/
/*	replace;*/
/*	sheet="3.2b)seoul_eq2_2018_prop_decile";*/
/*run;*/
/**/
/*proc export data=out.seoul_eq2_0608_inc_decile*/
/*	outfile="/userdata07/room285/data_out/output-social_mobility/social_mobility-3.xlsx"*/
/*	DBMS=xlsx*/
/*	replace;*/
/*	sheet="3.3b)seoul_eq2_0608_inc_decile";*/
/*run;*/
/**/
/*proc export data=out.seoul_eq2_0608_prop_decile*/
/*	outfile="/userdata07/room285/data_out/output-social_mobility/social_mobility-3.xlsx"*/
/*	DBMS=xlsx*/
/*	replace;*/
/*	sheet="3.4b)seoul_eq2_0608_prop_decile";*/
/*run;*/

/*----------------------------- RUN COMPLETE UP TO HERE! ------------------------------*/
/*가구원인 경우만*/
/*%create_dataset(seoul, eq2, inc_tot, savename=inc_eq2_2018_multi, year_lb=2018, year_ub=2018, multihh=1);*/
/*%create_dataset(seoul, hh2, prop_txbs_tot, savename=prop_hh2_2018_multi, year_lb=2018, year_ub=2018, multihh=1);*/
/**/
/*%create_dataset(seoul, eq2, inc_tot, savename=inc_eq2_2006_multi, year_lb=2006, year_ub=2006, multihh=1);*/
/*%create_dataset(seoul, hh2, prop_txbs_tot, savename=prop_hh2_2006_multi, year_lb=2006, year_ub=2006, multihh=1);*/
/**/
/*%create_dataset(seoul, eq2, prop_txbs_tot, savename=prop_eq2_2018_multi, year_lb=2018, year_ub=2018, multihh=1);*/
/*%create_dataset(seoul, eq2, inc_tot, savename=inc_eq2_2006_2008_multi, year_lb=2006, year_ub=2008, multihh=1);*/
/*%create_dataset(seoul, eq2, prop_txbs_tot, savename=prop_eq2_2006_2008_multi, year_lb=2006, year_ub=2008, multihh=1);*/
/**/
/**/
/*ods excel file="/userdata07/room285/data_out/output-social_mobility/social_mobility-3_multi_hh_dependent.xlsx"*/
/*	options(sheet_interval='none');*/
/**/
/*%create_crosstab('3.1) 2018 균등화소득10분위 2018 연령별', inc_eq2_2018_multi, dependent=1);*/
/*%create_crosstab('3.1.1) 2018 균등화소득10분위 2018 연령x성별', inc_eq2_2018_multi, dependent=1, by_sex=1);*/
/*%create_crosstab('3.2) 2018 가구재산10분위 2018 연령별', prop_hh2_2018_multi, dependent=1);*/
/*%create_crosstab('3.2.1) 2018 가구재산10분위 2018 연령x성별', prop_hh2_2018_multi, dependent=1, by_sex=1);*/
/*%create_crosstab('3.3) 2006 균등화소득10분위 2018 연령별', inc_eq2_2006_multi, dependent=1);*/
/*%create_crosstab('3.3.1) 2006 균등화소득10분위 2018 연령x성별', inc_eq2_2006_multi, dependent=1, by_sex=1);*/
/*%create_crosstab('3.4) 2006 가구재산10분위 2018 연령별', prop_hh2_2006_multi, dependent=1);*/
/*%create_crosstab('3.4.1) 2006 가구재산10분위 2018 연령x성별', prop_hh2_2006_multi, dependent=1, by_sex=1);*/
/*%create_crosstab('3.2b) 2018 균등화재산10분위 2018 연령별', prop_eq2_2018_multi, dependent=1);*/
/*%create_crosstab('3.2b.1) 2018 균등화재산10분위 2018 연령x성별', prop_eq2_2018_multi, dependent=1, by_sex=1);*/
/*%create_crosstab('3.3b) 2006-8 평균 균등화소득10분위 2018 연령별', inc_eq2_2006_2008_multi, dependent=1);*/
/*%create_crosstab('3.3b.1) 2006-8 평균 균등화소득10분위 2018 연령X성별', inc_eq2_2006_2008_multi, dependent=1, by_sex=1);*/
/*%create_crosstab('3.4b) 2006-8 평균 균등화재산10분위 2018 연령별', prop_eq2_2006_2008_multi, dependent=1);*/
/*%create_crosstab('3.4b.1) 2006-8 평균 균등화재산10분위 2018 연령x성별', prop_eq2_2006_2008_multi, dependent=1, by_sex=1);*/
/**/
/*ods excel close;*/
/**/
/*proc export data=out.seoul_eq2_2018_inc_decile_multi*/
/*	outfile="/userdata07/room285/data_out/output-social_mobility/social_mobility-3_multi_hh_dependent.xlsx"*/
/*	DBMS=xlsx*/
/*	replace;*/
/*	sheet="3.1)seoul_eq2_2018_inc_decile";*/
/*run;*/
/**/
/*proc export data=out.seoul_hh2_2018_prop_decile_multi*/
/*	outfile="/userdata07/room285/data_out/output-social_mobility/social_mobility-3_multi_hh_dependent.xlsx"*/
/*	DBMS=xlsx*/
/*	replace;*/
/*	sheet="3.2)seoul_hh2_2018_prop_decile";*/
/*run;*/
/**/
/*proc export data=out.seoul_eq2_2006_inc_decile_multi*/
/*	outfile="/userdata07/room285/data_out/output-social_mobility/social_mobility-3_multi_hh_dependent.xlsx"*/
/*	DBMS=xlsx*/
/*	replace;*/
/*	sheet="3.3)seoul_eq2_2006_inc_decile";*/
/*run;*/
/**/
/*proc export data=out.seoul_hh2_2006_prop_decile_multi*/
/*	outfile="/userdata07/room285/data_out/output-social_mobility/social_mobility-3_multi_hh_dependent.xlsx"*/
/*	DBMS=xlsx*/
/*	replace;*/
/*	sheet="3.4)seoul_hh2_2006_prop_decile";*/
/*run;*/
/**/
/**/
/*proc export data=out.seoul_eq2_2018_prop_decile_multi*/
/*	outfile="/userdata07/room285/data_out/output-social_mobility/social_mobility-3_multi_hh_dependent.xlsx"*/
/*	DBMS=xlsx*/
/*	replace;*/
/*	sheet="3.2b)seoul_eq2_2018_prop_decile";*/
/*run;*/
/**/
/*proc export data=out.seoul_eq2_0608_inc_decile_multi*/
/*	outfile="/userdata07/room285/data_out/output-social_mobility/social_mobility-3_multi_hh_dependent.xlsx"*/
/*	DBMS=xlsx*/
/*	replace;*/
/*	sheet="3.3b)seoul_eq2_0608_inc_decile";*/
/*run;*/
/**/
/*proc export data=out.seoul_eq2_0608_prop_decile_multi*/
/*	outfile="/userdata07/room285/data_out/output-social_mobility/social_mobility-3_multi_hh_dependent.xlsx"*/
/*	DBMS=xlsx*/
/*	replace;*/
/*	sheet="3.4b)seoul_eq2_0608_prop_decile";*/
/*run;*/
