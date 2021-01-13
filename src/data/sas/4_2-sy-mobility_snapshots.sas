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

%macro create_dataset(dname, vname, savename, year_lb, year_ub);
%if "&dname"="seoul_hh2" %then %do;
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

%if &year_lb eq &year_ub %then %do;
	%let year=&year_lb;
	proc sql;
	create table tmp_var as
		select &id, &vname
	from store.&dname
	where STD_YYYY="&year_lb"
	order by &vname;
	quit;
	%end;
%else %do;
	%let year=%substr(&year_lb,3,2)%substr(&year_ub,3,2);
	proc sql;
	create table tmp_var as
		select &id, mean(&vname) as &vname
	from store.&dname
	where input(STD_YYYY, 4.) between &year_lb and &year_ub
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

/* Save decile info */
proc sql;
create table out.&dname._&year._&var._decile as
select rnk+1 as rank
	, count(*) as freq
	, min(&vname) as rank_min
	, max(&vname) as rank_max
	, sum(&vname) as rank_sum
from work.tmp
group by rnk;
quit;

%if "&dname"="seoul_hh2" %then %do;
	proc sql;
	create table tmp2 as 
	select a.INDI_DSCM_NO, b.rnk
	from store.seoul_eq2 as a /* need to match new hh id */
	inner join tmp as b
	on a.HHRR_HEAD_INDI_DSCM_NO=b.HHRR_HEAD_INDI_DSCM_NO
	where input(a.STD_YYYY, 4.) between &year_lb and &year_ub;
	quit;
	/* Individual may have multiple household ID's over multiple years !! */
	/* Don't run until we resolve this issue */
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
from store.SEOUL as a
inner join &merge_dname as b
on a.INDI_DSCM_NO=b.INDI_DSCM_NO
where STD_YYYY="2018";
quit;
%mend create_dataset;


/*----------------------------- RUN COMPLETE UP TO HERE! ------------------------------*/

/* III. 가구 소득(재산) 수준에 따른 개인 노동소득*/
%create_dataset(seoul_eq2, inc_tot, savename=inc_2018, year_lb=2018, year_ub=2018);
%create_dataset(seoul_eq2, prop_txbs_tot, savename=prop_2018, year_lb=2018, year_ub=2018);
%create_dataset(seoul_eq2, inc_tot, savename=inc_2006_2008, year_lb=2006, year_ub=2008);
%create_dataset(seoul_eq2, prop_txbs_tot, savename=prop_2006_2008, year_lb=2006, year_ub=2008);

%macro create_crosstab(sheet_name, savename);
%new_sheet(name=&sheet_name);
proc tabulate data=store.&savename;
class age_group decile;
var inc_labor;
table age_group*inc_labor*mean, decile / nocellmerge;
quit;

proc tabulate data=store.&savename;
class decile;
var inc_labor;
table inc_labor*mean, decile / nocellmerge;
quit;
%mend create_crosstab;

ods excel file="/userdata07/room285/data_out/output-social_mobility/social_mobility-3.xlsx"
	options(sheet_interval='none');

%create_crosstab('3.1) 2018년 균등화소득10분위별 2018년 연령대별 평균 노동소득', inc_2018);
%create_crosstab('3.2) 2018년 균등화재산10분위별 2018년 연령대별 평균 노동소득', prop_2018);
%create_crosstab('3.3) 2006-2008 평균 균등화소득10분위별 2018년 연령대별 평균 노동소득', inc_2006_2008);
%create_crosstab('3.4) 2006-2008 평균 균등화재산10분위별 2018년 연령대별 평균 노동소득', prop_2006_2008);

ods excel close;

proc export data=out.seoul_eq2_0608_prop_decile
	outfile="/userdata07/room285/data_out/output-social_mobility/social_mobility-3.xlsx"
	DBMS=xlsx
	replace;
	sheet="seoul_eq2_0608_prop_decile";
run;

proc export data=out.seoul_eq2_0608_inc_decile
	outfile="/userdata07/room285/data_out/output-social_mobility/social_mobility-3.xlsx"
	DBMS=xlsx
	replace;
	sheet="seoul_eq2_0608_inc_decile";
run;

proc export data=out.seoul_eq2_2018_inc_decile
	outfile="/userdata07/room285/data_out/output-social_mobility/social_mobility-3.xlsx"
	DBMS=xlsx
	replace;
	sheet="seoul_eq2_2018_inc_decile";
run;

proc export data=out.seoul_eq2_2018_prop_decile
	outfile="/userdata07/room285/data_out/output-social_mobility/social_mobility-3.xlsx"
	DBMS=xlsx
	replace;
	sheet="seoul_eq2_2018_prop_decile";
run;
