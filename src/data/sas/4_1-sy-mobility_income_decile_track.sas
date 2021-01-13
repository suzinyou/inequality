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

%macro create_panel(dname, vname, savename, year_lb, year_ub);
%if &vname=prop_txbs_tot %then %do;
	%let id=HHRR_HEAD_INDI_DSCM_NO;
	%end;
%else %do;
	%let id=INDI_DSCM_NO;
	%end;

%if &year_lb eq &year_ub %then %do;
	%let year=&year_lb;
	%let age_group=;
	%let age_filter=and age between 25 and 29;
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
	%let age_group=, case when age<15 then '' when age>=15 and age<=19 then "15-19" when age>=20 and age<=24 then "20-24" when age>=25 and age<=29 then "25-29" when age>=30 and age<=34 then "30-34" when age>=35 and age<=39 then "35-39" when age>=40 and age<=44 then "40-44" when age>=45 and age<=49 then "45-49" when age>=50 and age<=54 then "50-54" when age>=55 and age<=59 then "55-59" when age>=60 and age<=64 then "60-64" else "65+" end as age_group;
	%let age_filter=;
	proc sql;
	create table tmp_var as
		select &id, sum(&vname)
	from store.&dname
	where input(STD_YYYY, 4.) between &year_lb and &year_ub
	group by &id
	order by &vname;
	quit;
	%end;

/* Assign decile groups to individuals in base year */
proc rank data=work.tmp_var groups=10 out=tmp ties=low;
	var &vname;
	ranks rnk;
run;

/* Save decile info */
proc sql;
create table out.&dname._&year._&vname._decile as
select rnk
	, count(*) as freq
	, min(&vname) as rank_min
	, max(&vname) as rank_max
	, sum(&vname) as rank_sum
from work.tmp
group by rnk;
quit;

/* Create panel data of people who were 25~29 in 2006 */
proc sql;
create table store.&savename as
select a.INDI_DSCM_NO
	, b.rnk+1 as decile
	, a.STD_YYYY
	, a.inc_wage
	, a.inc_wage+a.inc_bus as inc_labor
	, a.sex_type&age_group
from store.SEOULPANEL as a 
/* want individual's income! */
inner join tmp as b
on a.&id.=b.&id
where input(a.STD_YYYY, 4.) between 2006 and 2018 &age_filter;
quit;
%mend create_panel;


/* I. 가구소득 수준별 노동시장 진입 이후 소득 변동의 추세 */
/*		TODO: set base population; anyone living in SEOUL in 2006? or anyone who lives in Seoul from '06~'18?*/
%let savename=inc_panel; 
%create_panel(seoul_eq2, 2006, inc_tot, &savename);

/* I. 1. Create base dataset */
proc sql;
create table decile_inc_labor_by_sex as
select STD_YYYY
	, decile
	, sex_type
	, count(*) as count
	, mean(inc_labor) as mean
from store.&savename
group by STD_YYYY, decile, sex_type;
quit;

proc sql;
create table tmp_working as
select INDI_DSCM_NO
from store.&savename
where STD_YYYY="2006" and inc_labor >= 12*10**6;
quit;

proc sql;
create table decile_inc_labor_12m as
select a.STD_YYYY
	, a.decile
	, mean(a.inc_labor) as mean
	, count(*) as count
from store.&savename as a
inner join tmp_working as b
on a.INDI_DSCM_NO=b.INDI_DSCM_NO
group by STD_YYYY, decile;
quit;

proc sql;
create table inc_decile_stat as
select STD_YYYY
	, decile
	, mean(inc_labor) as mean_inc_labor
	, mean(inc_wage) as mean_inc_wage
	, count(*) as count
	, sum(inc_labor=0) as count_zero_inc_labor
	, sum(inc_labor=0)/count(*) as frac_zero_inc_labor
from store.&savename
group by STD_YYYY, decile;
quit;

proc sql;
create table out.inc_decile_stat as
select a.*
	, b.count as count_12m_in_2006
	, b.mean as mean_inc_labor_12m_in_2006
	, c.count as count_male
	, c.mean as mean_inc_labor_male
	, d.count as count_female
	, d.mean as mean_inc_labor_female
from inc_decile_stat as a
left join decile_inc_labor_12m as b
on a.STD_YYYY=b.STD_YYYY and a.decile=b.decile
left join (select * from decile_inc_labor_by_sex where sex_type="1") as c
on a.STD_YYYY=c.STD_YYYY and a.decile=c.decile
left join (select * from decile_inc_labor_by_sex where sex_type="2") as d
on a.STD_YYYY=d.STD_YYYY and a.decile=d.decile;
quit;

ods excel file="/userdata07/room285/data_out/output-social_mobility/social_mobility.xlsx"
	options(sheet_interval='none');

%new_sheet(name="1.1) 2006년 균등화소득10분위별 인원수");
/*ods excel options(sheet_name='1.1) 2006년 균등화소득10분위별 인원수');*/
proc tabulate data=out.inc_decile_stat;
class STD_YYYY decile;
var count;
table decile*count*min='', STD_YYYY / nocellmerge;
quit;

/* I. 2. Average income from labor */
%new_sheet(name='1.2) 평균 개인 노동소득');
/*ods excel options(sheet_name='1.2) 평균 개인 노동소득');*/
proc tabulate data=out.inc_decile_stat;
class STD_YYYY decile;
var mean_inc_labor;
table decile*mean_inc_labor*min='', STD_YYYY / nocellmerge;
quit;

/* I. 3. Average income from wage */
%new_sheet(name='1.3) 평균 개인 근로소득');
/*ods excel options(sheet_name='1.3) 평균 개인 근로소득');*/
proc tabulate data=out.inc_decile_stat;
class STD_YYYY decile;
var mean_inc_wage;
table decile*mean_inc_wage*min='', STD_YYYY / nocellmerge;
quit;

/* I. 4. Average income from labor, by sex */
%new_sheet(name='1.4) 성별 평균 개인 노동소득');
/*ods excel options(sheet_name='1.4) 성별 평균 개인 노동소득');*/
proc tabulate data=out.inc_decile_stat;
class STD_YYYY decile;
var mean_inc_labor_male mean_inc_labor_female;
table decile*(mean_inc_labor_male='male' mean_inc_labor_female='female')*min='mean_inc_labor', STD_YYYY / nocellmerge;
quit;

%new_sheet(name='1.4) 성별 인원수');
/*ods excel options(sheet_name='1.4) 성별 인원수');*/
proc tabulate data=out.inc_decile_stat;
class STD_YYYY decile;
var count_male count_female;
table decile*(count_male='male' count_female='female')*min='count', STD_YYYY / nocellmerge;
quit;

/* I. 5. Number of people who have zero labor income */
%new_sheet(name='1.5) 개인 노동소득 0원 비중');
/*ods excel options(sheet_name='1.5) 개인 노동소득 0원 비중');*/
proc tabulate data=out.inc_decile_stat;
class STD_YYYY decile;
var frac_zero_inc_labor;
table decile*frac_zero_inc_labor*min='', STD_YYYY / nocellmerge;
quit;

/* I. 6. How well have people who were working in 2006 been doing?  */
%new_sheet(name='1.6) 2006년에 노동시장 진입한 청년 평균 노동소득');
/*ods excel options(sheet_name='1.6) 2006년에 노동시장 진입한 청년 평균 노동소득');*/
proc tabulate data=out.inc_decile_stat;
class STD_YYYY decile;
var mean_inc_labor_12m_in_2006;
table decile*mean_inc_labor_12m_in_2006='mean_inc_labor'*min='', STD_YYYY / nocellmerge;
quit;

%new_sheet(name='1.6) 2006년에 노동시장 진입한 청년 인원수');
/*ods excel options(sheet_name='1.6) 2006년에 노동시장 진입한 청년 인원수');*/
proc tabulate data=out.inc_decile_stat;
class STD_YYYY decile;
var count_12m_in_2006;
table decile*count_12m_in_2006='count'*min='', STD_YYYY / nocellmerge;
quit;

ods excel close;

proc export data=out.inc_decile_stat
	outfile="/userdata07/room285/data_out/output-social_mobility/social_mobility.xlsx"
	DBMS=xlsx
	replace;
	sheet="inc_decile_stat";
run;

proc export data=out.seoul_eq2_2006_inc_tot_decile
	outfile="/userdata07/room285/data_out/output-social_mobility/social_mobility.xlsx"
	DBMS=xlsx
	replace;
	sheet="2006년 서울 균등화총소득 10분위";
run;

/* II. 가구재산 수준별 노동시장 진입 이후 소득 변동의 추세 */
/*		TODO: set base population; anyone living in SEOUL in 2006? or anyone who lives in Seoul from '06~'18?*/
%let savename=prop_panel; 
%create_panel(seoul_hh2, prop_txbs_tot, &savename, year_lb=2006, year_ub=2006);
/* 만약 균등화 아닌 총 가구재산이면 코드 다시써야함~ STORE.SEOUL_HH2로, join on hhrr_head */
proc sql;
create table out.prop_decile_stat as
select STD_YYYY
	, decile
	, mean(inc_labor) as mean_inc_labor
	, mean(inc_wage) as mean_inc_wage
	, count(*) as count
	, sum(inc_labor=0) as count_zero_inc_labor
	, sum(inc_labor=0)/count(*) as frac_zero_inc_labor
from store.&savename
group by STD_YYYY, decile;
quit;

ods excel options(sheet_name='2.1) 2006년 가구재산10분위별 인원수');
proc tabulate data=out.prop_decile_stat;
class STD_YYYY decile;
var count;
table decile*count*min='', STD_YYYY / nocellmerge;
quit;

/*----------------------------- RUN COMPLETE UP TO HERE! ------------------------------*/

/* II. 2. Average income from labor */
ods excel options(sheet_name='2.2) 평균 개인 노동소득');
proc tabulate data=out.prop_decile_stat;
class STD_YYYY decile;
var mean_inc_labor;
table decile*mean_inc_labor*min='', STD_YYYY / nocellmerge;
quit;

/* II. 3. Average income from wage */
ods excel options(sheet_name='2.3) 평균 개인 근로소득');
proc tabulate data=out.prop_decile_stat;
class STD_YYYY decile;
var mean_inc_wage;
table decile*mean_inc_wage*min='', STD_YYYY / nocellmerge;
quit;

/* III. 가구 소득(재산) 수준에 따른 개인 노동소득*/
proc sql;
create table pop as
select INDI_DSCM_NO
from store.seoulpanel
where STD_YYYY="2018" and age between 20 and 49;
quit;


proc sql;
create table inc_ten_yrs_ago as
select INDI_DSCM_NO
	, sum(inc_tot)
from store.seoul_eq2
where input(STD_YYYY, 4.) between 2006 and 2008
group by INDI_DSCM_NO, STD_YYYY;
quit;

/*.... TODO */

/* SAVE ALL TO EXCEL */
libname savepath excel "/userdata07/room285/data_out/output-social_mobility/social_mobility.xlsx";
proc copy in=savepath out=out;
run;
libname savepath clar;
