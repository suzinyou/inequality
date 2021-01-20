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

%macro create_panel(region, hhtype, vname, savename, ref_year);
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

/* Get reference year and variable for deciles*/
proc sql;
create table tmp_var as
	select *
from store.&region._&hhtype
where STD_YYYY="&ref_year" and sido="11" /* limit to Seoul */
order by &vname;
quit;

/* Assign decile groups to individuals/households in reference year */
proc rank data=work.tmp_var groups=10 out=tmp ties=low;
	var &vname;
	ranks rnk;
run;

/* Save decile info */
proc sql;
create table out.&region._&hhtype._&ref_year._&var._decile as
select rnk
	, count(*) as freq
	, min(&vname) as rank_min
	, max(&vname) as rank_max
	, sum(&vname) as rank_sum
from work.tmp
group by rnk;
quit;

/* Create list of individual ID's of ppl who were 25~29 in 2006, along with their household or equivalized income or property decile*/
%if "&hhtype"="hh2" %then %do;
	/* Match household's decile to individual and filter just 25~29 yo's in ref year*/
	proc sql;
	create table tmp2 as 
	select a.INDI_DSCM_NO, b.rnk
	from store.&region._eq2 as a /* need to match new hh id */
	inner join tmp as b
	on a.new_hh_id=b.HHRR_HEAD_INDI_DSCM_NO
	where a.STD_YYYY="&ref_year" and a.indi_age between 25 and 29;
	quit;
	%end;
%else %do;
	/* Filter 25~29 yo's in reference year*/
	proc sql;
	create table tmp2 as
	select INDI_DSCM_NO, rnk
	from tmp
	where indi_age between 25 and 29;
	quit;
	%end;

/* Create panel data of people who were 25~29 in 2006 */
proc sql;
create table store.&savename as
select a.INDI_DSCM_NO
	, b.rnk+1 as decile
	, a.STD_YYYY
	, a.inc_wage
	, a.inc_wage+a.inc_bus as inc_labor
	, a.sex_type
	, case when a.new_hh_id=a.INDI_DSCM_NO then "head" else "dependent" end as head_or_dependent
from store.SEOULPANEL2 as a 
/* want individual's income! */
inner join tmp2 as b
on a.INDI_DSCM_NO=b.INDI_DSCM_NO
where input(a.STD_YYYY, 4.) between 2006 and 2018;
quit;
%mend create_panel;


/* I. 가구소득 수준별 노동시장 진입 이후 소득 변동의 추세 */
/*		TODO: set base population; anyone living in SEOUL in 2006? or anyone who lives in Seoul from '06~'18?*/
%let savename=inc_panel; 
%create_panel(seoulpanel, eq2, inc_tot, &savename, 2006);

%macro create_base_dataset_analysis1(savename);
/* mean inc_labor by sex */
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

/* mean inc_labor by 가구주 vs 가구원 */
/*proc sql;*/
/*create table decile_inc_labor_by_ishead as*/
/*select STD_YYYY*/
/*	, decile*/
/*	, head_or_dependent*/
/*	, count(*) as count*/
/*	, mean(inc_labor) as mean*/
/*from store.&savename*/
/*group by STD_YYYY, decile, head_or_dependent;*/
/*quit;*/

/* mean inc_labor for 2006년에 1200만원 이상 번 청년 */
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

/* 2006년 소득 10분위별 연도별 mean inc_labor, mean inc_wage, 해당 인구, 무소득자 비율, ...*/
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

/* 위 결과를 하나로 합친 테이블 생성 (연도별, 분위별 OOOOO)*/
proc sql;
create table out.inc_decile_stat as
select a.*
	, b.count as count_12m_in_2006
	, b.mean as mean_inc_labor_12m_in_2006
	, c.count as count_male
	, c.mean as mean_inc_labor_male
	, d.count as count_female
	, d.mean as mean_inc_labor_female
	, e.count as count_head
	, e.mean as mean_inc_labor_head
	, f.count as count_dependent
	, f.mean as mean_inc_labor_dependent
from inc_decile_stat as a
left join decile_inc_labor_12m as b
on a.STD_YYYY=b.STD_YYYY and a.decile=b.decile
left join (select * from decile_inc_labor_by_sex where sex_type="1") as c
on a.STD_YYYY=c.STD_YYYY and a.decile=c.decile
left join (select * from decile_inc_labor_by_sex where sex_type="2") as d
on a.STD_YYYY=d.STD_YYYY and a.decile=d.decile
left join (select * from decile_inc_labor_by_ishead where head_or_dependent="head") as e
on a.STD_YYYY=e.STD_YYYY and a.decile=e.decile
left join (select * from decile_inc_labor_by_ishead where head_or_dependent="dependent") as f
on a.STD_YYYY=f.STD_YYYY and a.decile=f.decile;
quit;
%mend create_base_dataset_analysis1;

%create_base_dataset_analysis1(inc_panel);

/* II. 가구재산 수준별 노동시장 진입 이후 소득 변동의 추세 */
/*		TODO: set base population; anyone living in SEOUL in 2006? or anyone who lives in Seoul from '06~'18?*/
%let savename=prop_panel; 
%create_panel(seoulpanel, hh2, prop_txbs_tot, &savename, ref_year=2006);

proc sql;
create table out.prop_hh2_decile_stat as
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

%let savename=prop_eq2_panel;
%create_panel(seoulpanel, eq2, prop_txbs_tot, &savename, ref_year=2006);

proc sql;
create table out.prop_eq2_decile_stat as
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

/* Generate cross tabulations -------------------------------------------------------*/

ods excel file="/userdata07/room285/data_out/output-social_mobility/social_mobility-1and2.xlsx"
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

/* I. 7. 가구주/가구원 */
/*%new_sheet(name='1.7) 가구주 vs 가구원 평균 노동소득');*/
/*proc tabulate data=out.inc_decile_stat;*/
/*class STD_YYYY decile;*/
/*var mean_inc_labor_head mean_inc_labor_dependent;*/
/*table decile*(mean_inc_labor_head='가구주' mean_inc_labor_dependent='가구원')*min='mean_inc_labor', STD_YYYY / nocellmerge;*/
/*quit;*/
/**/
/*%new_sheet(name='1.7) 가구주 vs 가구원 수');*/
/*proc tabulate data=out.inc_decile_stat;*/
/*class STD_YYYY decile;*/
/*var count_head count_dependent;*/
/*table decile*(count_head='가구주' count_dependent='가구원')*min='count', STD_YYYY / nocellmerge;*/
/*quit;*/

/* II. */
%new_sheet(name='2.1) 2006년 가구재산10분위별 인원수');
proc tabulate data=out.prop_hh2_decile_stat;
class STD_YYYY decile;
var count;
table decile*count*min='', STD_YYYY / nocellmerge;
quit;

/* II. 2. Average income from labor */
%new_sheet(name='2.2) 평균 개인 노동소득');
proc tabulate data=out.prop_hh2_decile_stat;
class STD_YYYY decile;
var mean_inc_labor;
table decile*mean_inc_labor*min='', STD_YYYY / nocellmerge;
quit;

/* II. 3. Average income from wage */
%new_sheet(name='2.3) 평균 개인 근로소득');
proc tabulate data=out.prop_hh2_decile_stat;
class STD_YYYY decile;
var mean_inc_wage;
table decile*mean_inc_wage*min='', STD_YYYY / nocellmerge;
quit;

/* II. (가구 재산 말고 균등화 재산으로 분위 나눴을 때) */
%new_sheet(name='2.1b) 2006년 가구재산10분위별 인원수');
proc tabulate data=out.prop_eq2_decile_stat;
class STD_YYYY decile;
var count;
table decile*count*min='', STD_YYYY / nocellmerge;
quit;

/* II. 2. Average income from labor */
%new_sheet(name='2.2b) 평균 개인 노동소득');
proc tabulate data=out.prop_eq2_decile_stat;
class STD_YYYY decile;
var mean_inc_labor;
table decile*mean_inc_labor*min='', STD_YYYY / nocellmerge;
quit;

/* II. 3. Average income from wage */
%new_sheet(name='2.3b) 평균 개인 근로소득');
proc tabulate data=out.prop_eq2_decile_stat;
class STD_YYYY decile;
var mean_inc_wage;
table decile*mean_inc_wage*min='', STD_YYYY / nocellmerge;
quit;

ods excel close;

proc export data=out.inc_decile_stat
	outfile="/userdata07/room285/data_out/output-social_mobility/social_mobility-1and2.xlsx"
	DBMS=xlsx
	replace;
	sheet="1. inc_decile_stat";
run;

proc export data=out.seoul_eq2_2006_inc_tot_decile
	outfile="/userdata07/room285/data_out/output-social_mobility/social_mobility-1and2.xlsx"
	DBMS=xlsx
	replace;
	sheet="1. seoul_eq2_2006_inc_decile";
run;

proc export data=out.prop_hh2_decile_stat
	outfile="/userdata07/room285/data_out/output-social_mobility/social_mobility-1and2.xlsx"
	DBMS=xlsx
	replace;
	sheet="2. prop_hh2_decile_stat";
run;

proc export data=out.seoul_hh2_2006_prop_decile
	outfile="/userdata07/room285/data_out/output-social_mobility/social_mobility-1and2.xlsx"
	DBMS=xlsx
	replace;
	sheet="2. seoul_hh2_2006_prop_decile";
run;

proc export data=out.prop_eq2_decile_stat
	outfile="/userdata07/room285/data_out/output-social_mobility/social_mobility-1and2.xlsx"
	DBMS=xlsx
	replace;
	sheet="2. prop_eq2_decile_stat";
run;

proc export data=out.seoul_eq2_2006_prop_decile
	outfile="/userdata07/room285/data_out/output-social_mobility/social_mobility-1and2.xlsx"
	DBMS=xlsx
	replace;
	sheet="2. seoul_eq2_2006_prop_decile";
run;
