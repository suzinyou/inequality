options symbolgen nosqlremerge;

libname DATA '/userdata07/room285/data_source/user_data';
libname OUT '/userdata07/room285/data_out/data_out';
libname STORE '/userdata07/room285/data_out/data_store';

/* proc sql; */
/* create table sy.seoul_2018 as */
/* select * */
/* 	, STD_YYYY - BYEAR as age */
/* from sy.CSV2018; */
/* quit; */

%let region = seoul;
%let unit = indi;
/* %macro compute_age_group_income(region, unit); */
/* %if &unit = 'indi' %then %do */
/* 	%let dname = out.&region; */
/* %else %do */
/* 	%let dname = out.&region._&unit; */
/* %end; */

proc sql;
create table out.&region._byage_&year as
select (case when age < 15 then "0-14"
		when age >= 15 and age < 20 then "15-19"
		when age >= 20 and age < 25 then "20-24"
		when age >= 25 and age < 30 then "25-29"
		when age >= 30 and age < 35 then "30-34"
		when age >= 35 and age < 40 then "35-39"
		when age >= 40 and age < 45 then "40-44"
		when age >= 45 and age < 50 then "45-49"
		when age >= 50 and age < 55 then "50-54"
		when age >= 55 and age < 60 then "55-59"
		when age >= 60 and age < 65 then "60-64"
		when age >= 65 and age < 70 then "65-69"
		when age >= 70 and age < 75 then "70-74"
		when age >= 75 and age < 80 then "75-79"
		when age >= 80 and age < 85 then "80-84"
		else "85+" end) as age_group
	, count(*) as count
	, mean(inc_tot) as mean_inc_tot
    , median(inc_tot) as median_inc_tot
    , mean(inc_wage) as mean_inc_wage
    , median(inc_wage) as median_inc_wage
    , mean(inc_bus) as mean_inc_bus
    , median(inc_bus) as median_inc_bus
from &dname
where STD_YYYY="&year"
group by age_group;
quit;
/* %mend; */


proc sql;
create table out.&region._is_working_&year as
select STD_YYYY
	, (case when inc_tot > 1 then "Y"
		else "N" end) as is_working
	, count(*) as count
	, mean(inc_tot) as mean_inc_tot
    , median(inc_tot) as median_inc_tot
    , mean(inc_wage) as mean_inc_wage
    , median(inc_wage) as median_inc_wage
    , mean(inc_bus) as mean_inc_bus
    , median(inc_bus) as median_inc_bus
from &dname
where STD_YYYY="&year"
group by is_working;
quit;

/* 서울시 425개동별 평균, 중위 소득과 재산세과표*/
proc sql;
create table out.seoul_gu_income_proptxbs as
select STD_YYYY
/* 	, substr(RVSN_ADDR_CD, 6, 3) as dong */
	, count(*) as count
	, mean(inc_tot) as mean_inc_tot
    , median(inc_tot) as median_inc_tot
    , mean(inc_wage) as mean_inc_wage
    , median(inc_wage) as median_inc_wage
    , mean(inc_bus) as mean_inc_bus
    , median(inc_bus) as median_inc_bus
    , mean(prop_txbs_hs) as mean_prop_txbs_hs
	, median(prop_txbs_hs) as median_prop_txbs_hs
	, mean(prop_txbs_tot) as mean_prop_txbs_tot
    , median(prop_txbs_tot) as median_prop_txbs_tot
    , mean(prop_txbs_bldg) as mean_prop_txbs_bldg
    , median(prop_txbs_bldg) as median_prop_txbs_bldg
    , mean(prop_txbs_lnd) as mean_prop_txbs_lnd
    , median(prop_txbs_lnd) as median_prop_txbs_ln
from store.SEOUL_2018
group by dong;
quit;

/* 서울시 25개구 내 직역연금 수급자 분포 */
proc sql;
create table out.seoul_gu_pnsn_occup as
select STD_YYYY
	, sigungu
	, (case when inc_pnsn_occup > 0 then "Y" else "N" end) as is_pnsn_occup_recipient
	, count(*) as count
	, mean(inc_pnsn_occup) as mean_inc_pnsn_occup
	, median(inc_pnsn_occup) as median_inc_pnsn_occup
	, mean(age) as mean_age
	, median(age) as median_age
from (
	select STD_YYYY
		, sigungu
		, sum(inc_pnsn_occup, 0) as inc_pnsn_occup
		, age
	from store.SEOUL
	where STD_YYYY in ("2005", "2010", "2015", "2018")
)
group by STD_YYYY, sigungu, is_pnsn_occup_recipient;
quit;

proc export data=out.seoul_gu_pnsn_occup
	outfile="/userdata07/room285/data_out/data_out/seoul_gu_pnsn_occup.csv"
	replace;
run;

/* where to people work? */
proc sql;
create table out.num_workers_in_out_seoul as
select STD_YYYY
	, firm_sido
	, count(*) as count
from store.SEOUL
where STD_YYYY in ("2005", "2010", "2015", "2018")
group by STD_YYYY, firm_sido;
quit;

proc export data=out.num_workers_in_out_seoul
	outfile="/userdata07/room285/data_out/data_out/seoul_firm_sido.csv"
	replace;
run;

/* Where do people with top 10% wage income work?*/
proc sql;
select rank_min into :top10p_min
from out.seoul_indi_centile
where rank=90 and var="inc_wage";
quit;

proc sql;
create table out.seoul_top10p_wage_gu as
select STD_YYYY
	, sigungu
	, firm_sido
	, firm_sigungu
	, count(*) as count
from store.&region
where STD_YYYY in ("2005", "2010", "2015", "2018")
group by STD_YYYY, sigungu, firm_sido, firm_sigungu;
quit;

proc export data=out.seoul_top10p_wage_gu
	outfile="/userdata07/room285/data_out/data_out/seoul_top10p_wage_gu.csv"
	replace;
run;

/* where are big businesses? */
proc sql;
create table firms as
select STD_YYYY
	, FIRM_CD
	, max(FIRM_SCL_ENTER_NOP_ID) as FIRM_SCL_ENTER_NOP_ID
	, max(input(firm_sigungu, $3.)) as firm_sigungu
from store.SEOUL
where STD_YYYY in  ("2005", "2010", "2015", "2018")
	and firm_sido = "11"
group by STD_YYYY, FIRM_CD;
quit;

proc sql;
create table out.seoul_big_firms as
select STD_YYYY
	, firm_sigungu
	, sum(case 
		when FIRM_SCL_ENTER_NOP_ID >= 100 then 1 
		else 0 end) as n_big_firms
	, count(*) as n_firms
from firms
group by STD_YYYY, firm_sigungu;
quit;

proc export data=out.seoul_big_firms
	outfile="/userdata07/room285/data_out/data_out/seoul_big_firms.csv"
	replace;
run;
