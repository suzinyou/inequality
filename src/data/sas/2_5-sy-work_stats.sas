options symbolgen nosqlremerge;

libname OUT '/userdata07/room285/data_out/output-work';
libname QTS '/userdata07/room285/data_out/output-quantiles';
libname STORE '/userdata07/room285/data_out/data_store';

/* proc sql; */
/* create table sy.seoul_2018 as */
/* select * */
/* 	, STD_YYYY - BYEAR as age */
/* from sy.CSV2018; */
/* quit; */

/* 취업 VS 미취업 인구와 비율 */
%let savename=seoul_sigungu_adult15_working;
proc sql;
create table out.&savename as
select STD_YYYY
	, sigungu
	, sum(case when inc_wage+inc_bus > 0 then 1 else 0 end) as count
	, count(*) as num_indi
from store.seoul
where age >= 15
group by STD_YYYY, sigungu;
quit;

proc sql;
alter table out.&savename
add frac_working num
	, frac_not_working num;
update out.&savename
set frac_working=count / num_indi
	, frac_not_working=(num_indi - count)/num_indi;
quit;

proc export data=out.&savename
	/* CHANGE OUTFILE PATH !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!*/
	outfile="/userdata07/room285/data_out/output-work/work.xlsx"
	DBMS=xlsx
	replace;
	sheet="&savename";
run;

/*  */
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


proc sql;
create table out.seoul_gu_pnsn_occup as
select STD_YYYY
	, count(*) as count
	, (case when inc_pnsn_occup > 1 then "Y" else "N" end) as is_pnsn_occup_recipient
	, mean(inc_pnsn_occup)
	, median(inc_pnsn_occup)
	, mean(age)
	, median(age)
from store.SEOUL
where STD_YYYY in ("2005", "2010", "2015", "2018")
group by STD_YYYY, is_pnsn_occup_recipient;
quit;

/* where to people work? */
proc sql;
create table out.num_workers_in_out_seoul as
select STD_YYYY
	, firm_sido
	, count(*) as count
from store.SEOUL
group by STD_YYYY, firm_sido;
quit;

/* Where do people with top 20% wage income work?*/
proc sql;
create table tmp_wage as
	select STD_YYYY, inc_wage
from store.seoul
where STD_YYYY in ("2006", "2010", "2014", "2018") and age >= 15 and inc_wage > 0
order by STD_YYYY, inc_wage;
quit;

/* 10분위 */
proc rank data=work.tmp_wage groups=10 out=tmp ties=low;
	var inc_wage;
	by STD_YYYY;
	ranks rnk;
run;

%let savename=adult15_earner_wage_decile;
proc sql;
create table out.&savename as
	select STD_YYYY
		, rnk as rank
		, min(&vname) as rank_min
	from tmp group by STD_YYYY, rnk;
quit;
run;

proc sql;
select STD_YYYY, rank_min 
from out.&savename
where rank = "8";
quit;

%let savename=adult15_wage_earner_top20p;
proc sql;
create table out.&savename as
select STD_YYYY
	, sigungu
	, firm_sigungu
	, sum(case when inc_wage >= &top20p_min then 1 else 0 end) as num_top20p_wage
	, count(*) as num_indi
	, count / num_indi as frac_top20p_wage
from tmp
group by STD_YYYY, sigungu, firm_sigungu
where STD_YYYY in ("2006", "2010", "2014", "2018");
quit;

/* where are big businesses? */
proc sql;
create table out.seoul_big_firms as
select STD_YYYY
	, firm_sigungu
	, firm_SCL_ENTER_NOP_ID
	, DISTINCT(FIRM_CD) as FIRM_CD
from store.SEOUL
group by STD_YYYY
where STD_YYYY in ("2005", "2010", "2015", "2018")
	and firm_SCL_ENTER_NOP_ID >= 100;
quit;
