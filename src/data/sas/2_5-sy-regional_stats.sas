options symbolgen nosqlremerge;

libname DATA '/userdata07/room285/data_source/user_data/201111ADD';
libname OUT '/userdata07/room285/data_out/data_out';
libname STORE '/userdata07/room285/data_out/data_store';

/* 전국 샘플 validity 확인*/
proc sql;
select count(distinct(HHRR_HEAD_INDI_DSCM_NO)) as n_distinct_hh_head
from data.bfc_2010_smpl_xsection;
quit;

proc sql;
select count(*) as count
from data.bfc_2010_smpl_xsection;
select substr(RVSN_ADDR_CD, 1, 2) as sido
	, count(*) as count
from data.bfc_2010_smpl_xsection
group by sido;
quit;


/* fillna(0) */
proc sql;
create table tmp as
select STD_YYYY
/*		, (case when inc_pnsn_occup = . then 0*/
/*			else inc_pnsn_occup end) as inc_pnsn_occup*/
		, sum(inc_pnsn_occup, 0) as inc_pnsn_occup
		, age
	from store.SEOUL_2018;
	select count(inc_pnsn_occup) as nonempty, count(*) as all_count
	from tmp;
	quit;

proc sql;
select count(inc_pnsn_occup) as nonempty, count(*) as all_count
from tmp;
quit;

proc sql;
create table out.seoul_inc_wage_1s as
select STD_YYYY, count(*) as number_of_1s
from store.SEOUL
where inc_wage=1
group by STD_YYYY;
quit;

proc sql;
select count(*) as number_of_1s
from store.SEOUL_2018
where inc_tot=1;
quit;
