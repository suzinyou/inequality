options symbolgen nosqlremerge;

libname DATA '/userdata07/room285/data_source/user_data';
libname OUT '/userdata07/room285/data_out/data_out';
libname STORE '/userdata07/room285/data_out/data_store';

/*proc sql;*/
/*create table whatsmissing as*/
/*select SEX_TYPE, GAIBJA_TYPE, BYEAR, AGE, SIDO, SIGUNGU, FOREIGNER_Y*/
/*from STORE.SEOUL_2011*/
/*where missing(INC_TOT);*/
/*quit;*/
/*proc sql;*/
/*create table whosmissing as*/
/*select SEX_TYPE, GAIBJA_TYPE, FOREIGNER_Y, count(*) as count*/
/*from whatsmissing*/
/*group by SEX_TYPE, GAIBJA_TYPE, FOREIGNER_Y;*/
/*quit;*/
/*proc sql;*/
/*create table whosmissing_gender as*/
/*select SEX_TYPE, count(*) as count*/
/*from whatsmissing*/
/*group by SEX_TYPE;*/
/*quit;*/
/*proc sql;*/
/*create table whosmissing_foreign as*/
/*select FOREIGNER_Y, count(*) as count*/
/*from whatsmissing*/
/*group by FOREIGNER_Y;*/
/*quit;*/
/*proc sql;*/
/*create table whosmissing_gaibja as*/
/*select GAIBJA_TYPE, count(*) as count*/
/*from whatsmissing*/
/*group by GAIBJA_TYPE;*/
/*quit;*/
/**/
/**/
/*PROC SQL;*/
/*SELECT STD_YYYY, GAIBJA_TYPE, COUNT(*) AS COUNT*/
/*FROM STORE.SEOUL*/
/*GROUP BY STD_YYYY, GAIBJA_TYPE;*/
/*QUIT;*/

PROC MI DATA=STORE.SEOUL_2018;
VAR SEX_TYPE FOREIGNER_Y GAIBJA_TYPE EMP_Y INC_TOT PROP_TXBS_TOT;
ODS SELECT MISSPATTERN;
RUN;

PROC FORMAT;
VALUE NM . = '.' OTHER = 'X';
VALUE $CH ' ' = '.' OTHER = 'X';
VALUE AGEFMT
	LOW -< 10 = '<10' 
	10 -< 20 = '10<=X<20'
	20 - HIGH = '>=20';
RUN;

PROC FREQ DATA=STORE.SEOUL_2018;
TABLE AGE*INC_TOT*PROP_TXBS_TOT / LIST MISSING NOCUM;
FORMAT INC_TOT NM. PROP_TXBS_TOT NM. AGE AGEFMT.;
RUN;


proc sql;
create table out.validation_seoul_missing_inc_count as
select STD_YYYY
	, (case 
		when inc_wage = . 
			and inc_bus = . 
			and inc_divid=. 
			and inc_int=.
			and inc_pnsn_natl=.
			and inc_pnsn_occup=.
		then "Y" else "N" end) as all_empty
	, (case when inc_tot=. then "Y" else "N" end) as inc_tot_empty
	, count(*) as count
	, mean(age) as mean_age
	, median(age) as median_age
from store.seoul
where STD_YYYY in ('2005', '2010', '2015', '2018')
group by STD_YYYY, all_empty, inc_tot_empty;
quit;

proc export data=out.validation_seoul_missing_inc_count
	outfile="/userdata07/room285/data_out/data_out/validation_seoul_missing_inc_count.csv"
	replace;
run;

proc sql;
create table out.validation_seoul_income_1_count as
select STD_YYYY
	, sum(case when inc_tot=1 then 1 else 0 end) as num1_tot
	, sum(case when inc_wage=1 then 1 else 0 end) as num1_wage
	, sum(case when inc_bus=1 then 1 else 0 end) as num1_bus
from store.SEOUL
group by STD_YYYY;
quit;

proc export data=out.validation_seoul_income_1_count
	outfile="/userdata07/room285/data_out/data_out/validation_seoul_income_1_count.csv"
	replace;
run;

proc sql;
create table out.validation_seoul_missing_underage as
select STD_YYYY
	, (case 
		when age <15 then "0-14" 
		when age >=15 and age < 19 then "15-18"
		else "19-" end) as age_group
	, sum(case when inc_tot=. then 1 else 0 end) as nmiss_tot
	, sum(case when inc_wage=. then 1 else 0 end) as nmiss_wage
	, sum(case when inc_bus=. then 1 else 0 end) as nmiss_bus
	, count(*) as count
from store.SEOUL
where STD_YYYY in ('2005', '2010', '2015', '2018')
group by STD_YYYY, age_group;
quit;

proc export data=out.validation_seoul_missing_underage
	outfile="/userdata07/room285/data_out/data_out/validation_seoul_missing_underage.csv"
	replace;
run;
