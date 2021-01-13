/*OPTIONS NONOTES NOSOURCE NODATE NOCENTER LABEL NONUMBER LS=200 PS=MAX;*/
options symbolgen nosqlremerge;

libname DATA '/userdata07/room285/data_source/user_data';
libname STORE '/userdata07/room285/data_out/data_store';

%macro new_sheet(name=);
ods excel options(sheet_interval="TABLE" sheet_name=&name);
ods select none; data _null_; dcl odsout obj(); run; ods select all;
ods excel options(sheet_interval="NONE" sheet_name=&name);
ods select none; data _null_; dcl odsout obj(); run; ods select all;
%mend new_sheet;

%macro tabulate_household(dname, savename, hh_name);
ods excel file="/userdata07/room285/data_out/output-household_validation/&savename..xlsx"
	options(sheet_interval='none');

%new_sheet(name="&hh_name.원수별 건보세대 유형별 인구");
proc tabulate data=&dname;
class jumin_hh_size nhis_hh_size head_or_dependent;
table (jumin_hh_size="&hh_name.원수" all='Total')*(n='n' colpctn='colp' rowpctn='rowp' pctn='p'),
	(nhis_hh_size='' all='Total')*(head_or_dependent='') / NOCELLMERGE;
quit;

%new_sheet(name="&hh_name.원수별 건보세대 유형별 무소득/무재산 인구");
proc tabulate data=&dname;
var no_inc no_prop no_inc_no_prop;
class jumin_hh_size nhis_hh_size head_or_dependent;
table (jumin_hh_size="&hh_name.원수" all='Total')*(no_inc no_prop no_inc_no_prop)*(n=''),
	(nhis_hh_size='' all='Total')*head_or_dependent='' / NOCELLMERGE;
quit;

%new_sheet(name="&hh_name.원수별 건보세대 유형별 평균총소득, 평균총재산과표");
proc tabulate data=&dname;
var inc_tot prop_txbs_tot;
class jumin_hh_size nhis_hh_size head_or_dependent;
table jumin_hh_size="&hh_name.원수"*(inc_tot prop_txbs_tot)*(MEAN=''),
	nhis_hh_size=''*head_or_dependent='' / NOCELLMERGE;
quit;

%new_sheet(name="&hh_name.원수별 건보세대 유형별 소득>0의 평균총소득");
proc tabulate data=&dname(where=(inc_tot>0));
var inc_tot;
class jumin_hh_size nhis_hh_size head_or_dependent;
table jumin_hh_size="&hh_name.원수"*(inc_tot)*(MEAN=''),
	nhis_hh_size=''*head_or_dependent='' / NOCELLMERGE;
quit;

%new_sheet(name="&hh_name.원수별 건보세대 유형별 재산>0의 평균총재산과표");
proc tabulate data=&dname(where=(prop_txbs_tot>0));
var prop_txbs_tot;
class jumin_hh_size nhis_hh_size head_or_dependent;
table jumin_hh_size="&hh_name.원수"*(prop_txbs_tot)*(MEAN=''),
	nhis_hh_size=''*head_or_dependent='' / NOCELLMERGE;
quit;
ods excel close;
%mend;

/* 아래는 가구 재편 확인용코드... */
proc sql;
create table store.SEOUL_HH1_18 as
select a.INDI_DSCM_NO
	, a.indi_inc_tot as inc_tot
	, a.indi_prop_txbs_tot as prop_txbs_tot
	, b.cnt_id_hhhi_fd
	, case when a.hh_size = 1 then " 1" else "2+" end 
		as jumin_hh_size
	, case when b.cnt_id_hhhi_fd=1 then " 1" else "2+" end 
		as nhis_hh_size
	, case
		when b.GAIBJA_TYPE='' then ''
		when b.GAIBJA_TYPE="1" or b.GAIBJA_TYPE="5" or b.GAIBJA_TYPE="7" then " 세대주"
		else "세대원" end 
		as head_or_dependent
	, case 
		when a.indi_inc_tot=0 then 1 else . end 
		as no_inc
	, case 
		when a.indi_prop_txbs_tot=0 then 1 else . end 
		as no_prop
	, case when a.indi_inc_tot=0 and a.indi_prop_txbs_tot=0 then 1 else . end 
		as no_inc_no_prop
from store.SEOUL_EQ1 as a
left join store.SEOUL as b
on a.STD_YYYY=b.STD_YYYY and a.INDI_DSCM_NO=b.INDI_DSCM_NO
where a.STD_YYYY="2018";
quit;
/* 6. Cross-tabulation에 필요한 변수 생성*/
/*proc sql;*/
/*create table store.SEOUL_HH2_18 as*/
/*select INDI_DSCM_NO*/
/*	, indi_inc_tot as inc_tot*/
/*	, indi_prop_txbs_tot as prop_txbs_tot*/
/*	, cnt_id_hhhi_fd*/
/*	, indi_gaibja_type as gaibja_type*/
/*	, indi_age as age*/
/*	, case when hh_size = 1 then " 1" else "2+" end */
/*		as jumin_hh_size*/
/*	, case when cnt_id_hhhi_fd=1 then " 1" else "2+" end */
/*		as nhis_hh_size*/
/*	, case*/
/*		when INDI_GAIBJA_TYPE='' then ''*/
/*		when INDI_GAIBJA_TYPE="1" or INDI_GAIBJA_TYPE="5" or INDI_GAIBJA_TYPE="7" then " 세대주"*/
/*		else "세대원" end */
/*		as head_or_dependent*/
/*	, case */
/*		when indi_inc_tot=0 then 1 else . end */
/*		as no_inc*/
/*	, case */
/*		when indi_prop_txbs_tot=0 then 1 else . end */
/*		as no_prop*/
/*	, case when indi_inc_tot=0 and indi_prop_txbs_tot=0 then 1 else . end */
/*		as no_inc_no_prop*/
/*from store.SEOUL_EQ2*/
/*where STD_YYYY="2018";*/
/*quit;*/

proc sql;
select count(*) from store.SEOUL_HH1_18;
quit;

/* 7. Generate cross-tabs */
%tabulate_household(STORE.SEOUL_HH1_18, jumin_hh_x_nhis_hh, 주민등록세대);
/*%tabulate_household(STORE.SEOUL_HH2_18, new_hh_x_nhis_hh, 재편가구);*/


/* 가구 크기 */
/*proc sql;*/
/*create table hh2_size as*/
/*select case when hh_size < 8 then "        <8"*/
/*	when hh_size=8 then "       8"*/
/*	when hh_size=9 then "      9"*/
/*	when hh_size=10 then "     10"*/
/*	when hh_size=11 then "    11"*/
/*	when hh_size=12 then "   12"*/
/*	when hh_size=13 then "  13"*/
/*	when hh_size=14 then " 14"*/
/*	else "15+" end as hh_size_group*/
/*	, count(*) as count*/
/*	, mean(inc_tot/hh_size) as mean_indi_inc*/
/*from store.seoul_hh2*/
/*where STD_YYYY="2018"*/
/*group by hh_size_group;*/
/*quit;*/
/**/
/*proc sql;*/
/*create table hh1_size as*/
/*select case when hh_size < 8 then "        <8"*/
/*	when hh_size=8 then "       8"*/
/*	when hh_size=9 then "      9"*/
/*	when hh_size=10 then "     10"*/
/*	when hh_size=11 then "    11"*/
/*	when hh_size=12 then "   12"*/
/*	when hh_size=13 then "  13"*/
/*	when hh_size=14 then " 14"*/
/*	else "15+" end as hh_size_group*/
/*	, count(*) as count*/
/*	, mean(inc_tot/hh_size) as mean_indi_inc*/
/*from store.seoul_hh*/
/*where STD_YYYY="2018"*/
/*group by hh_size_group;*/
/*quit;*/
