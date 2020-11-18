options symbolgen nosqlremerge;

libname DATA '/userdata07/room285/data_source/user_data';
libname OUT '/userdata07/room285/data_out/data_out';
libname STORE '/userdata07/room285/data_out/data_store';
/* 실거래가 대비 과표 현실화율 */


proc sql;
create table out.seoul_txbs_realization_rate as
select STD_YYYY
	, count(*) as count
	, nmiss(PROP_TXBS_CHG) as prop_txbs_chg_nmiss
	, mean(PROP_TXBS_CHG / SALES_PRICE) as mean_realization_rate
	, min(PROP_TXBS_CHG / SALES_PRICE) as min_realization_rate
	, max(PROP_TXBS_CHG / SALES_PRICE) as max_realization_rate
	, sum(PROP_TXBS_CHG) / sum(SALES_PRICE) as total_realization_rate
from DATA.estate_link
group by STD_YYYY;
quit;

proc sql;
create table out.seoul_gu_txbs_realization_rate as
select STD_YYYY
	, SGG_NM
	, count(*) as count
	, nmiss(PROP_TXBS_CHG) as prop_txbs_chg_nmiss
	, mean(PROP_TXBS_CHG / SALES_PRICE) as mean_realization_rate
	, min(PROP_TXBS_CHG / SALES_PRICE) as min_realization_rate
	, max(PROP_TXBS_CHG / SALES_PRICE) as max_realization_rate
	, sum(PROP_TXBS_CHG) / sum(SALES_PRICE) as total_realization_rate
from DATA.estate_link
group by STD_YYYY, SGG_NM;
quit;

proc sql;
create table out.seoul_type_txbs_realization_rate as
select STD_YYYY
	, GUNMUL_USE
	, count(*) as count
	, nmiss(PROP_TXBS_CHG) as prop_txbs_chg_nmiss
	, mean(PROP_TXBS_CHG / SALES_PRICE) as mean_realization_rate
	, min(PROP_TXBS_CHG / SALES_PRICE) as min_realization_rate
	, max(PROP_TXBS_CHG / SALES_PRICE) as max_realization_rate
	, sum(PROP_TXBS_CHG) / sum(SALES_PRICE) as total_realization_rate
from DATA.estate_link
group by STD_YYYY, GUNMUL_USE, HOUSE_TYPE;
quit;


proc export data=out.seoul_txbs_realization_rate
	outfile="/userdata07/room285/data_out/data_out/seoul_txbs_realization_rate.csv"
	replace;
run;


proc export data=out.seoul_gu_txbs_realization_rate
	outfile="/userdata07/room285/data_out/data_out/seoul_gu_txbs_realization_rate.csv"
	replace;
run;

proc export data=out.seoul_type_txbs_realization_rate
	outfile="/userdata07/room285/data_out/data_out/seoul_type_txbs_realization_rate.csv"
	replace;
run;
