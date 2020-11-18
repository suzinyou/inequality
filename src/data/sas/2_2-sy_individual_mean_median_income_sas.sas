options symbolgen nosqlremerge;

libname DATA '/userdata07/room285/data_source/user_data';
libname OUT '/userdata07/room285/data_out/data_out';
libname STORE '/userdata07/room285/data_out/data_store';

%macro compute_seoul_gu_medians(unit);
%if &unit = indi %then 
	%do;
		%let dataset = store.seoul;
	%end;
%else %if &unit = hh %then
	%do;
		%let dataset = store.seoul_hh;
	%end;

	%let outname = seoul_&unit._medians_per_gu;
proc sql;
create table out.&outname as
select std_yyyy
, sido
, sigungu
, median(inc_tot) as inc_tot
, median(inc_wage) as inc_wage
, median(inc_bus) as inc_bus
/*, median(inc_int) as inc_int*/
/*, median(inc_divid) as inc_divid*/
/*, median(inc_pnsn_natl) as inc_pnsn_natl*/
/*, median(inc_pnsn_occup) as inc_pnsn_occup*/
/*, median(inc_othr) as inc_othr*/
, median(prop_txbs_bldg) as prop_txbs_bldg
, median(prop_txbs_lnd) as prop_txbs_lnd
, median(prop_txbs_hs) as prop_txbs_hs
/*, median(prop_txbs_ship) as prop_txbs_ship*/
, median(prop_txbs_tot) as prop_txbs_tot
from &dataset
group by STD_YYYY, sido, sigungu;
quit;
%mend;

%compute_seoul_gu_medians(hh);
proc export data=out.seoul_hh_medians_per_gu
	outfile="/userdata07/room285/data_out/data_out/seoul_hh_medians_per_gu.csv"
	replace;
run;

%compute_seoul_gu_medians(indi);
proc export data=out.SEOUL_INDI_MEDIANS_PER_GU
	outfile="/userdata07/room285/data_out/data_out/seoul_indi_medians_per_gu.csv"
	replace;
run;
