options symbolgen nosqlremerge;

/*libname DATA '/userdata07/room285/data_source/user_data';*/
/*libname OUT '/userdata07/room285/data_out/data_out';*/
/*libname STORE '/userdata07/room285/data_out/data_store';*/

%macro desc_stat_cols_year(i, col, reg);
proc sql;
    create table work.&reg._yearonly_&i as 
    select
    	STD_YYYY
        , "&col" as var length=16
        , count(&col) as count
        , nmiss(&col) as nmiss
        , mean(&col) as mean
        , max(&col) as max
        , min(&col) as min
        , std(&col) as std
    from store.&reg group by STD_YYYY;
    quit;
run;
%mend desc_stat_cols_year;

%macro desc_stat_cols_year_sido(i, col, reg);
proc sql;
    create table work.&reg._sido_&i as 
    select
    	STD_YYYY
		, sido
        , "&col" as var length=16
        , count(&col) as count
        , nmiss(&col) as nmiss
        , mean(&col) as mean
		, sum(&col) as sum
        , max(&col) as max
        , min(&col) as min
        , std(&col) as std
    from store.&reg group by STD_YYYY, sido;
    quit;
run;
%mend desc_stat_cols_year_sido;

%macro desc_stat_cols_year_sidosigungu(i, col, reg);
proc sql;
    create table work.&reg._sidosigungu_&i as 
    select
    	STD_YYYY
		, sido
		, sigungu
		, "&col" as var length=16
		, count(&col) as count
		, nmiss(&col) as nmiss
		, mean(&col) as mean
		, max(&col) as max
		, min(&col) as min
		, std(&col) as std
    from store.&reg group by STD_YYYY, sido, sigungu;
    quit;
run;
%mend desc_stat_cols_year_sidosigungu;

%macro desc_stat_sidosigungu(region);
%let vnames = inc_wage inc_bus inc_int inc_divid inc_pnsn_natl inc_pnsn_occup inc_othr inc_tot prop_txbs_bldg prop_txbs_lnd prop_txbs_hs prop_txbs_ship prop_txbs_tot;
%local i next_name;
%do i=1 %to %sysfunc(countw(&vnames));
   %let next_name = %scan(&vnames, &i);
   %desc_stat_cols_year(&i, &next_name, &region);
   %desc_stat_cols_year_sidosigungu(&i, &next_name, &region);
%end;
data out.bfc_&region._desc_stat_yearonly;
	set work.&region._yearonly_:;
run;
data out.bfc_&region._desc_stat_sidosigungu;
	set work.&region._sidosigungu_:;
run;
%mend desc_stat_sidosigungu;

%macro desc_stat_sido(region);
%let vnames = inc_wage inc_bus inc_int inc_divid inc_pnsn_natl inc_pnsn_occup inc_othr inc_tot prop_txbs_bldg prop_txbs_lnd prop_txbs_hs prop_txbs_ship prop_txbs_tot;
%local i next_name;
%do i=1 %to %sysfunc(countw(&vnames));
   %let next_name = %scan(&vnames, &i);
   %desc_stat_cols_year(&i, &next_name, &region);
   %if "&next_name" = "prop_txbs_ship" %then 
		%do;  /* TODO: pull this out of conditional */
   			%desc_stat_cols_year_sido(&i, &next_name, &region);
		%end;
%end;
data out.bfc_&region._desc_stat_yearonly;
	set work.&region._yearonly_:;
run;
data out.bfc_&region._desc_stat_sido;
	set work.&region._sido_:;
run;
%mend desc_stat_sido;

/*%desc_stat_sido(smpl);*/
/*%desc_stat_sidosigungu(seoul);*/
/**/
/*proc export data=out.seoul_desc_stat_sidosigungu*/
/*	outfile="/userdata07/room285/data_out/data_out/seoul_desc_stat_sidosigungu.csv"*/
/*	replace;*/
/*run;*/

/*proc export data=out.smpl_desc_stat_sido*/
/*	outfile="/userdata07/room285/data_out/data_out/smpl_desc_stat_sido.csv"*/
/*	replace;*/
/*run;*/

/*proc export data=out.bfc_seoul_desc_stat_yearonly*/
/*	outfile="/userdata07/room285/data_out/data_out/bfc_seoul_desc_stat_yearonly.csv"*/
/*	replace;*/
/*run;*/

/*proc export data=out.smpl_desc_stat_yearonly*/
/*	outfile="/userdata07/room285/data_out/data_out/smpl_desc_stat_yearonly.csv"*/
/*	replace;*/
/*run;*/

%macro desc_stat_seoul_hh;
%let vnames = inc_wage inc_bus inc_tot prop_txbs_hs prop_txbs_tot;
%local i next_name;
%do i=1 %to %sysfunc(countw(&vnames));
   %let next_name = %scan(&vnames, &i);
   %desc_stat_cols_year(&i, &next_name, seoul_hh);
%end;
data out.seoul_hh_desc_stat_yearonly;
	set work.seoul_hh_yearonly_:;
run;
%mend;
%desc_stat_seoul_hh;
proc export data=out.seoul_hh_desc_stat_yearonly
	outfile="/userdata07/room285/data_out/data_out/seoul_hh_desc_stat_yearonly.csv"
	replace;
run;
