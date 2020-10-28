options symbolgen;

/* data inc_variables; */
/* input name $; */
/* datalines; */
/* inc_wage */
/* inc_bus */
/* inc_int */
/* inc_divid */
/* inc_pnsn_natl */
/* inc_pnsn_occup */
/* inc_othr */
/* inc_tot */
/* ; */
/*  */
/* data taxbase_variables; */
/* input name $; */
/* datalines; */
/* prop_txbs_bldg */
/* prop_txbs_lnd */
/* prop_txbs_hs */
/* prop_txbs_shop */
/* prop_txbs_tot */
/* ; */
/* run; */

%macro desc_stat_cols(col);
proc sql;
    create table work.bfc_desc_stat_&col as 
    select
    	max(STD_YYYY) as std_yyyy,
        "&col" as var length=16,
        count(&col) as count,
        nmiss(&col) as nmiss,
        mean(&col) as mean,
        max(&col) as max,
        min(&col) as min,
        std(&col) as std
    from data.BFC group by STD_YYYY;
    quit;
run;
%mend desc_stat_cols;


%macro desc_stat_all;
/*inc_wage, inc_bus, inc_int, inc_divid, inc_pnsn_natl, inc_pnsn_occup, inc_othr, inc_tot
    prop_txbs_bldg, prop_txbs_lnd, prop_txbs_hs, prop_txbs_shop, prop_txbs_tot*/
%let vnames = inc_wage inc_bus inc_int inc_divid inc_pnsn_natl inc_pnsn_occup inc_othr inc_tot prop_txbs_bldg prop_txbs_lnd prop_txbs_hs prop_txbs_shop prop_txbs_tot;
%local i next_name;
%do i=1 %to %sysfunc(countw(&vnames));
   %let next_name = %scan(&vnames, &i);
   %desc_stat_cols(&next_name);
%end;
data data.bfc_desc_stat;
	set work.bfc_desc_stat_:;
run;
%mend desc_stat_all;

%desc_stat_all;
/**/
/*%desc_stat_all();*/;
/*proc sql noprint;*/
/*select count(*)*/
/*into :OBSCOUNT*/
/*from testdata;*/
/*quit;*/
/*%put ncols=&OBSCOUNT.;*/
/**/
/*proc sql;*/
/*SELECT "SELECT COUNT(" || STRIP(colname) || ") as " || STRIP(colname) || "_count"*/
/*INTO :descstat1-:descstat&cnt*/
/*FROM columns; */
/**/

/* || STRIP(name) || "' as VARNAME, SUBJID, length(strip("*/
/*|| STRIP(name) || ")) as MAX_LENGTH, "*/
/*|| STRIP(name) || " as DATAVALUE FROM &mylib.."*/
/*|| STRIP(memname) || "*/
/* count(&col) as &col._count,*/
/*nmiss(&col) as &col._nmiss,*/
/*mean(&col) as &col._mean,*/
/*max(&col) as &col._max,*/
/*min(&col) as &col._min,*/
/*std(&col) as &col._std*/

proc export data=data.bfc_desc_stat
	outfile="C:/Users/Suzin/workspace/inequality/data/processed/yearly_desc_stat.csv"
	replace;
run;
