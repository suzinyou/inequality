options symbolgen nosqlremerge;

libname SRC '/userdata07/room285/data_source/user_data';  /* other data: g1eq, estate_link, etc*/
libname SRCPANEL '/userdata07/room285/data_source/user_data/201111ADD';  /*seoul and kr sample panel*/
libname SRCXSECT '/userdata07/room285/data_source/user_data/201116ADD';  /*kr sample cross-section*/
libname STORE '/userdata07/room285/data_out/data_store';

%macro generate_features(region);
/* Generate composite income variables, parsed sido/sigungu/dong variables, */
/* and age variable. */
/* TAKES ~20 minutes per run !!!!!!!!!!!!!!!!!!!!!!!! */
%if &region=seoul or &region=seoulpanel %then %do;
	/* Compute sigungu and dong */
	proc sql;
		alter table STORE.&region
	    	add inc_fin num
				, inc_pnsn num
				, inc_main num
				, sido char(2)
				, sigungu char(3)
				, dong char(3)
				, firm_sido char(2)
				, firm_sigungu char(3)
				, firm_dong char(3)
				, age num;
			update STORE.&region
			set inc_fin=inc_int + inc_divid
				, inc_pnsn=inc_pnsn_natl + inc_pnsn_occup
				, inc_main=inc_wage + inc_bus + inc_othr + inc_int + inc_divid
				, sido=substr(RVSN_ADDR_CD, 1, 2)
				, sigungu= substr(RVSN_ADDR_CD, 3, 3)
				, dong= substr(RVSN_ADDR_CD, 6, 3)
		        , firm_sido= (case when RVSN_FIRM_ADDR_CD is null then '' else substr(RVSN_FIRM_ADDR_CD, 1, 2) end)
				, firm_sigungu= (case when RVSN_FIRM_ADDR_CD is null then '' else substr(RVSN_FIRM_ADDR_CD, 3, 3) end)
		    	, firm_dong= (case when RVSN_FIRM_ADDR_CD is null then '' else substr(RVSN_FIRM_ADDR_CD, 6, 3) end)
				, age= input(STD_YYYY,4.) - input(BYEAR,4.);
	    quit;
	run;
%end;
%else %do;
	proc sql;
		alter table STORE.&region
	    	add inc_fin num
				, inc_pnsn num
				, inc_main num
				, sido char(2)
				, firm_sido char(2)
				, age num;
			update STORE.&region
			set inc_fin=inc_int + inc_divid
				, inc_pnsn=inc_pnsn_natl + inc_pnsn_occup
				, inc_main=inc_wage + inc_bus + inc_othr + inc_int + inc_divid
				, sido=substr(RVSN_ADDR_CD, 1, 2)
		        , firm_sido= (case when RVSN_FIRM_ADDR_CD is null then '' else substr(RVSN_FIRM_ADDR_CD, 1, 2) end)
				, age= input(STD_YYYY,4.) - input(BYEAR,4.);
	    quit;
	run;
%end;
%mend generate_features;

%macro union_krpanel;
/* Union KR sample panel data (across all years) */
%local savename year;
%let savename=krpanel;
%do year=2002 %to 2019;
	%if &year=2002 %then %do;
   		data store.&savename;
		set srcpanel.bfc_&year._smpl_panel;
		run;
	%end;
	%else %do;
		proc append base=store.&savename data=srcpanel.bfc_&year._smpl_panel;
		run;
	%end;
%end;
%mend union_krpanel;

%macro union_kr;
%local savename year;
%let savename=kr;
%do year=2003 %to 2018;
	%if &year=2003 %then %do;
   		data store.&savename;
		set srcxsect.bfc_&year._smpl_xsection;
		run;
	%end;
	%else %if &year=2006 or &year=2010 or &year=2014 or &year=2018 %then %do;
		proc append base=store.&savename data=srcxsect.bfc_&year._smpl_xsection;
		run;
	%end;
%end;
%mend union_kr;

%macro union_seoulpanel;
%local savename year;
%let savename=seoulpanel;
%do year=2002 %to 2019;
	%if &year=2002 %then %do;
   		data store.&savename;
		set srcpanel.bfc_&year;
		run;
	%end;
	%else %do;
		proc append base=store.&savename data=srcpanel.bfc_&year;
		run;
	%end;
%end;
%mend union_seoulpanel;

%macro fillna(region);
/* Fill null values with zero in income and property tax base variables */
/* TAKES OVER AN HOUR for a single run!!!!! */
%let vnames = inc_tot inc_wage inc_bus inc_othr inc_int inc_divid inc_fin inc_pnsn_natl inc_pnsn_occup inc_pnsn inc_main prop_txbs_tot prop_txbs_hs prop_txbs_lnd prop_txbs_bldg prop_txbs_ship;
%do i=1 %to %sysfunc(countw(&vnames));
	%let vname = %scan(&vnames, &i);
	proc sql;
	update store.&region
	set &vname=(case when &vname=. then 0 else &vname end);
	quit;
%end;
%mend;

/* Korea sample (cross-section) ---------------------------------------------*/
%union_kr;
%generate_features(kr);
%fillna(kr);

/* Korea sample (panel) -----------------------------------------------------*/
%union_krpanel;
%generate_features(krpanel);
%fillna(krpanel);

/* Seoul (panel) ------------------------------------------------------------*/
%union_seoulpanel;
%generate_features(seoulpanel);
%fillna(seoulpanel);

/* Seoul (seoul only!) ------------------------------------------------------*/
proc sql;
	create table STORE.SEOUL as
	select * 
	from STORE.SEOULPANEL
	where sido = "11";
quit;


/* Seoul 1% sample ---------------------------------------------*/
proc surveyselect data=STORE.SEOUL
	method=SRS
	seed=170011
	rate=0.01
	out=store.SEOUL_SMPL;
STRATA STD_YYYY;
run;

/* TODO: sample based on household heads! */

/* Seoul household 1% sample -----------------------------------*/
proc surveyselect data=STORE.SEOUL_HH
	method=SRS
	seed=170011
	rate=0.01
	out=work.SEOUL_HH_SMPL;
STRATA STD_YYYY;
run;

