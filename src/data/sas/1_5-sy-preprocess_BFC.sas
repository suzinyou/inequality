options symbolgen nosqlremerge;

libname DATA '/userdata07/room285/data_source/user_data';
libname OUT '/userdata07/room285/data_out/data_out';
libname STORE '/userdata07/room285/data_out/data_store';

/* CHANGE NAME OF DATASET */
%macro change_dataset_name;
%do year = 2002 %to 2018;
	proc datasets lib=DATA;
	CHANGE BFC_&year. = BFC_SEOUL_&year.;
	
	proc datasets lib=DATA;
	CHANGE BFC_&year._SMPL = BFC_SMPL_&year.;
%end;
%mend change_dataset_name;

%macro generate_features(region, year);
proc sql;
	create table STORE.bfc_&region._&year as
    	select *, 
	    	substr(RVSN_ADDR_CD, 1, 2) as sido, 
	    	substr(RVSN_ADDR_CD, 3, 3) as sigungu, 
	    	substr(RVSN_ADDR_CD, 6, 3) as dong,
	        (case when RVSN_FIRM_ADDR_CD is null then '' else substr(RVSN_FIRM_ADDR_CD, 1, 2) end) as firm_sido, 
	        (case when RVSN_FIRM_ADDR_CD is null then '' else substr(RVSN_FIRM_ADDR_CD, 3, 3) end) as firm_sigungu, 
	    	(case when RVSN_FIRM_ADDR_CD is null then '' else substr(RVSN_FIRM_ADDR_CD, 6, 3) end) as firm_dong,
			input(STD_YYYY,4.) - input(BYEAR,4.) as age
	from DATA.BFC_&region._&year;
    quit;
run;
%mend generate_features;

/* UNION BFC's: using proc append to save memory/time */
%macro union_bfc(region);
%local year;
%do year=2002 %to 2018;
   %if &year=2002 %then %do;
   		data work.bfc_&region;
			set data.bfc_&region._&year;
	%end;
	%else %do;
		proc append base=work.bfc_&region data=data.bfc_&region._&year;
	%end;
%end;
%mend union_bfc;

/*%change_dataset_name;*/

%macro generate_features_all_years(region);
%local year;
%do year=2002 %to 2018;
   %generate_features(&region, &year);
%end;
%mend generate_features_all_years;

%generate_features_all_years(seoul);
%generate_features_all_years(smpl);

/* for each year, we need to filter people based on their region..*/
%macro filter_regions;
%do year=2002 %to 2018;
	proc sql;
		create table STORE.SEOUL_&year as
		select * 
		from STORE.BFC_SEOUL_&year
		where sido = "11";
	quit;
	proc sql;
		create table STORE.REST_&year as
		select * 
		from STORE.BFC_REST_&year
		where sido != "11";
	quit;
%end;
%mend filter_regions;

%filter_regions;

%macro sample_seoul;
%local year;
%do year=2002 %to 2018;
	/*1. Get HH_HEAD ID's*/
	proc sql;
		create table HH_HEAD_ALL_&year as
		select DISTINCT(HHRR_HEAD_INDI_DSCM_NO)
		from STORE.SEOUL_&year;
	quit;
	/*2. Get number of HH_HEAD's*/
	%local count;
	proc sql;
		select count(*) into &count from STORE.SEOUL_&year.;
	quit;
	/*3. Compute 5% of the number from 2.*/
	%let sample_count=input(0.05 * &count, 16.);
	/*TODO: Find out how to sample!!*/
%end;
%mend sample_seoul;

/*%union_bfc(seoul);*/
/*%union_bfc(smpl);*/


/* Compute features */
/* 
	[O] 주민등록주소 시도/시군구/동읍면 분리 
	[O] 사업장주소 시도/시군구/동읍면 분리
	[O] 나이 계산
	[  ] 부부 parse
    [  ] 부모-자식 parse
    [  ] 직장가입자가 있는 세대 vs 의료수급자가 있는 세대 vs 둘다 없고 지역가입자 있는 세대 vs 다 없는 세대
*/
/*proc sql;*/
/*	create table store.bfc_seoul as*/
/*    	select *, */
/*	    	substr(RVSN_ADDR_CD, 1, 2) as sido, */
/*	    	substr(RVSN_ADDR_CD, 3, 3) as sigungu, */
/*	    	substr(RVSN_ADDR_CD, 6, 3) as dong,*/
/*	        (case when RVSN_FIRM_ADDR_CD is null then '' else substr(RVSN_FIRM_ADDR_CD, 1, 2) end) as firm_sido, */
/*	        (case when RVSN_FIRM_ADDR_CD is null then '' else substr(RVSN_FIRM_ADDR_CD, 3, 3) end) as firm_sigungu, */
/*	    	(case when RVSN_FIRM_ADDR_CD is null then '' else substr(RVSN_FIRM_ADDR_CD, 6, 3) end) as firm_dong,*/
/*			input(STD_YYYY,4.) - input(BYEAR,4.) as age*/
/*	from work.bfc_seoul;*/
/*    quit;*/
/*run;*/
/**/
/*proc sql;*/
/*	create table store.bfc_smpl as*/
/*    	select *, */
/*	    	substr(RVSN_ADDR_CD, 1, 2) as sido, */
/*	    	substr(RVSN_ADDR_CD, 3, 3) as sigungu, */
/*	    	substr(RVSN_ADDR_CD, 6, 3) as dong,*/
/*	        (case when RVSN_FIRM_ADDR_CD is null then '' else substr(RVSN_FIRM_ADDR_CD, 1, 2) end) as firm_sido, */
/*	        (case when RVSN_FIRM_ADDR_CD is null then '' else substr(RVSN_FIRM_ADDR_CD, 3, 3) end) as firm_sigungu, */
/*	    	(case when RVSN_FIRM_ADDR_CD is null then '' else substr(RVSN_FIRM_ADDR_CD, 6, 3) end) as firm_dong,*/
/*			input(STD_YYYY,4.) - input(BYEAR,4.) as age*/
/*	from work.bfc_smpl;*/
/*    quit;*/
/*run;*/
