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

/* UNION BFC's: using proc append to save memory/time */
%macro union_bfc(name);
%local year;
%do year=2002 %to 2018;
   %if &year=2002 %then %do;
   		data work.bfc_&name;
			set data.bfc_&name._&year;
	%end;
	%else %do;
		proc append base=work.bfc_&name data=data.bfc_&name._&year;
	%end;
%end;
%mend union_bfc;

%change_dataset_name;
%union_bfc(seoul);
%union_bfc(smpl);

/* Compute features */
/* 
	[O] �ֹε���ּ� �õ�/�ñ���/������ �и� 
	[O] ������ּ� �õ�/�ñ���/������ �и�
	[O] ���� ���
	[  ] �κ� parse
    [  ] �θ�-�ڽ� parse
    [  ] ���尡���ڰ� �ִ� ���� vs �Ƿ�����ڰ� �ִ� ���� vs �Ѵ� ���� ���������� �ִ� ���� vs �� ���� ����
*/
proc sql;
	create table store.bfc as
    	select *, 
	    	substr(RVSN_ADDR_CD, 1, 2) as sido, 
	    	substr(RVSN_ADDR_CD, 3, 3) as sigungu, 
	    	substr(RVSN_ADDR_CD, 6, 3) as dong,
	        (case when RVSN_FIRM_ADDR_CD is null then '' else substr(RVSN_FIRM_ADDR_CD, 1, 2) end) as firm_sido, 
	        (case when RVSN_FIRM_ADDR_CD is null then '' else substr(RVSN_FIRM_ADDR_CD, 3, 3) end) as firm_sigungu, 
	    	(case when RVSN_FIRM_ADDR_CD is null then '' else substr(RVSN_FIRM_ADDR_CD, 6, 3) end) as firm_dong,
			input(STD_YYYY,4.) - input(BYEAR,4.) as age
	from work.bfc_seoul;
    quit;
run;

proc sql;
	create table store.bfc_smpl as
    	select *, 
	    	substr(RVSN_ADDR_CD, 1, 2) as sido, 
	    	substr(RVSN_ADDR_CD, 3, 3) as sigungu, 
	    	substr(RVSN_ADDR_CD, 6, 3) as dong,
	        (case when RVSN_FIRM_ADDR_CD is null then '' else substr(RVSN_FIRM_ADDR_CD, 1, 2) end) as firm_sido, 
	        (case when RVSN_FIRM_ADDR_CD is null then '' else substr(RVSN_FIRM_ADDR_CD, 3, 3) end) as firm_sigungu, 
	    	(case when RVSN_FIRM_ADDR_CD is null then '' else substr(RVSN_FIRM_ADDR_CD, 6, 3) end) as firm_dong,
			input(STD_YYYY,4.) - input(BYEAR,4.) as age
	from work.bfc_smpl;
    quit;
run;
