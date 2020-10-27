options symbolgen nosqlremerge;

data years;
	do year=2003 to 2017;
	output;
	end;
run;

/* CHANGE NAME OF DATASET */
%macro change_dataset_name;
%do year = 2003 %to 2017;
	proc datasets lib=DATA;
	CHANGE BFC_&year. = BFC_SEOUL_&year.;
	
	%if %sysfunc(exist(BFC_&year._SMPL)) %then
		%do;
			proc datasets lib=DATA;
			CHANGE BFC_&year._SMPL = BFC_SMPL_&year.;
		%end;
%end;
%mend change_dataset_name;

%change_dataset_name;


DATA data.bfc;
set data.BFC_SEOUL_:;
run;

proc sql;
	create table data.bfc as
    	select *, 
	    	substr(put(RVSN_ADDR_CD,z8.), 1, 2) as sido, 
	    	substr(put(RVSN_ADDR_CD,z8.), 3, 3) as sigungu, 
	    	substr(put(RVSN_ADDR_CD,z8.), 6, 3) as dong,
	        (case when RVSN_FIRM_ADDR_CD is null then '' else substr(put(RVSN_FIRM_ADDR_CD,z8.), 1, 2) end) as firm_sido, 
	        (case when RVSN_FIRM_ADDR_CD is null then '' else substr(put(RVSN_FIRM_ADDR_CD,z8.), 3, 3) end) as firm_sigungu, 
	    	(case when RVSN_FIRM_ADDR_CD is null then '' else substr(put(RVSN_FIRM_ADDR_CD,z8.), 6, 3) end) as firm_dong
	from data.bfc;
    quit;
run;
