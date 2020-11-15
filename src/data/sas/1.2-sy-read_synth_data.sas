%let mydir=C:/Users/Suzin/workspace/inequality/data/interim/synthetic;
libname DATA 'C:/Users/Suzin/workspace/inequality/data/saslib';

data files;
	do year=2003 to 2017;
	output;
	end;
run;


data datasets;
  fileno+1;  				/* ??? */
  set files;
  call execute(catx(' '
       ,'proc import datafile='
       ,quote(catx('/','&mydir', cats('BFC_', year, '.csv')))
       ,'out=', cats('DATA.BFC_', year),'replace'
       ,'dbms=csv'
       ,';run;'
  ));
run;


/* reference code:

data DSN;
	%let _EFIERR_=0;
	infile "&inputpath.\&filename..csv" delimiter=',' MISSOVER DSD lrecl=32767 firstobs=2;
	informat &infmtstatement.;
	format &fmtstatement.;
	input &inputstatement.;
	label &labelstatement.;
	if _ERROR_ then call symputx('_EFIERR_',1);
run;

 call execute(catx(' '
	,'%let _EFIERR_=0;'  	
   ,'data',cats('DATA.BFC_', year,';')
   ,'infile',quote(catx('/','&mydir', cats('BFC_', year, '.csv')))
   ,'delimiter=',quote(',')
   ,'MISSOVER','DSD'
   ,'lrecl=32767','firstobs=2;'
	,'informat STD_YYYY ' incomplete informat!!!
   ,';run;'
  ));

*/

 