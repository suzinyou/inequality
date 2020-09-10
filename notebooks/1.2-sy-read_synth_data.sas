%let mydir=C:/Users/Suzin/workspace/inequality/data/interim/synthetic;

data files;
	do year=2003 to 2018;
	output;
	end;
run;


data datasets;
  fileno+1;  				/* ??? */
  set files;
  call execute(catx(' '
       ,'proc import datafile='
       ,quote(catx('/','&mydir', cats(year, '.csv')))
       ,'out=', cats('SY.csv', year),'replace'
       ,'dbms=csv'
       ,';run;'
  ));
run;