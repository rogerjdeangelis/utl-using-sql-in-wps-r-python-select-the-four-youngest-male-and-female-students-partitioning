%let pgm=utl-using-sql-in-wps-r-python-select-the-four-youngest-male-and-female-students=partitioning;

Problem
  Given a sorted list of students by sex and age, select the first 4 youngest females and males

Obviously this is simpler with a wps datastep

  SOLUTIONS

      1 wps datastep
      2 wps sql
      3 wps r sql
      4 wps python sql
      5 wps r no sql

github
https://tinyurl.com/3jj7xk2h
https://github.com/rogerjdeangelis/utl-using-sql-in-wps-r-python-select-the-four-youngest-male-and-female-students-partitioning

/*                   _
(_)_ __  _ __  _   _| |_
| | `_ \| `_ \| | | | __|
| | | | | |_) | |_| | |_
|_|_| |_| .__/ \__,_|\__|
        |_|
*/

options validvarname=upcase;
libname sd1 "d:/sd1";
data sd1.have;
  set sashelp.class(obs=11 drop=height weight );
run;quit;

proc sort data=sd1.have out=sd1.have;;
by sex age name;
run;quit;

 /**************************************************************************************************************************/
 /*                            |              |                                                                            */
 /* Sorted for documentation   |              |                                                                            */
 /* Solution does not require  |              |                                                                            */
 /* sorted data,               |              |                                                                            */
 /*                            |              |                                                                            */
 /*  SortNotNeeded  obs=11     |    PROCESS   |    OUTPUT                                                                  */
 /*                            |              |                                                                            */
 /*  NAME       SEX    AGE     |              |    PARTITION    NAME       SEX    AGE                                      */
 /*                            |              |                                                                            */
 /*  Joyce       F      11     |              |        1        Joyce       F      11                                      */
 /*  Jane        F      12     |              |        2        Jane        F      12                                      */
 /*  Alice       F      13     |              |        3        Alice       F      13                                      */
 /*  Barbara     F      13     |              |        4        Barbara     F      13                                      */
 /*  Carol       F      14     |    * remove  |                                                                            */
 /*  Janet       F      15     |    * remove  |        1        James       M      12                                      */
 /*  James       M      12     |              |        2        John        M      12                                      */
 /*  John        M      12     |              |        3        Jeffrey     M      13                                      */
 /*  Jeffrey     M      13     |              |        4        Alfred      M      14                                      */
 /*  Alfred      M      14     |              |                                                                            */
 /*  Henry       M      14     |    * remove  |                                                                            */
 /*                            |              |                                                                            */
 /**************************************************************************************************************************/

/*                            _       _            _
/ | __      ___ __  ___    __| | __ _| |_ __ _ ___| |_ ___ _ __
| | \ \ /\ / / `_ \/ __|  / _` |/ _` | __/ _` / __| __/ _ \ `_ \
| |  \ V  V /| |_) \__ \ | (_| | (_| | || (_| \__ \ ||  __/ |_) |
|_|   \_/\_/ | .__/|___/  \__,_|\__,_|\__\__,_|___/\__\___| .__/
             |_|                                          |_|
*/

proc datasets lib=sd1 nolist nodetails;delete want; run;quit;

%utl_submit_wps64x('
  libname sd1 "d:/sd1";
  data sd1.want;
    retain partition 0;
    set sd1.have;
    by sex;
    partition=ifn(first.sex,1,partition+1);
    if partition le 4;
run;quit;
');

/**************************************************************************************************************************/
/*                                                                                                                        */
/*  PARTITION    NAME       SEX    AGE                                                                                    */
/*                                                                                                                        */
/*      1        Joyce       F      11                                                                                    */
/*      2        Jane        F      12                                                                                    */
/*      3        Alice       F      13                                                                                    */
/*      4        Barbara     F      13                                                                                    */
/*      1        James       M      12                                                                                    */
/*      2        John        M      12                                                                                    */
/*      3        Jeffrey     M      13                                                                                    */
/*      4        Alfred      M      14                                                                                    */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*___                                   _
|___ \  __      ___ __  ___   ___  __ _| |
  __) | \ \ /\ / / `_ \/ __| / __|/ _` | |
 / __/   \ V  V /| |_) \__ \ \__ \ (_| | |
|_____|   \_/\_/ | .__/|___/ |___/\__, |_|
                 |_|                 |_|
*/

proc datasets lib=sd1 nolist nodetails;delete want; run;quit;

%utl_submit_wps64x('
libname sd1 "d:/sd1";
options validvarname=any;
proc sql;
  create
    table sd1.want as
  select
    name
    ,sex
    ,age
    ,row_number - min(row_number)+1 as partition
 from
     (select *, monotonic() as row_number from
      (select *, max(sex) as delete from sd1.have group by sex ))
  group
    by sex
  having
    (row_number - min(row_number)+1) <= 4
;quit;
proc print data=sd1.want;
run;quit;
');

/*----                                                                   ----*/
/*---- FYI - YOU CANNOT NEST ORDER BY                                    ----*/
/*----                                                                   ----*/

proc sql;
   select
      *
   from
      (select * from sd1.have order by sex, age)
;quit;

4926!       (select * from sd1.have order by sex, age)
                                                     79
ERROR 79-322: Expecting a (.                         -


/**************************************************************************************************************************/
/*                                                                                                                        */
/* The WPS System                                                                                                         */
/*                                                                                                                        */
/* Obs     NAME      SEX    AGE    partition                                                                              */
/*                                                                                                                        */
/*  1     Joyce       F      11        1                                                                                  */
/*  2     Jane        F      12        2                                                                                  */
/*  3     Alice       F      13        3                                                                                  */
/*  4     Barbara     F      13        4                                                                                  */
/*  5     James       M      12        1                                                                                  */
/*  6     John        M      12        2                                                                                  */
/*  7     Jeffrey     M      13        3                                                                                  */
/*  8     Alfred      M      14        4                                                                                  */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*____                                         _
|___ /  __      ___ __  ___   _ __   ___  __ _| |
  |_ \  \ \ /\ / / `_ \/ __| | `__| / __|/ _` | |
 ___) |  \ V  V /| |_) \__ \ | |    \__ \ (_| | |
|____/    \_/\_/ | .__/|___/ |_|    |___/\__, |_|
                 |_|                        |_|
*/

proc datasets lib=sd1 nolist nodetails;delete want; run;quit;

%utl_submit_wps64x("
options validvarname=any;
libname sd1 'd:/sd1';
proc r;
export data=sd1.have r=have;
submit;
library(sqldf);
want<-sqldf('
   select
      *
     ,partition
   from
      (select *, row_number() over (partition by sex) as partition from have )
   where
      partition <= 4
   order
     by sex
');
want;
endsubmit;
run;quit;
");

/**************************************************************************************************************************/
/*                                                                                                                        */
/* The WPS System                                                                                                         */
/*                                                                                                                        */
/*      NAME SEX AGE partition partition                                                                                  */
/* 1   Joyce   F  11         1         1                                                                                  */
/* 2    Jane   F  12         2         2                                                                                  */
/* 3   Alice   F  13         3         3                                                                                  */
/* 4 Barbara   F  13         4         4                                                                                  */
/* 5   James   M  12         1         1                                                                                  */
/* 6    John   M  12         2         2                                                                                  */
/* 7 Jeffrey   M  13         3         3                                                                                  */
/* 8  Alfred   M  14         4         4                                                                                  */
/*                                                                                                                        */
/**************************************************************************************************************************/
/*  _                                     _   _                             _
| || |  __      ___ __  ___   _ __  _   _| |_| |__   ___  _ __    ___  __ _| |
| || |_ \ \ /\ / / `_ \/ __| | `_ \| | | | __| `_ \ / _ \| `_ \  / __|/ _` | |
|__   _| \ V  V /| |_) \__ \ | |_) | |_| | |_| | | | (_) | | | | \__ \ (_| | |
   |_|    \_/\_/ | .__/|___/ | .__/ \__, |\__|_| |_|\___/|_| |_| |___/\__, |_|
                 |_|         |_|    |___/                                |_|
*/
proc datasets lib=sd1 nolist nodetails;delete want; run;quit;

%utl_submit_wps64x("
libname sd1 'd:/sd1';
proc python;
export data=sd1.have python=have;
submit;
from os import path;
import pandas as pd;
import numpy as np;
import pandas as pd;
from pandasql import sqldf;
mysql = lambda q: sqldf(q, globals());
from pandasql import PandaSQL;
pdsql = PandaSQL(persist=True);
sqlite3conn = next(pdsql.conn.gen).connection.connection;
sqlite3conn.enable_load_extension(True);
sqlite3conn.load_extension('c:/temp/libsqlitefunctions.dll');
mysql = lambda q: sqldf(q, globals());
want = pdsql('''
   select
      *
     ,partition
   from
      (select *, row_number() over (partition by sex) as partition from have )
   where
      partition <= 4
   order
     by sex
''');
print(want);
endsubmit;
run;quit;
"));

/**************************************************************************************************************************/
/*                                                                                                                        */
/* The WPS System                                                                                                         */
/*                                                                                                                        */
/* The PYTHON Procedure                                                                                                   */
/*                                                                                                                        */
/*        NAME SEX   AGE  partition                                                                                       */
/* 0  Joyce      F  11.0          1                                                                                       */
/* 1  Jane       F  12.0          2                                                                                       */
/* 2  Alice      F  13.0          3                                                                                       */
/* 3  Barbara    F  13.0          4                                                                                       */
/* 4  James      M  12.0          1                                                                                       */
/* 5  John       M  12.0          2                                                                                       */
/* 6  Jeffrey    M  13.0          3                                                                                       */
/* 7  Alfred     M  14.0          4                                                                                       */
/*                                                                                                                        */
/**************************************************************************************************************************/


/*___                                                        _
| ___|  __      ___ __  ___   _ __   _ __   ___    ___  __ _| |
|___ \  \ \ /\ / / `_ \/ __| | `__| | `_ \ / _ \  / __|/ _` | |
 ___) |  \ V  V /| |_) \__ \ | |    | | | | (_) | \__ \ (_| | |
|____/    \_/\_/ | .__/|___/ |_|    |_| |_|\___/  |___/\__, |_|
                 |_|                                      |_|
*/

proc datasets lib=sd1 nolist nodetails;delete want; run;quit;

%utl_submit_wps64x('
libname sd1 "d:/sd1";
proc r;
export data=sd1.have r=have;
submit;
library(dplyr);
library(tidyr);
df <- have %>%
  group_by(SEX) %>%
  mutate(SEX_INSTANCE = row_number()) %>%
  ungroup();
df$SEX_INSTANCE <- ifelse(df$SEX_INSTANCE <= 4, df$SEX_INSTANCE, NA);
df %>% drop_na(SEX_INSTANCE) ;
endsubmit;
import data=sd1.want r=df;
run;quit;
');

/**************************************************************************************************************************/
/*                                                                                                                        */
/* The WPS System                                                                                                         */
/*                                                                                                                        */
/* # A tibble: 8 x 4                                                                                                      */
/*   NAME    SEX     AGE SEX_INSTANCE                                                                                     */
/*   <chr>   <chr> <dbl>        <int>                                                                                     */
/* 1 Joyce   F        11            1                                                                                     */
/* 2 Jane    F        12            2                                                                                     */
/* 3 Alice   F        13            3                                                                                     */
/* 4 Barbara F        13            4                                                                                     */
/* 5 James   M        12            1                                                                                     */
/* 6 John    M        12            2                                                                                     */
/* 7 Jeffrey M        13            3                                                                                     */
/* 8 Alfred  M        14            4                                                                                     */
/*                                                                                                                        */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*              _
  ___ _ __   __| |
 / _ \ `_ \ / _` |
|  __/ | | | (_| |
 \___|_| |_|\__,_|

*/
