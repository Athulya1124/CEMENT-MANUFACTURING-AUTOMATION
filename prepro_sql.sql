create database cement;

use cement;
select * from dataset;

SELECT Date_Time,COUNT(Date_Time) FROM dataset
GROUP BY Date_Time HAVING COUNT(Date_Time) > 1;    #finding duplicates, 5 duplicates

create table dataset_copy select distinct * from dataset;      #creating a new table without duplicates

drop table dataset;
alter table dataset_copy rename to dataset;       #treated duplicates  
   
create table dataset_copy select * from dataset;  

select * from dataset where DFA_TPH is NULL;
select * from dataset where Mill_VentBF_OL_Draft is NULL;
select * from dataset where Sep_Ventbagfilterfan_rpm is NULL;
select * from dataset where SepKW is NULL;                                 #locating missing values

SELECT DFA_TPH 
FROM (
    SELECT DFA_TPH, ROW_NUMBER() OVER (ORDER BY DFA_TPH) AS row_num,
           COUNT(*) OVER () AS total_count
    FROM dataset
) AS subquery
WHERE row_num = (total_count + 1) / 2 OR row_num = (total_count + 2) / 2; 

update dataset
set DFA_TPH = 58.13
where DFA_TPH is NULL;

SELECT Mill_VentBF_OL_Draft
FROM (
    SELECT Mill_VentBF_OL_Draft, ROW_NUMBER() OVER (ORDER BY Mill_VentBF_OL_Draft) AS row_num,
           COUNT(*) OVER () AS total_count
    FROM dataset
) AS subquery
WHERE row_num = (total_count + 1) / 2 OR row_num = (total_count + 2) / 2; 

update dataset
set Mill_VentBF_OL_Draft = 184.08
where Mill_VentBF_OL_Draft is NULL;

SELECT Sep_Ventbagfilterfan_rpm
FROM (
    SELECT Sep_Ventbagfilterfan_rpm, ROW_NUMBER() OVER (ORDER BY Sep_Ventbagfilterfan_rpm) AS row_num,
           COUNT(*) OVER () AS total_count
    FROM dataset
) AS subquery
WHERE row_num = (total_count + 1) / 2 OR row_num = (total_count + 2) / 2; 

update dataset
set Sep_Ventbagfilterfan_rpm = 750
where Sep_Ventbagfilterfan_rpm is NULL;

SELECT SepKW
FROM (
    SELECT SepKW, ROW_NUMBER() OVER (ORDER BY SepKW) AS row_num,
           COUNT(*) OVER () AS total_count
    FROM dataset
) AS subquery
WHERE row_num = (total_count + 1) / 2 OR row_num = (total_count + 2) / 2; 

update dataset
set SepKW = 54.99
where SepKW is NULL;
set sql_safe_updates=0;                                                            #median imputation had done

SELECT MAX(Mill_TPH) AS fifth_percentile
FROM (
  SELECT Mill_TPH, NTILE(100) OVER (ORDER BY Mill_TPH) AS ntiles
  FROM dataset
) AS ntile_data
WHERE ntiles = 5;
update dataset
 set Mill_TPH =
CASE
    WHEN Mill_TPH < 147.64  THEN 147.64
    ELSE Mill_TPH
  END;
 
 SELECT MAX(Clinker_TPH) AS fifth_percentile
FROM (
  SELECT Clinker_TPH, NTILE(100) OVER (ORDER BY Clinker_TPH) AS ntiles
  FROM dataset
) AS ntile_data
WHERE ntiles = 5;
SELECT MAX(Clinker_TPH) AS ninetyfifth_percentile
FROM (
  SELECT Clinker_TPH, NTILE(100) OVER (ORDER BY Clinker_TPH) AS ntiles
  FROM dataset
) AS ntile_data
WHERE ntiles = 95;
update dataset
 set Clinker_TPH =
CASE
    WHEN Clinker_TPH < 86.03  THEN 86.03
    ELSE Clinker_TPH
  END;

 SELECT MAX(Gypsum_TPH) AS fifth_percentile
FROM (
  SELECT Gypsum_TPH, NTILE(100) OVER (ORDER BY Gypsum_TPH) AS ntiles
  FROM dataset
) AS ntile_data
WHERE ntiles = 5;
update dataset
 set Gypsum_TPH =
CASE
    WHEN Gypsum_TPH < 3.38 THEN 3.38
    ELSE Gypsum_TPH
  END;
  
  SELECT MAX(DFA_TPH) AS fifth_percentile
FROM (
  SELECT DFA_TPH, NTILE(100) OVER (ORDER BY DFA_TPH) AS ntiles
  FROM dataset
) AS ntile_data
WHERE ntiles = 5;
update dataset
 set DFA_TPH =
CASE
    WHEN DFA_TPH < 43.75 THEN 43.75
    ELSE DFA_TPH
  END;
  

SELECT MAX(WFA_TPH) AS ninetyfifth_percentile
FROM (
  SELECT WFA_TPH, NTILE(100) OVER (ORDER BY WFA_TPH) AS ntiles
  FROM dataset
) AS ntile_data
WHERE ntiles = 95;
update dataset
 set WFA_TPH =
CASE
    WHEN WFA_TPH > 9.64 THEN 9.64
    ELSE WFA_TPH
  END;
  
 SELECT MAX(Mill_KW) AS fifth_percentile
FROM (
  SELECT Mill_KW, NTILE(100) OVER (ORDER BY Mill_KW) AS ntiles
  FROM dataset
) AS ntile_data
WHERE ntiles = 5;
update dataset
 set Mill_KW =
CASE
    WHEN Mill_KW < 3536.93  THEN 3536.93
    ELSE Mill_KW
  END;
  
 SELECT MAX(Mill_IL_Temp) AS fifth_percentile
FROM (
  SELECT Mill_IL_Temp, NTILE(100) OVER (ORDER BY Mill_IL_Temp) AS ntiles
  FROM dataset
) AS ntile_data
WHERE ntiles = 5;
SELECT MAX(Mill_IL_Temp) AS ninetyfifth_percentile
FROM (
  SELECT Mill_IL_Temp, NTILE(100) OVER (ORDER BY Mill_IL_Temp) AS ntiles
  FROM dataset
) AS ntile_data
WHERE ntiles = 95;
update dataset
 set Mill_IL_Temp =
CASE
    WHEN Mill_IL_Temp < 28.09  THEN 28.09
    WHEN Mill_IL_Temp > 41.73  THEN 41.73
    ELSE Mill_IL_Temp
  END;
  
SELECT MAX(Mill_OL_Temp) AS fifth_percentile
FROM (
  SELECT Mill_OL_Temp, NTILE(100) OVER (ORDER BY Mill_OL_Temp) AS ntiles
  FROM dataset
) AS ntile_data
WHERE ntiles = 5;
SELECT MAX(Mill_OL_Temp) AS ninetyfifth_percentile
FROM (
  SELECT Mill_OL_Temp, NTILE(100) OVER (ORDER BY Mill_OL_Temp) AS ntiles
  FROM dataset
) AS ntile_data
WHERE ntiles = 95;
update dataset
 set Mill_OL_Temp =
CASE
    WHEN Mill_OL_Temp < 82.49  THEN 82.49
    WHEN Mill_OL_Temp > 101.63  THEN 101.63
    ELSE Mill_OL_Temp
  END;
  
SELECT MAX(Mill_OL_BE_Amp) AS fifth_percentile
FROM (
  SELECT Mill_OL_BE_Amp, NTILE(100) OVER (ORDER BY Mill_OL_BE_Amp) AS ntiles
  FROM dataset
) AS ntile_data
WHERE ntiles = 5;
SELECT MAX(Mill_OL_BE_Amp) AS ninetyfifth_percentile
FROM (
  SELECT Mill_OL_BE_Amp, NTILE(100) OVER (ORDER BY Mill_OL_BE_Amp) AS ntiles
  FROM dataset
) AS ntile_data
WHERE ntiles = 95;
update dataset
 set Mill_OL_BE_Amp =
CASE
    WHEN Mill_OL_BE_Amp < 42.31  THEN 42.31
    WHEN Mill_OL_BE_Amp > 60.82  THEN 60.82
    ELSE Mill_OL_BE_Amp
  END;  
  
SELECT MAX(Mill_Vent_Fan_KW) AS fifth_percentile
FROM (
  SELECT Mill_Vent_Fan_KW, NTILE(100) OVER (ORDER BY Mill_Vent_Fan_KW) AS ntiles
  FROM dataset
) AS ntile_data
WHERE ntiles = 5;
SELECT MAX(Mill_Vent_Fan_KW) AS ninetyfifth_percentile
FROM (
  SELECT Mill_Vent_Fan_KW, NTILE(100) OVER (ORDER BY Mill_Vent_Fan_KW) AS ntiles
  FROM dataset
) AS ntile_data
WHERE ntiles = 95;
update dataset
 set Mill_Vent_Fan_KW =
CASE
    WHEN Mill_Vent_Fan_KW < 37.9  THEN 37.9
    WHEN Mill_Vent_Fan_KW > 55.53  THEN 55.53
    ELSE Mill_Vent_Fan_KW
  END;  
  
SELECT MAX(Mill_VentBF_IL_Draft) AS fifth_percentile
FROM (
  SELECT Mill_VentBF_IL_Draft, NTILE(100) OVER (ORDER BY Mill_VentBF_IL_Draft) AS ntiles
  FROM dataset
) AS ntile_data
WHERE ntiles = 5;
SELECT MAX(Mill_VentBF_IL_Draft) AS ninetyfifth_percentile
FROM (
  SELECT Mill_VentBF_IL_Draft, NTILE(100) OVER (ORDER BY Mill_VentBF_IL_Draft) AS ntiles
  FROM dataset
) AS ntile_data
WHERE ntiles = 95;
update dataset
 set Mill_VentBF_IL_Draft =
CASE
    WHEN Mill_VentBF_IL_Draft < 61.7  THEN 61.7
    WHEN Mill_VentBF_IL_Draft > 102.42  THEN 102.42
    ELSE Mill_VentBF_IL_Draft
  END;  
  
SELECT MAX(Reject) AS fifth_percentile
FROM (
  SELECT Reject, NTILE(100) OVER (ORDER BY Reject) AS ntiles
  FROM dataset
) AS ntile_data
WHERE ntiles = 5;
SELECT MAX(Reject) AS ninetyfifth_percentile
FROM (
  SELECT Reject, NTILE(100) OVER (ORDER BY Reject) AS ntiles
  FROM dataset
) AS ntile_data
WHERE ntiles = 95;
update dataset
 set Reject =
CASE
    WHEN Reject < 59.03  THEN 59.03
    WHEN Reject > 189.89  THEN 189.89
    ELSE Reject
  END;
  
SELECT MAX(SepRPM) AS fifth_percentile
FROM (
  SELECT SepRPM, NTILE(100) OVER (ORDER BY SepRPM) AS ntiles
  FROM dataset
) AS ntile_data
WHERE ntiles = 5;
SELECT MAX(SepRPM) AS ninetyfifth_percentile
FROM (
  SELECT SepRPM, NTILE(100) OVER (ORDER BY SepRPM) AS ntiles
  FROM dataset
) AS ntile_data
WHERE ntiles = 95;
update dataset
 set SepRPM =
CASE
    WHEN SepRPM < 907.29  THEN 907.29
    WHEN SepRPM > 996.55  THEN 996.55
    ELSE SepRPM
  END;

SELECT MAX(SepKW) AS fifth_percentile
FROM (
  SELECT SepKW, NTILE(100) OVER (ORDER BY SepKW) AS ntiles
  FROM dataset
) AS ntile_data
WHERE ntiles = 5;
SELECT MAX(SepKW) AS ninetyfifth_percentile
FROM (
  SELECT SepKW, NTILE(100) OVER (ORDER BY SepKW) AS ntiles
  FROM dataset
) AS ntile_data
WHERE ntiles = 95;
update dataset
 set SepKW =
CASE
    WHEN SepKW < 31.99  THEN 31.99
    WHEN SepKW > 74.16  THEN 74.16
    ELSE SepKW
  END;
  
SELECT MAX(SepAmp) AS fifth_percentile
FROM (
  SELECT SepAmp, NTILE(100) OVER (ORDER BY SepAmp) AS ntiles
  FROM dataset
) AS ntile_data
WHERE ntiles = 5;
SELECT MAX(SepAmp) AS ninetyfifth_percentile
FROM (
  SELECT SepAmp, NTILE(100) OVER (ORDER BY SepAmp) AS ntiles
  FROM dataset
) AS ntile_data
WHERE ntiles = 95;
update dataset
 set SepAmp =
CASE
    WHEN SepAmp < 127.89  THEN 127.89
    WHEN SepAmp > 159.84  THEN 159.84
    ELSE SepAmp
  END;

SELECT MAX(CAFanRPM) AS fifth_percentile
FROM (
  SELECT CAFanRPM, NTILE(100) OVER (ORDER BY CAFanRPM) AS ntiles
  FROM dataset
) AS ntile_data
WHERE ntiles = 5;
SELECT MAX(CAFanRPM) AS ninetyfifth_percentile
FROM (
  SELECT CAFanRPM, NTILE(100) OVER (ORDER BY CAFanRPM) AS ntiles
  FROM dataset
) AS ntile_data
WHERE ntiles = 95;
update dataset
 set CAFanRPM =
CASE
    WHEN CAFanRPM < 890.28 THEN 890.28
    WHEN CAFanRPM > 921.96  THEN 921.96
    ELSE CAFanRPM
  END;

SELECT MAX(CAFanKW) AS fifth_percentile
FROM (
  SELECT CAFanKW, NTILE(100) OVER (ORDER BY CAFanKW) AS ntiles
  FROM dataset
) AS ntile_data
WHERE ntiles = 5;
SELECT MAX(CAFanKW) AS ninetyfifth_percentile
FROM (
  SELECT CAFanKW, NTILE(100) OVER (ORDER BY CAFanKW) AS ntiles
  FROM dataset
) AS ntile_data
WHERE ntiles = 95;
update dataset
 set CAFanKW =
CASE
    WHEN CAFanKW < 316.55 THEN 316.55
    WHEN CAFanKW > 343.75  THEN 343.75
    ELSE CAFanKW
  END;  

SELECT MAX(Mill_IL_Draft) AS fifth_percentile
FROM (
  SELECT Mill_IL_Draft, NTILE(100) OVER (ORDER BY Mill_IL_Draft) AS ntiles
  FROM dataset
) AS ntile_data
WHERE ntiles = 5;
SELECT MAX(Mill_IL_Draft) AS ninetyfifth_percentile
FROM (
  SELECT Mill_IL_Draft, NTILE(100) OVER (ORDER BY Mill_IL_Draft) AS ntiles
  FROM dataset
) AS ntile_data
WHERE ntiles = 95;
update dataset
 set Mill_IL_Draft =
CASE
    WHEN Mill_IL_Draft < 18.6 THEN 18.6
    WHEN Mill_IL_Draft > 35.08  THEN 35.08
    ELSE Mill_IL_Draft
  END; 
  
SELECT MAX(SepVent_IL_Draft) AS fifth_percentile
FROM (
  SELECT SepVent_IL_Draft, NTILE(100) OVER (ORDER BY SepVent_IL_Draft) AS ntiles
  FROM dataset
) AS ntile_data
WHERE ntiles = 5;
SELECT MAX(SepVent_IL_Draft) AS ninetyfifth_percentile
FROM (
  SELECT SepVent_IL_Draft, NTILE(100) OVER (ORDER BY SepVent_IL_Draft) AS ntiles
  FROM dataset
) AS ntile_data
WHERE ntiles = 95;
update dataset
 set SepVent_IL_Draft =
CASE
    WHEN SepVent_IL_Draft < 132.3 THEN 132.3
    WHEN SepVent_IL_Draft > 144.52 THEN 144.52
    ELSE SepVent_IL_Draft
  END; 

SELECT MAX(SepVent_OL_Draft) AS fifth_percentile
FROM (
  SELECT SepVent_OL_Draft, NTILE(100) OVER (ORDER BY SepVent_OL_Draft) AS ntiles
  FROM dataset
) AS ntile_data
WHERE ntiles = 5;
SELECT MAX(SepVent_OL_Draft) AS ninetyfifth_percentile
FROM (
  SELECT SepVent_OL_Draft, NTILE(100) OVER (ORDER BY SepVent_OL_Draft) AS ntiles
  FROM dataset
) AS ntile_data
WHERE ntiles = 95;
update dataset
 set SepVent_OL_Draft =
CASE
    WHEN SepVent_OL_Draft < 199.26 THEN 199.26
    WHEN SepVent_OL_Draft > 224.93 THEN 224.93
    ELSE SepVent_OL_Draft
  END;
  
SELECT MAX(Sep_Ventbagfilterfan_kw) AS fifth_percentile
FROM (
  SELECT Sep_Ventbagfilterfan_kw, NTILE(100) OVER (ORDER BY Sep_Ventbagfilterfan_kw) AS ntiles
  FROM dataset
) AS ntile_data
WHERE ntiles = 5;
SELECT MAX(Sep_Ventbagfilterfan_kw) AS ninetyfifth_percentile
FROM (
  SELECT Sep_Ventbagfilterfan_kw, NTILE(100) OVER (ORDER BY Sep_Ventbagfilterfan_kw) AS ntiles
  FROM dataset
) AS ntile_data
WHERE ntiles = 95;
update dataset
 set Sep_Ventbagfilterfan_kw =
CASE
    WHEN Sep_Ventbagfilterfan_kw < 14.2 THEN 14.2
    WHEN Sep_Ventbagfilterfan_kw > 28.6 THEN 28.6
    ELSE Sep_Ventbagfilterfan_kw
  END;
  
SELECT MAX(Sep_Ventbagfilterfan_rpm) AS fifth_percentile
FROM (
  SELECT Sep_Ventbagfilterfan_rpm, NTILE(100) OVER (ORDER BY Sep_Ventbagfilterfan_rpm) AS ntiles
  FROM dataset
) AS ntile_data
WHERE ntiles = 5;
SELECT MAX(Sep_Ventbagfilterfan_rpm) AS ninetyfifth_percentile
FROM (
  SELECT Sep_Ventbagfilterfan_rpm, NTILE(100) OVER (ORDER BY Sep_Ventbagfilterfan_rpm) AS ntiles
  FROM dataset
) AS ntile_data
WHERE ntiles = 95;
update dataset
 set Sep_Ventbagfilterfan_rpm =
CASE
    WHEN Sep_Ventbagfilterfan_rpm < 745.47 THEN 745.47
    WHEN Sep_Ventbagfilterfan_rpm > 785.33 THEN 785.33
    ELSE Sep_Ventbagfilterfan_rpm
  END;
  
SELECT MAX(Residue) AS fifth_percentile
FROM (
  SELECT Residue, NTILE(100) OVER (ORDER BY Residue) AS ntiles
  FROM dataset
) AS ntile_data
WHERE ntiles = 5;
SELECT MAX(Residue) AS ninetyfifth_percentile
FROM (
  SELECT Residue, NTILE(100) OVER (ORDER BY Residue) AS ntiles
  FROM dataset
) AS ntile_data
WHERE ntiles = 95;
update dataset
 set Residue =
CASE
    WHEN Residue < 13.25 THEN 13.25
    WHEN Residue > 17.4 THEN 17.4
    ELSE Residue
  END;                                                  #treated outliers
  
  SELECT Mill_TPH 
FROM (
    SELECT Mill_TPH, ROW_NUMBER() OVER (ORDER BY Mill_TPH) AS row_num,
           COUNT(*) OVER () AS total_count
    FROM dataset
) AS subquery
WHERE row_num = (total_count + 1) / 2 OR row_num = (total_count + 2) / 2;  
  





    

