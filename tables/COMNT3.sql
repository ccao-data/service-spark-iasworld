CREATE TABLE `iasworld.comnt3`(
  `jur` varchar(6),
  `parid` varchar(30),
  `comntno` decimal(10,2),
  `fileyr` decimal(4,0),
  `linkid` decimal(10,2),
  `code` varchar(4),
  `comnt` varchar(80),
  `who` varchar(50),
  `wen` string,
  `status` varchar(1),
  `iasw_id` decimal(10,0),
  `trans_id` decimal(10,0),
  `upd_status` varchar(1),
  `bldgno` varchar(10),
  `unit` varchar(10),
  `comnt2` string,
  `loaded_at` string)
PARTITIONED BY (`taxyr` string)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');
