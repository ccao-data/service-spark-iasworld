CREATE TABLE `iasworld.pardat`(
  `jur` varchar(6),
  `parid` varchar(30),
  `seq` decimal(3,0),
  `cur` varchar(1),
  `who` varchar(50),
  `wen` string,
  `whocalc` varchar(10),
  `wencalc` string,
  `status` varchar(1),
  `alt_id` varchar(30),
  `muni` varchar(4),
  `block` varchar(4),
  `unit` varchar(3),
  `mappre` varchar(6),
  `mapsuf` varchar(2),
  `rtepre` varchar(3),
  `rtesuf` varchar(2),
  `adrpre` varchar(10),
  `adrno` decimal(10,0),
  `adradd` varchar(10),
  `adrdir` varchar(2),
  `adrstr` varchar(50),
  `adrsuf` varchar(8),
  `adrsuf2` varchar(8),
  `cityname` varchar(50),
  `statecode` varchar(2),
  `unitdesc` varchar(20),
  `unitno` varchar(20),
  `loc2` varchar(40),
  `zip1` varchar(5),
  `zip2` varchar(4),
  `nbhd` varchar(8),
  `spot` decimal(3,0),
  `juris` varchar(5),
  `class` varchar(4),
  `luc` varchar(4),
  `lucmult` varchar(1),
  `livunit` decimal(5,0),
  `tieback` varchar(30),
  `tiebkcd` varchar(2),
  `tielandpct` decimal(10,7),
  `tiebldgpct` decimal(10,7),
  `landisc` varchar(10),
  `acres` decimal(12,4),
  `calcacres` decimal(12,4),
  `location` varchar(2),
  `fronting` varchar(1),
  `street1` varchar(2),
  `street2` varchar(2),
  `traffic` varchar(2),
  `topo1` varchar(1),
  `topo2` varchar(1),
  `topo3` varchar(1),
  `util1` varchar(2),
  `util2` varchar(2),
  `util3` varchar(2),
  `parkprox` varchar(1),
  `parkquanit` varchar(1),
  `parktype` varchar(1),
  `restrict1` varchar(1),
  `restrict2` varchar(1),
  `restrict3` varchar(1),
  `zoning` varchar(8),
  `note1` varchar(40),
  `note2` varchar(40),
  `note3` varchar(40),
  `note4` varchar(40),
  `notecd1` varchar(2),
  `notecd2` varchar(2),
  `ofcard` decimal(4,0),
  `partial` varchar(1),
  `bldgros_d` varchar(21),
  `bldgros_v` decimal(10,0),
  `mscbld_n` decimal(2,0),
  `prefactmscbld` decimal(10,0),
  `adjfact` decimal(7,5),
  `mscbld_v` decimal(10,0),
  `rectype` varchar(1),
  `fldref` varchar(10),
  `pctown` decimal(7,4),
  `farminc` decimal(10,0),
  `nonfarminc` decimal(10,0),
  `aguse` varchar(1),
  `nrinc` decimal(10,0),
  `chgrsn` varchar(2),
  `zfar` decimal(5,3),
  `pfar` decimal(5,3),
  `afar` decimal(5,3),
  `pfarsf` decimal(10,0),
  `afarsf` decimal(10,0),
  `user1` varchar(20),
  `user2` varchar(20),
  `user3` varchar(20),
  `user4` varchar(20),
  `user5` varchar(20),
  `user6` varchar(20),
  `user7` varchar(20),
  `user8` varchar(20),
  `user9` varchar(20),
  `user10` varchar(20),
  `user11` varchar(20),
  `user12` varchar(20),
  `user13` varchar(20),
  `user14` varchar(20),
  `user15` varchar(20),
  `user16` varchar(20),
  `user17` varchar(20),
  `user18` varchar(20),
  `user19` varchar(20),
  `user20` varchar(20),
  `user21` varchar(20),
  `user22` varchar(20),
  `user23` varchar(20),
  `user24` varchar(20),
  `user25` varchar(20),
  `user26` varchar(20),
  `user27` varchar(20),
  `user28` varchar(20),
  `user29` varchar(20),
  `user30` varchar(20),
  `user31` varchar(20),
  `user32` varchar(20),
  `user33` varchar(20),
  `user34` varchar(20),
  `user35` varchar(20),
  `user36` varchar(100),
  `user37` varchar(20),
  `user38` varchar(20),
  `user39` varchar(20),
  `uid1` varchar(10),
  `uid2` varchar(10),
  `uid3` varchar(10),
  `uid4` varchar(10),
  `uid5` varchar(10),
  `uid6` varchar(10),
  `uid7` varchar(10),
  `uid8` varchar(10),
  `uid9` varchar(10),
  `deactivat` string,
  `salekey` decimal(8,0),
  `iasw_id` decimal(10,0),
  `trans_id` decimal(10,0),
  `upd_status` varchar(1),
  `user40` varchar(100),
  `user41` varchar(100),
  `user42` varchar(100),
  `user43` varchar(100),
  `user44` varchar(100),
  `user45` varchar(100),
  `userval1` decimal(10,0),
  `userval2` decimal(10,0),
  `userval3` decimal(10,0),
  `userval4` decimal(10,0),
  `userval5` decimal(10,0),
  `postalcode` varchar(10),
  `splitno` decimal(15,0),
  `adrid` decimal(10,0),
  `adrparchild` varchar(1),
  `adrstatus` varchar(2),
  `adrpremod` varchar(20),
  `adrpretype` varchar(20),
  `adrpostmod` varchar(20),
  `floorno` varchar(20),
  `procname` varchar(10),
  `procdate` string,
  `addrsrc` varchar(10),
  `areacd` varchar(10),
  `municd` varchar(10),
  `rollno` varchar(15),
  `addrvalid` varchar(1),
  `nbhdie` varchar(8),
  `strcd` varchar(10),
  `strreloc` varchar(150),
  `sec_fld` varchar(30),
  `user46` varchar(40),
  `user47` varchar(40),
  `user48` varchar(40),
  `user49` varchar(40),
  `user50` varchar(40),
  `user51` varchar(40),
  `user52` varchar(40),
  `user53` varchar(40),
  `user54` varchar(40),
  `user55` varchar(40),
  `user56` varchar(40),
  `user57` varchar(40),
  `user58` varchar(40),
  `user59` varchar(40),
  `user60` varchar(40),
  `user61` varchar(40),
  `user62` varchar(40),
  `user63` varchar(40),
  `user64` varchar(40),
  `user65` varchar(40),
  `user66` varchar(40),
  `user67` varchar(40),
  `user68` varchar(40),
  `user69` varchar(40),
  `user70` varchar(40),
  `user71` varchar(40),
  `user72` varchar(40),
  `user73` varchar(40),
  `user74` varchar(40),
  `user75` varchar(40),
  `user76` varchar(40),
  `user77` varchar(40),
  `user78` varchar(40),
  `user79` varchar(40),
  `user80` varchar(40),
  `user81` varchar(40),
  `user82` varchar(40),
  `user83` varchar(40),
  `user84` varchar(40),
  `user85` varchar(40),
  `userval6` decimal(10,0),
  `userval7` decimal(10,0),
  `userval8` decimal(10,0),
  `userval9` decimal(10,0),
  `userval10` decimal(10,0),
  `skip_addr_validation` varchar(1),
  `udate1` string,
  `udate2` string,
  `udate3` string,
  `udate4` string,
  `udate5` string,
  `assessorid` decimal(10,0),
  `ovrassessorid` decimal(10,0),
  `user86` varchar(80),
  `user87` varchar(80),
  `user88` varchar(80),
  `user89` varchar(80),
  `user90` varchar(80),
  `userval11` decimal(10,0),
  `userval12` decimal(10,0),
  `userval13` decimal(10,0),
  `userval14` decimal(10,0),
  `userval15` decimal(10,0),
  `ts_unit_count` decimal(10,0),
  `loaded_at` string)
PARTITIONED BY (`taxyr` string)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');
