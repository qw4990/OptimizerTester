CREATE TABLE `customer` ( -- 150000
  `C_CUSTKEY` bigint(20) NOT NULL,
  `C_NAME` varchar(25) NOT NULL,
  `C_ADDRESS` varchar(40) NOT NULL,
  `C_NATIONKEY` bigint(20) NOT NULL,
  `C_PHONE` char(15) NOT NULL,
  `C_ACCTBAL` decimal(15,2) NOT NULL,
  `C_MKTSEGMENT` char(10) NOT NULL,
  `C_COMMENT` varchar(117) NOT NULL,
  PRIMARY KEY (`C_CUSTKEY`) /*T![clustered_index] CLUSTERED */
);

CREATE TABLE `lineitem` ( -- 6000000
  `L_ORDERKEY` bigint(20) NOT NULL,
  `L_PARTKEY` bigint(20) NOT NULL,
  `L_SUPPKEY` bigint(20) NOT NULL,
  `L_LINENUMBER` bigint(20) NOT NULL,
  `L_QUANTITY` decimal(15,2) NOT NULL,
  `L_EXTENDEDPRICE` decimal(15,2) NOT NULL,
  `L_DISCOUNT` decimal(15,2) NOT NULL,
  `L_TAX` decimal(15,2) NOT NULL,
  `L_RETURNFLAG` char(1) NOT NULL,
  `L_LINESTATUS` char(1) NOT NULL,
  `L_SHIPDATE` date NOT NULL,
  `L_COMMITDATE` date NOT NULL,
  `L_RECEIPTDATE` date NOT NULL,
  `L_SHIPINSTRUCT` char(25) NOT NULL,
  `L_SHIPMODE` char(10) NOT NULL,
  `L_COMMENT` varchar(44) NOT NULL,
  PRIMARY KEY (`L_ORDERKEY`,`L_LINENUMBER`) /*T![clustered_index] NONCLUSTERED */
);

ALTER TABLE lineitem ADD INDEX `L_SUPPKEY` (`L_SUPPKEY`);

CREATE TABLE `nation` ( -- 25
  `N_NATIONKEY` bigint(20) NOT NULL,
  `N_NAME` char(25) NOT NULL,
  `N_REGIONKEY` bigint(20) NOT NULL,
  `N_COMMENT` varchar(152) DEFAULT NULL,
  PRIMARY KEY (`N_NATIONKEY`) /*T![clustered_index] CLUSTERED */
);

CREATE TABLE `orders` ( -- 1500000
  `O_ORDERKEY` bigint(20) NOT NULL,
  `O_CUSTKEY` bigint(20) NOT NULL,
  `O_ORDERSTATUS` char(1) NOT NULL,
  `O_TOTALPRICE` decimal(15,2) NOT NULL,
  `O_ORDERDATE` date NOT NULL,
  `O_ORDERPRIORITY` char(15) NOT NULL,
  `O_CLERK` char(15) NOT NULL,
  `O_SHIPPRIORITY` bigint(20) NOT NULL,
  `O_COMMENT` varchar(79) NOT NULL,
  PRIMARY KEY (`O_ORDERKEY`) /*T![clustered_index] CLUSTERED */
);

ALTER TABLE orders ADD INDEX `O_CUSTKEY` (`O_CUSTKEY`);

CREATE TABLE `part` ( -- 200000
  `P_PARTKEY` bigint(20) NOT NULL,
  `P_NAME` varchar(55) NOT NULL,
  `P_MFGR` char(25) NOT NULL,
  `P_BRAND` char(10) NOT NULL,
  `P_TYPE` varchar(25) NOT NULL,
  `P_SIZE` bigint(20) NOT NULL,
  `P_CONTAINER` char(10) NOT NULL,
  `P_RETAILPRICE` decimal(15,2) NOT NULL,
  `P_COMMENT` varchar(23) NOT NULL,
  PRIMARY KEY (`P_PARTKEY`) /*T![clustered_index] CLUSTERED */
);

CREATE TABLE `partsupp` ( -- 800000
  `PS_PARTKEY` bigint(20) NOT NULL,
  `PS_SUPPKEY` bigint(20) NOT NULL,
  `PS_AVAILQTY` bigint(20) NOT NULL,
  `PS_SUPPLYCOST` decimal(15,2) NOT NULL,
  `PS_COMMENT` varchar(199) NOT NULL,
  PRIMARY KEY (`PS_PARTKEY`,`PS_SUPPKEY`) /*T![clustered_index] NONCLUSTERED */
);

CREATE TABLE `region` ( -- 5
  `R_REGIONKEY` bigint(20) NOT NULL,
  `R_NAME` char(25) NOT NULL,
  `R_COMMENT` varchar(152) DEFAULT NULL,
  PRIMARY KEY (`R_REGIONKEY`) /*T![clustered_index] CLUSTERED */
);

CREATE TABLE `supplier` ( -- 10000
  `S_SUPPKEY` bigint(20) NOT NULL,
  `S_NAME` char(25) NOT NULL,
  `S_ADDRESS` varchar(40) NOT NULL,
  `S_NATIONKEY` bigint(20) NOT NULL,
  `S_PHONE` char(15) NOT NULL,
  `S_ACCTBAL` decimal(15,2) NOT NULL,
  `S_COMMENT` varchar(101) NOT NULL,
  PRIMARY KEY (`S_SUPPKEY`) /*T![clustered_index] CLUSTERED */
);

-- ########################## evaluation queries ##########################

-- Table/Index Acs/Desc Scan
SELECT /*+ use_index(customer, primary) */ * FROM customer WHERE C_CUSTKEY>=? AND C_CUSTKEY<=?; -- table scan
SELECT /*+ use_index(lineitem, primary) */ L_LINENUMBER FROM lineitem WHERE L_ORDERKEY>=? AND L_ORDERKEY<=?; -- index(PK) scan
SELECT /*+ use_index(orders, primary) */ O_ORDERKEY FROM orders WHERE O_ORDERKEY>=? AND O_ORDERKEY<=?; -- index(PK) scan
SELECT /*+ use_index(orders, primary), no_reorder() */ O_ORDERKEY FROM orders WHERE O_ORDERKEY>=? AND O_ORDERKEY<=? ORDER BY O_ORDERKEY DESC; -- index(PK) desc scan
SELECT /*+ use_index(partsupp, primary) */ * FROM partsupp WHERE PS_PARTKEY>=? AND PS_PARTKEY<=? ORDER BY PS_PARTKEY DESC; -- desc table scan

-- IndexLookup
SELECT /*+ use_index(orders, O_CUSTKEY) */ * FROM orders WHERE O_CUSTKEY>=? AND O_CUSTKEY<=?; -- index lookup
SELECT /*+ use_index(lineitem, L_SUPPKEY) */ * FROM lineitem WHERE L_SUPPKEY>=? AND L_SUPPKEY<=?; -- index lookup

-- Aggregation

-- Sort

