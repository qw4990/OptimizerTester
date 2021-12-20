CREATE TABLE `customer` (
  `C_CUSTKEY` bigint(20) NOT NULL,
  `C_NAME` varchar(25) NOT NULL,
  `C_ADDRESS` varchar(40) NOT NULL,
  `C_NATIONKEY` bigint(20) NOT NULL,
  `C_PHONE` char(15) NOT NULL,
  `C_ACCTBAL` decimal(15,2) NOT NULL,
  `C_MKTSEGMENT` char(10) NOT NULL,
  `C_COMMENT` varchar(117) NOT NULL,
  PRIMARY KEY (`C_CUSTKEY`) /*T![clustered_index] CLUSTERED */,
  CONSTRAINT `CUSTOMER_FK1` FOREIGN KEY (`C_NATIONKEY`) REFERENCES `nation` (`N_NATIONKEY`)
);

SELECT * FROM customer WHERE C_CUSTKEY = ?;                             -- find a customer by its key
SELECT * FROM customer WHERE C_NAME = ?;                                -- find customers by the name
SELECT * FROM customer WHERE C_ADDRESS = ?;                             -- find customers by the address
SELECT * FROM customer WHERE C_PHONE = ?;                               -- find customers by the phone


CREATE TABLE `nation` (
  `N_NATIONKEY` bigint(20) NOT NULL,
  `N_NAME` char(25) NOT NULL,
  `N_REGIONKEY` bigint(20) NOT NULL,
  `N_COMMENT` varchar(152) DEFAULT NULL,
  PRIMARY KEY (`N_NATIONKEY`) /*T![clustered_index] CLUSTERED */
);

SELECT * FROM nation WHERE N_NAME = ?;                                      -- find a nation by its name
SELECT * FROM nation WHERE N_REGIONKEY = ?;                                 -- find nations by the region-key


CREATE TABLE `supplier` (
  `S_SUPPKEY` bigint(20) NOT NULL,
  `S_NAME` char(25) NOT NULL,
  `S_ADDRESS` varchar(40) NOT NULL,
  `S_NATIONKEY` bigint(20) NOT NULL,
  `S_PHONE` char(15) NOT NULL,
  `S_ACCTBAL` decimal(15,2) NOT NULL,
  `S_COMMENT` varchar(101) NOT NULL,
  PRIMARY KEY (`S_SUPPKEY`) /*T![clustered_index] CLUSTERED */,
  CONSTRAINT `SUPPLIER_FK1` FOREIGN KEY (`S_NATIONKEY`) REFERENCES `nation` (`N_NATIONKEY`)
);

SELECT * FROM supplier WHERE S_SUPPKEY = ?;                                 -- find a supplier by its key
SELECT * FROM supplier WHERE S_NAME = ?;                                    -- find a supplier by its address


CREATE TABLE `orders` (
  `O_ORDERKEY` bigint(20) NOT NULL,
  `O_CUSTKEY` bigint(20) NOT NULL,
  `O_ORDERSTATUS` char(1) NOT NULL,
  `O_TOTALPRICE` decimal(15,2) NOT NULL,
  `O_ORDERDATE` date NOT NULL,
  `O_ORDERPRIORITY` char(15) NOT NULL,
  `O_CLERK` char(15) NOT NULL,
  `O_SHIPPRIORITY` bigint(20) NOT NULL,
  `O_COMMENT` varchar(79) NOT NULL,
  PRIMARY KEY (`O_ORDERKEY`) /*T![clustered_index] CLUSTERED */,
  CONSTRAINT `ORDERS_FK1` FOREIGN KEY (`O_CUSTKEY`) REFERENCES `customer` (`C_CUSTKEY`)
);

SELECT * FROM orders WHERE O_ORDERKEY = ?;                                  -- find a order by its key
SELECT * FROM orders WHERE O_CUSTKEY = ?;                                   -- find orders by customer key
SELECT * FROM orders WHERE O_ORDERDATE = ?;                                 -- find orders by date
SELECT * FROM orders WHERE O_ORDERDATE BETWEEN ? AND ?;                     -- find orders by date


CREATE TABLE `lineitem` (
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
  PRIMARY KEY (`L_ORDERKEY`,`L_LINENUMBER`) /*T![clustered_index] NONCLUSTERED */,
  CONSTRAINT `LINEITEM_FK1` FOREIGN KEY (`L_ORDERKEY`) REFERENCES `orders` (`O_ORDERKEY`),
  CONSTRAINT `LINEITEM_FK2` FOREIGN KEY (`L_PARTKEY`,`L_SUPPKEY`) REFERENCES `partsupp` (`PS_PARTKEY`,`PS_SUPPKEY`)
);

SELECT * FROM lineitem WHERE L_QUANTITY BETWEEN ? AND ?;                        -- find lineitems by quantity
SELECT * FROM lineitem WHERE L_EXTENDEDPRICE BETWEEN ? AND ?;                   -- find lineitems by price
SELECT * FROM lineitem WHERE L_DISCOUNT BETWEEN ? AND ?;                        -- find lineitems by discount
SELECT * FROM lineitem WHERE L_TAX BETWEEN ? AND ?;                             -- find lineitems by tax
SELECT * FROM lineitem WHERE L_COMMITDATE BETWEEN ? AND ?;                      -- find lineitems by commit date
SELECT * FROM lineitem WHERE L_RECEIPTDATE BETWEEN ? AND ?;                     -- find lineitems by receip date


SELECT * FROM lineitem WHERE L_QUANTITY BETWEEN ? AND ? ORDER BY L_QUANTITY;                                -- find lineitems by quantity
SELECT * FROM lineitem WHERE L_EXTENDEDPRICE BETWEEN ? AND ? ORDER BY L_EXTENDEDPRICE;                      -- find lineitems by price
SELECT * FROM lineitem WHERE L_DISCOUNT BETWEEN ? AND ? ORDER BY L_DISCOUNT;                                -- find lineitems by discount
SELECT * FROM lineitem WHERE L_TAX BETWEEN ? AND ? ORDER BY L_TAX;                                          -- find lineitems by tax
SELECT * FROM lineitem WHERE L_COMMITDATE BETWEEN ? AND ? ORDER BY L_COMMITDATE;                            -- find lineitems by commit date
SELECT * FROM lineitem WHERE L_RECEIPTDATE BETWEEN ? AND ? ORDER BY L_RECEIPTDATE;                          -- find lineitems by receip date


SELECT COUNT(*) FROM lineitem WHERE L_QUANTITY BETWEEN ? AND ?;                        -- find count of lineitems by quantity
SELECT COUNT(*) FROM lineitem WHERE L_EXTENDEDPRICE BETWEEN ? AND ?;                   -- find count of lineitems by price
SELECT COUNT(*) FROM lineitem WHERE L_DISCOUNT BETWEEN ? AND ?;                        -- find count of lineitems by discount
SELECT COUNT(*) FROM lineitem WHERE L_TAX BETWEEN ? AND ?;                             -- find count of lineitems by tax
SELECT COUNT(*) FROM lineitem WHERE L_COMMITDATE BETWEEN ? AND ?;                      -- find count of lineitems by commit date
SELECT COUNT(*) FROM lineitem WHERE L_RECEIPTDATE BETWEEN ? AND ?;                     -- find count of lineitems by receip date
