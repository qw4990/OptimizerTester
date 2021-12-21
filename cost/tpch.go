package cost

import "github.com/qw4990/OptimizerTester/tidb"

/*

SELECT * FROM customer WHERE C_CUSTKEY = ?;                             -- find a customer by its key
SELECT * FROM customer WHERE C_NAME = ?;                                -- find customers by the name
SELECT * FROM customer WHERE C_ADDRESS = ?;                             -- find customers by the address
SELECT * FROM customer WHERE C_PHONE = ?;                               -- find customers by the phone


SELECT * FROM nation WHERE N_NAME = ?;                                      -- find a nation by its name
SELECT * FROM nation WHERE N_REGIONKEY = ?;                                 -- find nations by the region-key


SELECT * FROM supplier WHERE S_SUPPKEY = ?;                                 -- find a supplier by its key
SELECT * FROM supplier WHERE S_NAME = ?;                                    -- find a supplier by its address

SELECT * FROM orders WHERE O_ORDERKEY = ?;                                  -- find a order by its key
SELECT * FROM orders WHERE O_CUSTKEY = ?;                                   -- find orders by customer key
SELECT * FROM orders WHERE O_ORDERDATE = ?;                                 -- find orders by date
SELECT * FROM orders WHERE O_ORDERDATE BETWEEN ? AND ?;                     -- find orders by date


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

*/

func genTPCHQueries(ins tidb.Instance, db string) []string {
	return nil
}