-- SCHEMA --
create table t (
	a int, 
	b int,
	c varchar(128), -- always space(128)
	d int,
	primary key(a),
	key b(b),
	key bc(b, c)
);

ALTER TABLE t SET TIFLASH REPLICA 1;

use synthetic;
set @@tidb_distsql_scan_concurrency=1;
set @@tidb_executor_concurrency=1;
set @@tidb_opt_tiflash_concurrency_factor=1;
set @@tidb_cost_variant=1;
-- disable coprocessor cache
-- disable plan cache



-- TiFlash Queries --

-- Table Scan
set @@session.tidb_enforce_mpp=1;
EXPLAIN ANALYZE SELECT /*+ trace_cost(), display_cost(), true_cardinality(TableRangeScan_5=16), read_from_storage(tiflash[t]) */ a FROM t WHERE a>=0 AND a<=15; -- MPPScan


-- MPP Agg
set @@session.tidb_allow_mpp=1;
set @@session.tidb_enforce_mpp=1;

EXPLAIN ANALYZE SELECT /*+ display_cost(), trace_cost(), read_from_storage(tiflash[t]) */ COUNT(*) FROM t WHERE b>=1 and b<=10; -- MPPTiDB

EXPLAIN ANALYZE SELECT /*+ display_cost(), trace_cost(), read_from_storage(tiflash[t]) */ COUNT(*), b FROM t WHERE b>=1 and b<=10 GROUP BY b; -- MPP2Phase

-- MPP JOIN
set @@session.tidb_allow_mpp=1;
set @@session.tidb_broadcast_join_threshold_size = 1;
set @@session.tidb_broadcast_join_threshold_count = 1;
set @@session.tidb_opt_broadcast_join=1;

set @@session.tidb_enforce_mpp=1;
set @@session.tidb_opt_broadcast_join=0;
EXPLAIN ANALYZE SELECT /*+ display_cost(), trace_cost(), read_from_storage(tiflash[t1, t2]) */ t1.b, t2.b FROM t t1, t t2 WHERE t1.b=t2.b and t1.b>=1 and t1.b<=100 and t2.b>=1 and t2.b<=100; -- MPPHJ


set @@session.tidb_enforce_mpp=0;
set @@session.tidb_opt_broadcast_join=1;
EXPLAIN ANALYZE SELECT /*+ display_cost(), trace_cost(), broadcast_join(t1, t2), read_from_storage(tiflash[t1, t2]) */ t1.b from t t1 left join t t2 on t1.b=t2.b where t1.b>=1 and t1.b<=100 and t2.b>=1 and t2.b<=100; -- MPPBCJ










mysql> EXPLAIN ANALYZE SELECT /*+ display_cost(), trace_cost(), broadcast_join(t1, t2), read_from_storage(tiflash[t1, t2]) */ t1.b from t t1 left join t t2 on t1.b=t2.b where t1.b>=1 and t1.b<=100 and t2.b>=1 and t2.b<=100; -- MPPBCJ
+----------------------------------------+----------+-------------------------------------------------------------------------+---------+--------------+---------------+---------------------------------------------------------------------------------------+--------------------------------------------------------------------------+---------+------+
| id                                     | estRows  | estCost                                                                 | actRows | task         | access object | execution info                                                                        | operator info                                                            | memory  | disk |
+----------------------------------------+----------+-------------------------------------------------------------------------+---------+--------------+---------------+---------------------------------------------------------------------------------------+--------------------------------------------------------------------------+---------+------+
| Projection_6                           | 312.50   | 573239.75:[879.00,20000.00,312.50,340000.00,0.00,250.00,2.00]:572989.75 | 238     | root         |               | time:87.3ms, loops:2, Concurrency:OFF                                                 | synthetic.t.b                                                            | 4.28 KB | N/A  |
| └─TableReader_16                       | 312.50   | 572296.25:[564.50,20000.00,312.50,340000.00,0.00,250.00,2.00]:572046.25 | 238     | root         |               | time:87.3ms, loops:2, cop_task: {num: 1, max: 0s, proc_keys: 0, copr_cache: disabled} | data:ExchangeSender_15                                                   | N/A     | N/A  |
|   └─ExchangeSender_15                  | 312.50   | 571983.75:[564.50,20000.00,0.00,340000.00,0.00,250.00,2.00]:571733.75   | 238     | cop[tiflash] |               | tiflash_task:{time:29.6ms, loops:3, threads:8}                                        | ExchangeType: PassThrough                                                | N/A     | N/A  |
|     └─HashJoin_8                       | 312.50   | 571983.75:[564.50,20000.00,0.00,340000.00,0.00,250.00,2.00]:571733.75   | 238     | cop[tiflash] |               | tiflash_task:{time:29.6ms, loops:3, threads:8}                                        | inner join, equal:[eq(synthetic.t.b, synthetic.t.b)]                     | N/A     | N/A  |
|       ├─ExchangeReceiver_12(Build)     | 250.00   | 285270.00:[0.00,10000.00,0.00,170000.00,0.00,0.00,1.00]:285020.00       | 100     | cop[tiflash] |               | tiflash_task:{time:19.6ms, loops:3, threads:8}                                        |                                                                          | N/A     | N/A  |
|       │ └─ExchangeSender_11            | 250.00   | 285270.00:[0.00,10000.00,0.00,170000.00,0.00,0.00,1.00]:285020.00       | 100     | cop[tiflash] |               | tiflash_task:{time:20.8ms, loops:3, threads:1}                                        | ExchangeType: Broadcast                                                  | N/A     | N/A  |
|       │   └─Selection_10               | 250.00   | 285020.00:[0.00,10000.00,0.00,170000.00,0.00,0.00,1.00]:285020.00       | 100     | cop[tiflash] |               | tiflash_task:{time:20.8ms, loops:3, threads:1}                                        | ge(synthetic.t.b, 1), le(synthetic.t.b, 100), not(isnull(synthetic.t.b)) | N/A     | N/A  |
|       │     └─TableFullScan_9          | 10000.00 | 255020.00:[0.00,0.00,0.00,170000.00,0.00,0.00,1.00]:255020.00           | 100000  | cop[tiflash] | table:t1      | tiflash_task:{time:18.8ms, loops:3, threads:1}                                        | keep order:false, stats:pseudo                                           | N/A     | N/A  |
|       └─Selection_14(Probe)            | 250.00   | 285020.00:[0.00,10000.00,0.00,170000.00,0.00,0.00,1.00]:285020.00       | 100     | cop[tiflash] |               | tiflash_task:{time:16.6ms, loops:3, threads:1}                                        | ge(synthetic.t.b, 1), le(synthetic.t.b, 100), not(isnull(synthetic.t.b)) | N/A     | N/A  |
|         └─TableFullScan_13             | 10000.00 | 255020.00:[0.00,0.00,0.00,170000.00,0.00,0.00,1.00]:255020.00           | 100000  | cop[tiflash] | table:t2      | tiflash_task:{time:15.6ms, loops:3, threads:1}                                        | keep order:false, stats:pseudo                                           | N/A     | N/A  |
+----------------------------------------+----------+-------------------------------------------------------------------------+---------+--------------+---------------+---------------------------------------------------------------------------------------+--------------------------------------------------------------------------+---------+------+
10 rows in set, 14 warnings (0.22 sec)

mysql> 
mysql> set @@session.tidb_opt_broadcast_join=0;
Query OK, 0 rows affected (0.00 sec)

mysql> EXPLAIN ANALYZE SELECT /*+ display_cost(), trace_cost(), read_from_storage(tiflash[t1, t2]) */ t1.b, t2.b FROM t t1, t t2 WHERE t1.b=t2.b and t1.b>=1 and t1.b<=100 and t2.b>=1 and t2.b<=100; -- MPPHJ
+--------------------------------------+----------+-------------------------------------------------------------------------+---------+--------------+---------------+----------------------------------------------------------------------------------------------------+-----------------------------------------------------------------------------+--------+------+
| id                                   | estRows  | estCost                                                                 | actRows | task         | access object | execution info                                                                                     | operator info                                                               | memory | disk |
+--------------------------------------+----------+-------------------------------------------------------------------------+---------+--------------+---------------+----------------------------------------------------------------------------------------------------+-----------------------------------------------------------------------------+--------+------+
| TableReader_23                       | 312.50   | 572546.25:[564.50,20000.00,312.50,340000.00,0.00,250.00,2.00]:572046.25 | 238     | root         |               | time:88.5ms, loops:2, cop_task: {num: 3, max: 0s, min: 0s, avg: 0s, p95: 0s, copr_cache: disabled} | data:ExchangeSender_22                                                      | N/A    | N/A  |
| └─ExchangeSender_22                  | 312.50   | 572233.75:[564.50,20000.00,0.00,340000.00,0.00,250.00,2.00]:571733.75   | 238     | cop[tiflash] |               | tiflash_task:{time:17.7ms, loops:3, threads:8}                                                     | ExchangeType: PassThrough                                                   | N/A    | N/A  |
|   └─HashJoin_9                       | 312.50   | 572233.75:[564.50,20000.00,0.00,340000.00,0.00,250.00,2.00]:571733.75   | 238     | cop[tiflash] |               | tiflash_task:{time:17.7ms, loops:3, threads:8}                                                     | inner join, equal:[eq(synthetic.t.b, synthetic.t.b)]                        | N/A    | N/A  |
|     ├─ExchangeReceiver_17(Build)     | 250.00   | 285270.00:[0.00,10000.00,0.00,170000.00,0.00,0.00,1.00]:285020.00       | 100     | cop[tiflash] |               | tiflash_task:{time:17.7ms, loops:3, threads:8}                                                     |                                                                             | N/A    | N/A  |
|     │ └─ExchangeSender_16            | 250.00   | 285270.00:[0.00,10000.00,0.00,170000.00,0.00,0.00,1.00]:285020.00       | 100     | cop[tiflash] |               | tiflash_task:{time:17.8ms, loops:3, threads:1}                                                     | ExchangeType: HashPartition, Hash Cols: [name: synthetic.t.b, collate: N/A] | N/A    | N/A  |
|     │   └─Selection_15               | 250.00   | 285020.00:[0.00,10000.00,0.00,170000.00,0.00,0.00,1.00]:285020.00       | 100     | cop[tiflash] |               | tiflash_task:{time:17.8ms, loops:3, threads:1}                                                     | ge(synthetic.t.b, 1), le(synthetic.t.b, 100), not(isnull(synthetic.t.b))    | N/A    | N/A  |
|     │     └─TableFullScan_14         | 10000.00 | 255020.00:[0.00,0.00,0.00,170000.00,0.00,0.00,1.00]:255020.00           | 100000  | cop[tiflash] | table:t1      | tiflash_task:{time:14.8ms, loops:3, threads:1}                                                     | keep order:false, stats:pseudo                                              | N/A    | N/A  |
|     └─ExchangeReceiver_21(Probe)     | 250.00   | 285270.00:[0.00,10000.00,0.00,170000.00,0.00,0.00,1.00]:285020.00       | 100     | cop[tiflash] |               | tiflash_task:{time:2.7ms, loops:3, threads:8}                                                      |                                                                             | N/A    | N/A  |
|       └─ExchangeSender_20            | 250.00   | 285270.00:[0.00,10000.00,0.00,170000.00,0.00,0.00,1.00]:285020.00       | 100     | cop[tiflash] |               | tiflash_task:{time:16.1ms, loops:3, threads:1}                                                     | ExchangeType: HashPartition, Hash Cols: [name: synthetic.t.b, collate: N/A] | N/A    | N/A  |
|         └─Selection_19               | 250.00   | 285020.00:[0.00,10000.00,0.00,170000.00,0.00,0.00,1.00]:285020.00       | 100     | cop[tiflash] |               | tiflash_task:{time:16.1ms, loops:3, threads:1}                                                     | ge(synthetic.t.b, 1), le(synthetic.t.b, 100), not(isnull(synthetic.t.b))    | N/A    | N/A  |
|           └─TableFullScan_18         | 10000.00 | 255020.00:[0.00,0.00,0.00,170000.00,0.00,0.00,1.00]:255020.00           | 100000  | cop[tiflash] | table:t2      | tiflash_task:{time:13.1ms, loops:3, threads:1}                                                     | keep order:false, stats:pseudo                                              | N/A    | N/A  |
+--------------------------------------+----------+-------------------------------------------------------------------------+---------+--------------+---------------+----------------------------------------------------------------------------------------------------+-----------------------------------------------------------------------------+--------+------+
11 rows in set, 12 warnings (0.26 sec)
















-- SIMPLE DATA --

insert into t values (0, 0, '0', 0);
insert into t values (1, 1, '1', 1);
insert into t values (2, 2, '2', 2);
insert into t values (3, 3, '3', 3);
insert into t values (4, 4, '4', 4);
insert into t values (5, 5, '5', 5);
insert into t values (6, 6, '6', 6);
insert into t values (7, 7, '7', 7);
insert into t values (8, 8, '8', 8);
insert into t values (9, 9, '9', 9);
insert into t values (10, 10, '10', 10);
insert into t values (11, 11, '11', 11);
insert into t values (12, 12, '12', 12);
insert into t values (13, 13, '13', 13);
insert into t values (14, 14, '14', 14);
insert into t values (15, 15, '15', 15);
insert into t values (16, 16, '16', 16);
insert into t values (17, 17, '17', 17);
insert into t values (18, 18, '18', 18);
insert into t values (19, 19, '19', 19);
insert into t values (20, 20, '20', 20);
insert into t values (21, 21, '21', 21);
insert into t values (22, 22, '22', 22);
insert into t values (23, 23, '23', 23);
insert into t values (24, 24, '24', 24);
insert into t values (25, 25, '25', 25);
insert into t values (26, 26, '26', 26);
insert into t values (27, 27, '27', 27);
insert into t values (28, 28, '28', 28);
insert into t values (29, 29, '29', 29);
insert into t values (30, 30, '30', 30);
insert into t values (31, 31, '31', 31);
insert into t values (32, 32, '32', 32);
insert into t values (33, 33, '33', 33);
insert into t values (34, 34, '34', 34);
insert into t values (35, 35, '35', 35);
insert into t values (36, 36, '36', 36);
insert into t values (37, 37, '37', 37);
insert into t values (38, 38, '38', 38);
insert into t values (39, 39, '39', 39);
insert into t values (40, 40, '40', 40);
insert into t values (41, 41, '41', 41);
insert into t values (42, 42, '42', 42);
insert into t values (43, 43, '43', 43);
insert into t values (44, 44, '44', 44);
insert into t values (45, 45, '45', 45);
insert into t values (46, 46, '46', 46);
insert into t values (47, 47, '47', 47);
insert into t values (48, 48, '48', 48);
insert into t values (49, 49, '49', 49);