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


-- TiFlash Queries --

-- Table Scan
EXPLAIN ANALYZE SELECT /*+ trace_cost(), display_cost(), true_cardinality(TableRangeScan_5=16), read_from_storage(tiflash[t]) */ a FROM t WHERE a>=0 AND a<=15;


-- MPP Agg
set @@session.tidb_allow_mpp=1;
set @@session.tidb_enforce_mpp=1;

EXPLAIN ANALYZE SELECT /*+ read_from_storage(tiflash[t]) */ COUNT(*) FROM t; -- MPPTiDB

EXPLAIN ANALYZE SELECT /*+ read_from_storage(tiflash[t]) */ COUNT(*), b FROM t GROUP BY b; -- MPP2Phase


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