use synthetic;
set @@tidb_index_join_batch_size=1024;
set @@tidb_distsql_scan_concurrency=1;
set @@tidb_executor_concurrency=1;
set @@tidb_opt_tiflash_concurrency_factor=1;
set @@tidb_cost_calibration_mode=2;
set @@tidb_cost_variant=1;
-- disable coprocessor cache
-- disable plan cache
