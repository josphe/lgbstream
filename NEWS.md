# lgbstream 0.1.0

* Initial release.
* `lgb_fit()` / `lgb_predict()` tidy interface.
* `DataBackendParquet` and `LearnerRegrLGBStream` for mlr3.
* C++ streaming ingestion: `LGBM_DatasetCreateFromSampledColumn()` +
  `LGBM_DatasetPushRowsWithMetadata()` + `LGBM_DatasetMarkFinished()`.
* DuckDB-backed Parquet backend with row-range reads.
