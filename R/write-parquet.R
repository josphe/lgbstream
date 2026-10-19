lgb_write_parquet <- function(data, path) {
  con <- DBI::dbConnect(duckdb::duckdb(shared_home = FALSE))
  on.exit(try(DBI::dbDisconnect(con, shutdown = TRUE), silent = TRUE), add = TRUE)
  DBI::dbWriteTable(con, "lgbstream_data", data, overwrite = TRUE)
  DBI::dbExecute(
    con,
    sprintf(
      "COPY (SELECT * FROM lgbstream_data) TO %s (FORMAT PARQUET, COMPRESSION ZSTD)",
      DBI::dbQuoteString(con, path)
    )
  )
  invisible(path)
}
