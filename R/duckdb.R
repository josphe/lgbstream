lgb_ident <- function(con, x) {
  as.character(DBI::dbQuoteIdentifier(con, x))
}

lgb_string <- function(con, x) {
  as.character(DBI::dbQuoteString(con, x))
}

lgb_duckdb_view <- function(path) {
  if (!file.exists(path) && !dir.exists(path)) {
    rlang::abort(sprintf("Parquet path does not exist: %s.", path))
  }
  is_dir <- dir.exists(path)
  source <- if (is_dir) file.path(path, "*.parquet") else path
  con <- tryCatch(
    suppressMessages(DBI::dbConnect(duckdb::duckdb(shared_home = FALSE))),
    error = function(e) DBI::dbConnect(duckdb::duckdb())
  )
  view <- paste0("lgbstream_", as.integer(Sys.time()), "_", sample.int(1e6L, 1L))
  source_sql <- if (is_dir) {
    sprintf(
      "read_parquet(%s, file_row_number=true, filename=true)",
      lgb_string(con, source)
    )
  } else {
    sprintf(
      "read_parquet(%s, file_row_number=true)",
      lgb_string(con, source)
    )
  }
  source_cols <- DBI::dbGetQuery(
    con,
    sprintf("DESCRIBE SELECT * FROM %s", source_sql)
  )$column_name
  if ("..row_id" %in% source_cols) {
    sql <- sprintf(
      "CREATE VIEW %s AS SELECT * FROM %s",
      lgb_ident(con, view), source_sql
    )
  } else if (!is_dir && "file_row_number" %in% source_cols) {
    # Physical row numbers are stable and let DuckDB prune Parquet row groups.
    sql <- sprintf(
      "CREATE VIEW %s AS SELECT file_row_number + 1 AS %s, * EXCLUDE (file_row_number) FROM %s",
      lgb_ident(con, view),
      lgb_ident(con, "..row_id"),
      source_sql
    )
  } else {
    sql <- sprintf(
      "CREATE VIEW %s AS SELECT row_number() OVER (ORDER BY filename, file_row_number) AS %s, * EXCLUDE (file_row_number, filename) FROM %s",
      lgb_ident(con, view),
      lgb_ident(con, "..row_id"),
      source_sql
    )
  }
  DBI::dbExecute(con, sql)

  n <- DBI::dbGetQuery(
    con,
    sprintf("SELECT count(*) AS n FROM %s", lgb_ident(con, view))
  )$n[[1L]]
  schema <- DBI::dbGetQuery(
    con,
    sprintf("DESCRIBE %s", lgb_ident(con, view))
  )
  list(con = con, view = view, n = as.integer(n), schema = schema)
}
