#' Prepare Parquet with streaming preprocessing
#'
#' Applies a data-frame transform to each Arrow `RecordBatch` and writes the
#' result to a new Parquet file.  The output is staged in an on-disk DuckDB
#' table, so the full transformed data set is never held in R.
#'
#' @param path Single input Parquet file.
#' @param output Output Parquet path.
#' @param preprocess Function taking a `data.frame` and returning a
#'   `data.frame`; `NULL` leaves batches unchanged.
#' @param overwrite Replace an existing output file.
#'
#' @return The output path, invisibly.
#' @export
lgb_prepare_parquet <- function(path, output, preprocess = NULL,
                                overwrite = FALSE) {
  if (dir.exists(path)) {
    rlang::abort("`lgb_prepare_parquet()` supports a single Parquet file.")
  }
  if (!file.exists(path)) {
    rlang::abort("Input Parquet file does not exist.")
  }
  lgb_check_flag(overwrite, "overwrite")
  if (file.exists(output) && !isTRUE(overwrite)) {
    rlang::abort("Output exists; set `overwrite = TRUE`.")
  }
  if (isTRUE(overwrite) && file.exists(output)) {
    unlink(output)
  }

  db <- tempfile(fileext = ".duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(dbdir = db))
  on.exit({
    try(DBI::dbDisconnect(con, shutdown = TRUE), silent = TRUE)
    unlink(db)
  }, add = TRUE)

  all_names <- cpp_arrow_schema(path)
  reader <- cpp_arrow_open(path, all_names)
  first <- TRUE
  repeat {
    batch <- cpp_arrow_next(reader)
    if (is.null(batch)) break
    data <- lgb_apply_preprocess(as.data.frame(batch$x), preprocess)
    if (nrow(data)) {
      if (first) {
        DBI::dbWriteTable(con, "prepared", data, overwrite = TRUE)
        first <- FALSE
      } else {
        DBI::dbAppendTable(con, "prepared", data)
      }
    }
    rm(batch, data)
    gc(FALSE)
  }
  reader <- NULL
  if (first) {
    rlang::abort("Preprocessing produced no rows.")
  }

  DBI::dbExecute(
    con,
    sprintf(
      "COPY (SELECT * FROM prepared) TO %s (FORMAT PARQUET, COMPRESSION ZSTD)",
      DBI::dbQuoteString(con, output)
    )
  )
  invisible(output)
}
