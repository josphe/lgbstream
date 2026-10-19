#' Build a streaming LightGBM dataset from a Parquet source
#'
#' DuckDB reads a prefix of the Parquet file to fit the bin mappers, then
#' streams the rows in batches to the LightGBM C API.  Labels travel with
#' each batch as float32; no CSV and no full feature matrix are created.
#'
#' @param path Parquet file or directory.
#' @param features Feature columns.
#' @param y Numeric label vector, one value per row.
#' @param sample_rows Optional row ids used to fit the bin mappers.
#' @param weights Optional row weights.
#' @param sample_size Rows or fraction of rows used to fit the bin mappers
#'   (capped at one million rows).
#' @param batch_size Rows or fraction of rows per DuckDB fetch.
#' @param max_bin Maximum number of bins.
#' @param min_data_in_bin Minimum data per bin.
#' @param dataset_params Optional full LightGBM dataset parameter string.
#'
#' @return A list with `dataset`, `n` and `features`.
lgb_stream_dataset_arrow <- function(
    path, features, y,
    sample_rows = NULL,
    weights = NULL,
    sample_size = 50000L,
    batch_size = 32768L,
    max_bin = 255L,
    min_data_in_bin = 3L,
    dataset_params = NULL) {
  n_total <- length(y)
  sample_n <- if (is.null(sample_rows)) n_total else length(sample_rows)
  sample_size <- min(
    lgb_size(sample_size, sample_n, "sample_size"),
    n_total,
    1000000L
  )
  batch_size <- lgb_size(batch_size, n_total, "batch_size")
  if (is.null(dataset_params)) {
    dataset_params <- sprintf(
      "max_bin=%d min_data_in_bin=%d verbosity=-1",
      as.integer(max_bin), as.integer(min_data_in_bin)
    )
  }
  reader <- cpp_arrow_open(path, features)
  sample_parts <- list()
  collected <- 0L
  while (collected < sample_size) {
    batch <- cpp_arrow_next(reader)
    if (is.null(batch)) break
    x <- batch$x
    ids <- batch$row_start + seq_len(nrow(x)) - 1L
    if (!is.null(sample_rows)) {
      keep <- ids %in% sample_rows
      x <- x[keep, , drop = FALSE]
      ids <- ids[keep]
    }
    if (nrow(x)) {
      take <- min(nrow(x), sample_size - collected)
      sample_parts[[length(sample_parts) + 1L]] <-
        x[seq_len(take), , drop = FALSE]
      collected <- collected + take
    }
  }
  reader <- NULL
  gc(FALSE)
  sample_x <- do.call(rbind, sample_parts)
  rm(sample_parts)
  dataset <- cpp_dataset_from_sampled(
    sample_x, seq_len(nrow(sample_x)) - 1L, n_total, dataset_params
  )
  rm(sample_x)
  gc(FALSE)

  reader <- cpp_arrow_open(path, features)
  offset <- 0L
  while (offset < n_total) {
    batch <- cpp_arrow_next(reader)
    if (is.null(batch)) break
    x <- batch$x
    cpp_push(dataset, x, y, offset)
    offset <- offset + nrow(x)
    rm(x)
    gc(FALSE)
  }
  reader <- NULL
  if (!is.null(weights)) {
    if (length(weights) != n_total) {
      rlang::abort("`weights` must have one value per Parquet row.")
    }
    cpp_set_weight(dataset, as.numeric(weights))
  }
  cpp_finish(dataset)
  list(dataset = dataset, n = n_total, features = features)
}

lgb_stream_dataset <- function(
    path, features, y,
    sample_rows = NULL,
    weights = NULL,
    sample_size = 50000L,
    batch_size = 32768L,
    max_bin = 255L,
    min_data_in_bin = 3L,
    dataset_params = NULL) {
  lgb_init()
  if (!dir.exists(path)) {
    return(lgb_stream_dataset_arrow(
      path, features, y,
      sample_rows = sample_rows, weights = weights,
      sample_size = sample_size, batch_size = batch_size,
      max_bin = max_bin, min_data_in_bin = min_data_in_bin,
      dataset_params = dataset_params
    ))
  }
  d <- lgb_duckdb_view(path)
  on.exit(try(DBI::dbDisconnect(d$con, shutdown = TRUE), silent = TRUE), add = TRUE)

  n_total <- d$n
  if (length(y) != n_total) {
    rlang::abort(sprintf(
      "`y` has %d values but the Parquet source has %d rows.",
      length(y), n_total
    ))
  }
  missing <- setdiff(features, d$schema$column_name)
  if (length(missing)) {
    rlang::abort(sprintf(
      "Unknown feature column(s): %s.",
      paste(missing, collapse = ", ")
    ))
  }

  sample_n <- if (is.null(sample_rows)) n_total else length(sample_rows)
  sample_size <- min(
    lgb_size(sample_size, sample_n, "sample_size"),
    n_total,
    1000000L
  )
  batch_size <- lgb_size(batch_size, n_total, "batch_size")
  if (is.null(dataset_params)) {
    if (!is.numeric(max_bin) || length(max_bin) != 1L || !is.finite(max_bin) || max_bin < 2) {
      rlang::abort("`max_bin` must be a finite number >= 2.")
    }
    if (!is.numeric(min_data_in_bin) || length(min_data_in_bin) != 1L || !is.finite(min_data_in_bin) || min_data_in_bin < 1) {
      rlang::abort("`min_data_in_bin` must be a finite number >= 1.")
    }
    dataset_params <- sprintf(
      "max_bin=%d min_data_in_bin=%d verbosity=-1",
      as.integer(max_bin), as.integer(min_data_in_bin)
    )
  }
  con <- d$con
  view <- lgb_ident(con, d$view)
  rid <- lgb_ident(con, "..row_id")
  cols <- paste(vapply(features, function(x) lgb_ident(con, x), character(1L)),
                collapse = ", ")

  if (!is.null(sample_rows)) {
    sample_rows <- sort(unique(as.integer(sample_rows)))
    if (!length(sample_rows)) {
      rlang::abort("`sample_rows` must contain at least one row id.")
    }
    sample_rows <- sample_rows[seq_len(min(sample_size, length(sample_rows)))]
    sample_sql <- sprintf(
      "SELECT %s FROM %s WHERE %s IN (%s)",
      cols, view, rid, paste(sample_rows, collapse = ", ")
    )
    sample_df <- DBI::dbGetQuery(con, sample_sql)
    sample_x <- as.matrix(sample_df)
    sample_idx <- seq_len(nrow(sample_x)) - 1L
    dataset <- cpp_dataset_from_sampled(
      sample_x, sample_idx, n_total, dataset_params
    )
    rm(sample_df, sample_x, sample_idx)
    gc(FALSE)

    res <- DBI::dbSendQuery(con, sprintf("SELECT %s FROM %s", cols, view), stream = TRUE)
    on.exit(try(DBI::dbClearResult(res), silent = TRUE), add = TRUE)
    offset <- 0L
    while (offset < n_total) {
      x <- as.matrix(DBI::dbFetch(res, n = batch_size))
      if (!nrow(x)) break
      cpp_push(dataset, x, y, offset)
      offset <- offset + nrow(x)
      rm(x)
      gc(FALSE)
    }
  } else {
    # One cursor scan: the sample is the first batch and is reused for push.
    res <- DBI::dbSendQuery(con, sprintf("SELECT %s FROM %s", cols, view), stream = TRUE)
    on.exit(try(DBI::dbClearResult(res), silent = TRUE), add = TRUE)
    sample_df <- DBI::dbFetch(res, n = sample_size)
    sample_x <- as.matrix(sample_df)
    sample_idx <- seq_len(nrow(sample_x)) - 1L
    dataset <- cpp_dataset_from_sampled(
      sample_x, sample_idx, n_total, dataset_params
    )
    offset <- 0L
    if (nrow(sample_df)) {
      cpp_push(dataset, sample_x, y, offset)
      offset <- nrow(sample_x)
    }
    rm(sample_df, sample_x, sample_idx)
    gc(FALSE)

    while (offset < n_total) {
      x <- as.matrix(DBI::dbFetch(res, n = batch_size))
      if (!nrow(x)) break
      cpp_push(dataset, x, y, offset)
      offset <- offset + nrow(x)
      rm(x)
      gc(FALSE)
    }
  }
  if (!is.null(weights)) {
    if (length(weights) != n_total) {
      rlang::abort("`weights` must have one value per Parquet row.")
    }
    cpp_set_weight(dataset, as.numeric(weights))
  }
  cpp_finish(dataset)

  list(dataset = dataset, n = n_total, features = features)
}

#' Fit a LightGBM model with the streaming engine
#'
#' @param path Parquet file or directory.
#' @param features Feature columns.
#' @param y Numeric label vector, one value per row.
#' @param nrounds Number of boosting rounds.
#' @param sample_size Rows or fraction of rows used to fit the bin mappers
#'   (capped at one million rows).
#' @param batch_size Rows or fraction of rows per DuckDB fetch.
#' @param max_bin Maximum number of bins.
#' @param min_data_in_bin Minimum data per bin.
#' @param params LightGBM booster parameters.
#' @param dataset_params Optional full dataset parameter string.
#'
#' @return A list with `booster`, `eval`, `features` and `n`.
lgb_stream_fit <- function(path, features, y, nrounds = 100L,
                           sample_size = 50000L, batch_size = 32768L,
                           max_bin = 255L, min_data_in_bin = 3L,
                           params = "objective=regression metric=rmse verbosity=-1",
                           dataset_params = NULL) {
  obj <- lgb_stream_dataset(
    path, features, y,
    sample_size = sample_size, batch_size = batch_size,
    max_bin = max_bin, min_data_in_bin = min_data_in_bin,
    dataset_params = dataset_params
  )
  fit <- cpp_train(obj$dataset, as.integer(nrounds), params)
  list(
    booster = fit$booster,
    eval = fit$eval,
    features = features,
    n = obj$n
  )
}

#' Predict with a streaming booster
#'
#' @param object Result of [lgb_stream_fit()].
#' @param new_data A matrix or data frame with the same features.
#'
#' @return A numeric vector.
lgb_stream_predict <- function(object, new_data) {
  x <- as.matrix(new_data[, object$features, drop = FALSE])
  cpp_predict(object$booster, x)
}
