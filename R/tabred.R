#' TabReD-style time features from `fact_time`
#'
#' Derives calendar features and drops the columns used by the reference
#' TabReD script.  `fact_time` may be numeric seconds since the epoch or a
#' `POSIXct` column.
#'
#' @param chunk A data frame with a `fact_time` column.
#'
#' @return The transformed data frame.
#' @export
lgb_tabred_preprocess <- function(chunk) {
  if (!"fact_time" %in% names(chunk)) {
    return(chunk)
  }
  time <- chunk$fact_time
  if (inherits(time, "POSIXct")) {
    tt <- as.POSIXlt(time, tz = "UTC")
  } else {
    tt <- as.POSIXlt(as.POSIXct(time, origin = "1970-01-01", tz = "UTC"), tz = "UTC")
  }
  chunk$day_of_week <- (tt$wday + 6) %% 7
  chunk$day_of_month <- tt$mday
  chunk$minute_of_day <- tt$hour * 60 + tt$min
  chunk$hour_of_day <- tt$hour
  chunk$month <- tt$mon + 1

  drop <- intersect(
    c("apply_time_rl", "fact_latitude", "fact_longitude", "fact_station_id"),
    names(chunk)
  )
  chunk[, setdiff(names(chunk), drop), drop = FALSE]
}

lgb_apply_preprocess <- function(data, preprocess) {
  if ("..row_id" %in% names(data)) {
    data[["..row_id"]] <- NULL
  }
  if (is.null(preprocess)) {
    return(data)
  }
  out <- preprocess(data)
  if (!is.data.frame(out)) {
    rlang::abort("`preprocess` must return a data frame.")
  }
  out
}


lgb_make_split <- function(n, split_vector, split_values, seed) {
  if (!is.null(split_vector)) {
    if (is.null(split_values)) {
      rlang::abort("`split_val` is required when `split_col` is set.")
    }
    split_values <- as.numeric(split_values)
    if (!length(split_values) %in% 1:2 || any(!is.finite(split_values))) {
      rlang::abort("`split_val` must contain one or two finite thresholds.")
    }
    split_values <- sort(split_values)
    if (length(split_values) == 1L) {
      train <- which(split_vector < split_values[[1L]])
      val <- integer()
      test <- which(split_vector >= split_values[[1L]])
    } else {
      train <- which(split_vector < split_values[[1L]])
      val <- which(split_vector >= split_values[[1L]] & split_vector < split_values[[2L]])
      test <- which(split_vector >= split_values[[2L]])
    }
  } else {
    if (is.null(split_values)) {
      rlang::abort("Provide `split_val` or `train_prop`/`val_prop`.")
    }
    split_values <- as.numeric(split_values)
    if (length(split_values) > 2L ||
        any(!is.finite(split_values)) ||
        any(split_values < 0) ||
        sum(split_values) > 1) {
      rlang::abort("Split proportions must be finite, non-negative, and sum to at most 1.")
    }
    if (length(split_values) == 1L) {
      split_values <- c(split_values, 0)
    }
    set.seed(seed)
    perm <- sample.int(n)
    n_train <- floor(n * split_values[[1L]])
    n_val <- floor(n * split_values[[2L]])
    train <- sort(perm[seq_len(n_train)])
    val <- sort(perm[n_train + seq_len(n_val)])
    test <- sort(perm[-seq_len(n_train + n_val)])
  }
  list(train = train, val = val, test = test)
}

lgb_scaler_from_x <- function(x, min_sd = 1e-12) {
  if (!is.numeric(min_sd) || length(min_sd) != 1L ||
      !is.finite(min_sd) || min_sd <= 0) {
    rlang::abort("`min_sd` must be a positive finite number.")
  }
  counts <- colSums(!is.na(x))
  mean <- colSums(x, na.rm = TRUE) / pmax(counts, 1)
  variance <- colSums(x^2, na.rm = TRUE) / pmax(counts, 1) - mean^2
  sd <- sqrt(pmax(variance, 0))
  sd[!is.finite(sd) | sd < min_sd] <- 1
  list(mean = mean, sd = sd)
}


lgb_scale_x <- function(x, scaler, na_fill = 0) {
  if (is.null(scaler)) {
    return(x)
  }
  x <- sweep(x, 2L, scaler$mean, "-")
  x <- sweep(x, 2L, scaler$sd, "/")
  x[is.na(x)] <- na_fill
  x
}

#' Fit LightGBM with preprocess, three-way split, and standard scaling
#'
#' Mirrors the reference `gbdt_float_fit()` workflow while keeping the
#' DuckDB/C++ streaming ingestion.
#'
#' @param path Single Parquet file.
#' @param target Target column.
#' @param features Feature columns; `NULL` uses the preprocessed numeric
#'   columns except `target` and `split_col`.
#' @param preprocess Optional per-batch data-frame transform.
#' @param split_col Column used for a time/ordered split.
#' @param split_val Thresholds for `split_col`, or proportions when
#'   `split_col` is `NULL`.
#' @param train_prop Training fraction when `split_col` is `NULL`.
#' @param val_prop Validation fraction when `split_col` is `NULL`.
#' @param scale `"standard"` or `"none"`.
#' @param nrounds Boosting rounds.
#' @param learning_rate Learning rate.
#' @param num_leaves Maximum leaves.
#' @param min_data_in_leaf Minimum data per leaf.
#' @param feature_fraction Column subsampling.
#' @param bagging_fraction Row subsampling.
#' @param bagging_freq Bagging frequency.
#' @param max_bin Maximum bins.
#' @param min_data_in_bin Minimum data per bin.
#' @param num_threads Threads.
#' @param sample_size Rows or fraction used to fit the bin mappers and, when
#'   `scale = "standard"`, the scaler (capped at one million rows).
#' @param min_sd Standard-deviation floor for `scale = "standard"`.
#' @param na_fill Value written after scaling for missing cells.
#' @param seed Random seed.
#' @param large_data Logical; when `TRUE` and `single_pass` is not supplied,
#'   use the larger-data sample scaler.
#' @param single_pass Logical; defaults to `FALSE` for the exact full-train
#'   scaler. Set to `TRUE` to use the sample scaler.
#'
#' @return An object of class `lgb_tabred_model`.
#' @export
lgb_fit_tabred <- function(
    path, target,
    features = NULL,
    preprocess = lgb_tabred_preprocess,
    split_col = NULL,
    split_val = NULL,
    train_prop = 0.8,
    val_prop = 0,
    scale = c("standard", "none"),
    nrounds = 300L,
    learning_rate = 0.1,
    num_leaves = 31L,
    min_data_in_leaf = 20L,
    feature_fraction = 1,
    bagging_fraction = 1,
    bagging_freq = 0L,
    max_bin = 255L,
    min_data_in_bin = 3L,
    num_threads = 0L,
    sample_size = 50000L,
    min_sd = 1e-12,
    na_fill = 0,
    seed = 42L,
    large_data = FALSE,
    single_pass = FALSE) {
  scale <- rlang::arg_match(scale)
  if (!is.numeric(nrounds) || length(nrounds) != 1L || !is.finite(nrounds) || nrounds < 1) {
    rlang::abort("`nrounds` must be a positive integer.")
  }
  if (!is.numeric(max_bin) || length(max_bin) != 1L || !is.finite(max_bin) || max_bin < 2) {
    rlang::abort("`max_bin` must be a finite number >= 2.")
  }
  if (!is.numeric(min_data_in_bin) || length(min_data_in_bin) != 1L || !is.finite(min_data_in_bin) || min_data_in_bin < 1) {
    rlang::abort("`min_data_in_bin` must be a finite number >= 1.")
  }
  lgb_check_flag(large_data, "large_data")
  if (isTRUE(large_data) && missing(single_pass)) {
    single_pass <- TRUE
  }
  lgb_check_flag(single_pass, "single_pass")
  lgb_init()
  if (dir.exists(path)) {
    rlang::abort("`lgb_fit_tabred()` supports single Parquet files; directory sources are not supported.")
  }
  args <- as.list(environment())
  args$large_data <- NULL
  rlang::exec(lgb_fit_tabred_arrow, !!!args)
}

#' Predict with a `lgb_tabred_model`
#'
#' @param object A model returned by [lgb_fit_tabred()].
#' @param new_data A data frame.
#'
#' @return A numeric vector.
#' @export
lgb_predict_tabred <- function(object, new_data) {
  if (!inherits(object, "lgb_tabred_model")) {
    rlang::abort("`object` must be a `lgb_tabred_model`.")
  }
  data <- lgb_apply_preprocess(new_data, object$preprocess)
  x <- as.matrix(data[, object$features, drop = FALSE])
  x <- lgb_scale_x(x, object$scaler, na_fill = object$na_fill %||% 0)
  cpp_predict(object$booster, x)
}

#' @export
print.lgb_tabred_model <- function(x, ...) {
  cat("<lgb_tabred_model>\n")
  cat("  rows:    ", x$n, "\n", sep = "")
  cat("  features:", x$nfeat, "\n", sep = " ")
  cat("  train/val/test: ", x$ntrain, "/", x$nval, "/", x$ntest, "\n", sep = "")
  cat("  val RMSE: ", round(x$val_rmse, 5), " MAE: ", round(x$val_mae, 5), "\n", sep = "")
  cat("  test RMSE:", round(x$test_rmse, 5), " MAE: ", round(x$test_mae, 5), "\n", sep = " ")
  invisible(x)
}
