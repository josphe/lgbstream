#!/usr/bin/env Rscript

# Standard large-data check for lgbstream.
#
# Usage:
#   LGBSTREAM_PROFILE=tabred \
#   LGBSTREAM_DATA=/path/weather.parquet \
#   Rscript large-data.R
#
# Environment:
#   LGBSTREAM_PROFILE       tabred (default) or random
#   LGBSTREAM_DATA          input Parquet (required if no positional arg)
#   LGBSTREAM_TARGET        target column (default: fact_temperature)
#   LGBSTREAM_SPLIT_COL     split column (default: fact_time for tabred)
#   LGBSTREAM_NROUNDS       boosting rounds (default: 3)
#   LGBSTREAM_THREADS       LightGBM threads (default: 16)
#   LGBSTREAM_SAMPLE_SIZE   bin/scaler sample rows (default: 50000)
#   LGBSTREAM_BATCH_SIZE    Arrow push batch rows (default: 32768)
#   LGBSTREAM_SINGLE_PASS   TRUE to use the sample scaler
#   LGBSTREAM_EXPECT_ROWS   optional expected row count
#   LGBSTREAM_EXPECT_RMSE   optional expected test RMSE
#   LGBSTREAM_RMSE_TOL      optional RMSE tolerance (default: 0.01)

args <- commandArgs(trailingOnly = TRUE)
env <- function(name, default = NULL) {
  value <- Sys.getenv(name, unset = "")
  if (nzchar(value)) value else default
}
num_env <- function(name, default) {
  value <- env(name)
  if (is.null(value)) return(default)
  as.numeric(value)
}
int_env <- function(name, default) {
  as.integer(num_env(name, default))
}

path <- if (length(args)) args[[1]] else env("LGBSTREAM_DATA")
if (is.null(path) || !file.exists(path)) {
  stop("Set LGBSTREAM_DATA or pass a Parquet path.", call. = FALSE)
}
profile <- env("LGBSTREAM_PROFILE", "tabred")
target <- env("LGBSTREAM_TARGET", if (profile == "tabred") "fact_temperature" else "y")
nrounds <- int_env("LGBSTREAM_NROUNDS", 3L)
threads <- int_env("LGBSTREAM_THREADS", 16L)
sample_size <- int_env("LGBSTREAM_SAMPLE_SIZE", 50000L)
single_pass <- isTRUE(as.logical(env("LGBSTREAM_SINGLE_PASS", "FALSE")))

if (profile == "tabred") {
  split_col <- env("LGBSTREAM_SPLIT_COL", "fact_time")
  split_val <- c(
    as.numeric(as.POSIXct("2023-06-01", tz = "UTC")),
    as.numeric(as.POSIXct("2023-07-01", tz = "UTC"))
  )
  train_prop <- 0.8
  val_prop <- 0
} else if (profile == "random") {
  split_col <- NULL
  split_val <- NULL
  train_prop <- 0.8
  val_prop <- 0.1
} else {
  stop("LGBSTREAM_PROFILE must be 'tabred' or 'random'.", call. = FALSE)
}

peak_hwm_gb <- function() {
  status <- tryCatch(readLines("/proc/self/status"), error = function(e) character())
  line <- grep("^VmHWM:", status, value = TRUE)
  if (!length(line)) return(NA_real_)
  as.numeric(sub("\\D+", "", line[[1L]])) / 1024^2
}

message("lgbstream: ", as.character(utils::packageVersion("lgbstream")))
message("data: ", normalizePath(path))
message("profile: ", profile, " target: ", target, " nrounds: ", nrounds)

tm <- system.time({
  fit <- lgbstream::lgb_fit_tabred(
    path = path,
    target = target,
    preprocess = lgbstream::lgb_tabred_preprocess,
    split_col = split_col,
    split_val = split_val,
    train_prop = train_prop,
    val_prop = val_prop,
    scale = "standard",
    nrounds = nrounds,
    num_threads = threads,
    sample_size = sample_size,
    single_pass = single_pass
  )
})

hwm <- peak_hwm_gb()
expected_rows <- num_env("LGBSTREAM_EXPECT_ROWS", NA_real_)
if (is.finite(expected_rows) && fit$n != expected_rows) {
  stop(sprintf("row count %s != expected %s", fit$n, expected_rows), call. = FALSE)
}
if (!is.finite(fit$test_rmse) || !is.finite(fit$val_rmse)) {
  stop("non-finite validation/test RMSE", call. = FALSE)
}
expected_rmse <- num_env("LGBSTREAM_EXPECT_RMSE", NA_real_)
if (is.finite(expected_rmse)) {
  tol <- num_env("LGBSTREAM_RMSE_TOL", 0.01)
  if (abs(fit$test_rmse - expected_rmse) > tol) {
    stop(sprintf(
      "test RMSE %.4f outside %.4f +/- %.4f",
      fit$test_rmse, expected_rmse, tol
    ), call. = FALSE)
  }
}

cat(sprintf(
  paste0(
    "RESULT rows=%d ntrain=%d nval=%d ntest=%d ",
    "elapsed_sec=%.3f peak_hwm_gb=%.3f val_rmse=%.6f test_rmse=%.6f\n"
  ),
  as.integer(fit$n), as.integer(fit$ntrain), as.integer(fit$nval), as.integer(fit$ntest),
  tm[["elapsed"]], hwm, fit$val_rmse, fit$test_rmse
))
