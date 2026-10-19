#!/usr/bin/env Rscript

# Large-data classification benchmark.
#
# Environment:
#   LGBSTREAM_DATA         input Parquet
#   LGBSTREAM_TARGET       binary/multiclass target column
#   LGBSTREAM_NROUNDS      boosting rounds (default: 300)
#   LGBSTREAM_THREADS      LightGBM threads (default: 16)
#   LGBSTREAM_SAMPLE_SIZE  bin sample rows (default: 50000)
#   LGBSTREAM_POSITIVE     optional positive class

suppressPackageStartupMessages({
  library(mlr3)
  library(lgbstream)
})

env <- function(name, default = NULL) {
  value <- Sys.getenv(name, unset = "")
  if (nzchar(value)) value else default
}

path <- env("LGBSTREAM_DATA")
target <- env("LGBSTREAM_TARGET", "b001")
if (is.null(path) || !file.exists(path)) {
  stop("Set LGBSTREAM_DATA to a Parquet file.", call. = FALSE)
}
nrounds <- as.integer(env("LGBSTREAM_NROUNDS", "300"))
threads <- as.integer(env("LGBSTREAM_THREADS", "16"))
sample_size <- as.integer(env("LGBSTREAM_SAMPLE_SIZE", "50000"))
positive <- env("LGBSTREAM_POSITIVE", NULL)

task <- as_task_parquet(
  path, target,
  task_type = "classif", positive = positive
)
learner <- lrn(
  "classif.lgbstream",
  nrounds = nrounds,
  num_threads = threads,
  sample_size = sample_size
)

tm_train <- system.time(learner$train(task))
tm_pred <- system.time(pred <- learner$predict(task))

cat(sprintf(
  paste0(
    "CLASSIF_RESULT rows=%d classes=%s train_sec=%.3f ",
    "predict_sec=%.3f pred_rows=%d\n"
  ),
  task$nrow,
  paste(task$class_names, collapse = ","),
  tm_train[["elapsed"]],
  tm_pred[["elapsed"]],
  length(pred$response)
))
