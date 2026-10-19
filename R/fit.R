#' Fit a gradient boosting model on a Parquet file
#'
#' A tidy wrapper around [LearnerRegrLGBStream].  Training reads Parquet
#' through the C++ Arrow `RecordBatchReader` and pushes each batch to the
#' LightGBM C API; no CSV and no full feature matrix are created.
#'
#' @param path Parquet file or directory.
#' @param target Target column.
#' @param features Feature columns; defaults to all non-target columns.
#' @param nrounds Number of boosting rounds.
#' @param learning_rate Learning rate.
#' @param num_leaves Maximum number of leaves.
#' @param min_data_in_leaf Minimum data per leaf.
#' @param feature_fraction Column subsampling.
#' @param bagging_fraction Row subsampling.
#' @param bagging_freq Bagging frequency.
#' @param max_bin Maximum number of bins.
#' @param num_threads Number of threads.
#' @param sample_size Rows used to fit the bin mappers.
#' @param batch_size Rows per batch for the DuckDB fallback.
#' @param ... Additional learner parameters.
#'
#' @return An object of class `lgb_model`.
#' @export
lgb_fit <- function(path, target, features = NULL,
                    nrounds = 100L, learning_rate = 0.05,
                    num_leaves = 127L, min_data_in_leaf = 100L,
                    feature_fraction = 0.8, bagging_fraction = 0.8,
                    bagging_freq = 1L, max_bin = 255L, num_threads = 0L,
                    sample_size = 50000L, batch_size = 32768L,
                    ...) {
  task <- as_task_parquet(path, target, features)

  learner <- LearnerRegrLGBStream$new()
  learner$param_set$set_values(
    nrounds = nrounds,
    learning_rate = learning_rate,
    num_leaves = num_leaves,
    min_data_in_leaf = min_data_in_leaf,
    feature_fraction = feature_fraction,
    bagging_fraction = bagging_fraction,
    bagging_freq = bagging_freq,
    max_bin = max_bin,
    num_threads = num_threads,
    sample_size = sample_size,
    batch_size = batch_size,
    ...
  )
  learner$train(task)

  structure(
    list(
      learner = learner,
      task = task,
      features = task$feature_names,
      target = target
    ),
    class = "lgb_model"
  )
}

#' Predict from a fitted `lgb_model`
#'
#' @param object An `lgb_model` returned by [lgb_fit()].
#' @param new_data A data frame, matrix, or path to a Parquet file.
#' @param batch_size Rows per prediction batch.
#'
#' @return A numeric vector.
#' @export
lgb_predict <- function(object, new_data, batch_size = 250000L) {
  if (!inherits(object, "lgb_model")) {
    rlang::abort("`object` must be a `lgb_model`.")
  }
  if (is.character(new_data)) {
    task <- as_task_parquet(new_data, object$target, object$features)
    return(object$learner$predict(task)$response)
  }

  new_data <- lgb_as_data_frame(new_data, "new_data")
  missing <- setdiff(object$features, colnames(new_data))
  if (length(missing)) {
    rlang::abort(sprintf(
      "`new_data` is missing feature column(s): %s.",
      paste(missing, collapse = ", ")
    ))
  }
  x <- as.matrix(new_data[, object$features, drop = FALSE])
  predict_chunk <- function(chunk) {
    cpp_predict(object$learner$model$booster, chunk)
  }

  limit <- lgb_size(batch_size, nrow(x), "batch_size")
  if (nrow(x) <= limit) {
    return(predict_chunk(x))
  }
  pieces <- split(seq_len(nrow(x)), ceiling(seq_len(nrow(x)) / limit))
  unlist(lapply(pieces, function(idx) predict_chunk(x[idx, , drop = FALSE])),
         use.names = FALSE)
}

#' @export
print.lgb_model <- function(x, ...) {
  cat("<lgb_model>\n")
  cat("  target:   ", x$target, "\n", sep = "")
  cat("  features: ", length(x$features), "\n", sep = "")
  cat("  rows:     ", x$task$nrow, "\n", sep = "")
  invisible(x)
}
