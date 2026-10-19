#' Fit a LightGBM classification model on a Parquet file
#'
#' A tidy wrapper around [LearnerClassifLGBStream].  Training streams Parquet
#' through Arrow; no full feature matrix is created in R.
#'
#' @param path Parquet file or directory.
#' @param target Target column.
#' @param features Feature columns; defaults to all non-target columns.
#' @param positive Positive class for binary classification.
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
#' @return An object of class `lgb_classif_model`.
#' @export
lgb_fit_classif <- function(path, target, features = NULL, positive = NULL,
                            nrounds = 100L, learning_rate = 0.05,
                            num_leaves = 127L, min_data_in_leaf = 100L,
                            feature_fraction = 0.8, bagging_fraction = 0.8,
                            bagging_freq = 1L, max_bin = 255L,
                            num_threads = 0L, sample_size = 50000L,
                            batch_size = 32768L, ...) {
  task <- as_task_parquet(
    path, target, features,
    task_type = "classif", positive = positive
  )
  learner <- LearnerClassifLGBStream$new()
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
      target = target,
      class_names = task$class_names,
      positive = positive
    ),
    class = "lgb_classif_model"
  )
}

#' Predict with a `lgb_classif_model`
#'
#' @param object A model returned by [lgb_fit_classif()].
#' @param new_data A data frame or path to a Parquet file.
#'
#' @return An [mlr3::PredictionClassif].
#' @export
lgb_predict_classif <- function(object, new_data) {
  if (!inherits(object, "lgb_classif_model")) {
    rlang::abort("`object` must be a `lgb_classif_model`.")
  }
  if (is.character(new_data)) {
    task <- as_task_parquet(
      new_data, object$target, object$features,
      task_type = "classif", positive = object$positive
    )
    return(object$learner$predict(task))
  }

  new_data <- lgb_as_data_frame(new_data, "new_data")
  missing <- setdiff(object$features, colnames(new_data))
  if (length(missing)) {
    rlang::abort(sprintf(
      "Missing feature column(s): %s.",
      paste(missing, collapse = ", ")
    ))
  }
  task <- mlr3::as_task_classif(
    new_data[, c(object$features, object$target), drop = FALSE],
    target = object$target,
    positive = object$positive
  )
  object$learner$predict(task)
}

#' @export
print.lgb_classif_model <- function(x, ...) {
  cat("<lgb_classif_model>\n")
  cat("  target:   ", x$target, "\n", sep = "")
  cat("  features: ", length(x$features), "\n", sep = "")
  cat("  classes:  ", paste(x$class_names, collapse = ", "), "\n", sep = "")
  cat("  rows:     ", x$task$nrow, "\n", sep = "")
  invisible(x)
}
