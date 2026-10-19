#' @title Streaming LightGBM Classification Learner
#'
#' @description
#' A [mlr3::LearnerClassif] for binary and multiclass LightGBM models backed
#' by the same Arrow/C++ streaming path as [LearnerRegrLGBStream].  Tasks
#' produced by `mlr3pipelines` are converted to a temporary Parquet file at
#' the learner boundary.
#'
#' @export
LearnerClassifLGBStream <- R6::R6Class(
  "LearnerClassifLGBStream",
  inherit = mlr3::LearnerClassif,
  public = list(
    #' @description Create the learner.
    initialize = function() {
      ps <- paradox::ps(
        nrounds = paradox::p_int(lower = 1L, upper = 100000L, default = 100L),
        learning_rate = paradox::p_dbl(lower = 1e-5, upper = 1, default = 0.1),
        num_leaves = paradox::p_int(lower = 2L, upper = 131072L, default = 31L),
        min_data_in_leaf = paradox::p_int(lower = 1L, upper = 1000000L, default = 20L),
        feature_fraction = paradox::p_dbl(lower = 0.01, upper = 1, default = 1),
        bagging_fraction = paradox::p_dbl(lower = 0.01, upper = 1, default = 1),
        bagging_freq = paradox::p_int(lower = 0L, upper = 100L, default = 0L),
        max_bin = paradox::p_int(lower = 2L, upper = 1023L, default = 255L),
        min_data_in_bin = paradox::p_int(lower = 1L, upper = 1000000L, default = 3L),
        num_threads = paradox::p_int(lower = 0L, upper = 128L, default = 0L),
        sample_size = paradox::p_dbl(lower = 1e-4, upper = 1e9, default = 50000),
        batch_size = paradox::p_dbl(lower = 1e-4, upper = 1e9, default = 32768),
        predict_batch_size = paradox::p_dbl(lower = 1e-4, upper = 1e9, default = 250000)
      )
      super$initialize(
        id = "classif.lgbstream",
        label = "Streaming LightGBM Classification",
        param_set = ps,
        feature_types = c(
          "numeric", "integer", "logical", "factor", "ordered",
          "character", "POSIXct"
        ),
        predict_types = c("response", "prob"),
        properties = c("twoclass", "multiclass", "missings", "weights"),
        packages = c(
          "lgbstream", "lightgbm", "duckdb", "DBI", "data.table"
        )
      )
    },

    #' @description Feature importance.
    #' @return A named numeric vector.
    importance = function() {
      if (is.null(self$model)) {
        rlang::abort("Train the learner before extracting importance.")
      }
      imp <- cpp_importance(self$model$booster)
      stats::setNames(imp, self$model$features)
    }
  ),
  private = list(
    .train = function(task) {
      weights <- tryCatch({
        if (length(task$col_roles$weight)) task$weights else NULL
      }, error = function(e) NULL)
      backend <- task$backend
      converted_path <- NULL
      preprocess <- list(
        converted = FALSE, levels = NULL, types = NULL,
        backend = class(backend)[[1L]]
      )
      needs_conversion <- !inherits(backend, "DataBackendParquet") ||
        any(task$feature_types$type %in% c(
          "factor", "ordered", "character", "logical", "POSIXct"
        ))
      if (needs_conversion) {
        converted <- as_task_parquet(task)
        preprocess <- attr(converted, "lgbstream")
        preprocess$converted <- TRUE
        converted_path <- preprocess$parquet
        task <- converted
        backend <- task$backend
      }
      on.exit(
        if (!is.null(converted_path)) unlink(converted_path),
        add = TRUE
      )

      pv <- self$param_set$values
      nrounds <- pv$nrounds %||% 100L
      learning_rate <- pv$learning_rate %||% 0.1
      num_leaves <- pv$num_leaves %||% 31L
      min_data_in_leaf <- pv$min_data_in_leaf %||% 20L
      feature_fraction <- pv$feature_fraction %||% 1
      bagging_fraction <- pv$bagging_fraction %||% 1
      bagging_freq <- pv$bagging_freq %||% 0L
      max_bin <- pv$max_bin %||% 255L
      min_data_in_bin <- pv$min_data_in_bin %||% 3L
      num_threads <- pv$num_threads %||% 1L
      sample_size <- pv$sample_size %||% 50000
      batch_size <- pv$batch_size %||% 32768

      class_names <- task$class_names
      target <- task$target_names[[1L]]
      y <- match(
        as.character(backend$data(backend$rownames, target)[[1L]]),
        class_names
      ) - 1L
      num_class <- length(class_names)
      objective <- if (num_class == 2L) "binary" else "multiclass"
      metric <- if (num_class == 2L) "binary_logloss" else "multi_logloss"

      obj <- lgb_stream_dataset(
        backend$path, task$feature_names, as.numeric(y),
        sample_rows = task$row_ids,
        weights = weights,
        sample_size = sample_size,
        batch_size = batch_size,
        max_bin = max_bin,
        min_data_in_bin = min_data_in_bin
      )
      ds_train <- if (length(task$row_ids) == obj$n &&
                      identical(sort(task$row_ids), seq_len(obj$n))) {
        obj$dataset
      } else {
        cpp_subset(obj$dataset, task$row_ids - 1L)
      }

      booster_params <- sprintf(
        paste0(
          "objective=%s metric=%s learning_rate=%s num_leaves=%d ",
          "min_data_in_leaf=%d feature_fraction=%s bagging_fraction=%s ",
          "bagging_freq=%d num_threads=%d verbosity=-1%s"
        ),
        objective, metric, learning_rate, num_leaves, min_data_in_leaf,
        feature_fraction, bagging_fraction, bagging_freq, num_threads,
        if (num_class > 2L) sprintf(" num_class=%d", num_class) else ""
      )
      fit <- cpp_train(ds_train, nrounds, booster_params)
      list(
        booster = fit$booster,
        features = task$feature_names,
        class_names = class_names,
        num_class = num_class,
        preprocess = preprocess
      )
    },

    .predict = function(task) {
      rows <- task$row_ids
      truth <- task$truth(rows)
      class_names <- self$model$class_names
      num_class <- self$model$num_class
      predict_batch_size <- self$param_set$values$predict_batch_size %||% 250000
      chunk_size <- lgb_size(predict_batch_size, length(rows), "predict_batch_size")

      n <- length(rows)
      chunks <- split(
        seq_len(n),
        ceiling(seq_len(n) / chunk_size)
      )
      get_x <- function(ch) {
        data <- as.data.frame(task$data(
          rows = rows[ch], cols = task$feature_names
        ))
        data <- lgb_encode_columns(
          data, levels = self$model$preprocess$levels
        )$data
        as.matrix(data[, self$model$features, drop = FALSE])
      }

      if (num_class == 2L) {
        prob1 <- numeric(n)
        for (ch in chunks) {
          prob1[ch] <- cpp_predict(self$model$booster, get_x(ch), 0L, 1L)
        }
        prob <- cbind(1 - prob1, prob1)
        colnames(prob) <- class_names
        response <- class_names[ifelse(prob1 >= 0.5, 2L, 1L)]
      } else {
        raw <- numeric(n * num_class)
        for (ch in chunks) {
          raw_chunk <- cpp_predict(
            self$model$booster, get_x(ch), 0L, num_class
          )
          start <- (ch[[1L]] - 1L) * num_class
          raw[start + seq_len(length(ch) * num_class)] <- raw_chunk
        }
        prob <- matrix(raw, nrow = n, byrow = TRUE)
        colnames(prob) <- class_names
        response <- class_names[max.col(prob, ties.method = "first")]
      }

      mlr3::PredictionClassif$new(
        row_ids = rows,
        truth = truth,
        response = response,
        prob = prob
      )
    }
  )
)
