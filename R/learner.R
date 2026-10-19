#' @title Gradient Boosting Learner with Streaming Data Access
#'
#' @description
#' A [mlr3::LearnerRegr] for LightGBM that reads a
#' [DataBackendParquet] backed by DuckDB and streams batches into the
#' LightGBM C API.  The binned dataset is built in C++ with
#' `LGBM_DatasetCreateFromSampledColumn()` + `LGBM_DatasetPushRowsWithMetadata()`;
#' no CSV and no full feature matrix are materialised.
#'
#' For other backends, such as the in-memory output of `mlr3pipelines`, the
#' learner converts the current task to a temporary Parquet file once per
#' train or predict call, retaining factor levels, column roles and task
#' metadata.
#'
#' @export
LearnerRegrLGBStream <- R6::R6Class(
  "LearnerRegrLGBStream",
  inherit = mlr3::LearnerRegr,
  public = list(
    #' @description Create the learner.
    initialize = function() {
      ps <- paradox::ps(
        nrounds = paradox::p_int(lower = 1L, upper = 100000L, default = 100L),
        learning_rate = paradox::p_dbl(lower = 1e-5, upper = 1, default = 0.05),
        num_leaves = paradox::p_int(lower = 2L, upper = 131072L, default = 127L),
        min_data_in_leaf = paradox::p_int(lower = 1L, upper = 1000000L, default = 100L),
        feature_fraction = paradox::p_dbl(lower = 0.01, upper = 1, default = 0.8),
        bagging_fraction = paradox::p_dbl(lower = 0.01, upper = 1, default = 0.8),
        bagging_freq = paradox::p_int(lower = 0L, upper = 100L, default = 1L),
        max_bin = paradox::p_int(lower = 2L, upper = 1023L, default = 255L),
        min_data_in_bin = paradox::p_int(lower = 1L, upper = 1000000L, default = 3L),
        num_threads = paradox::p_int(lower = 0L, upper = 128L, default = 0L),
        sample_size = paradox::p_dbl(lower = 1e-4, upper = 1e9, default = 50000),
        batch_size = paradox::p_dbl(lower = 1e-4, upper = 1e9, default = 32768),
        predict_batch_size = paradox::p_dbl(lower = 1e-4, upper = 1e9, default = 250000)
      )
      super$initialize(
        id = "regr.lgbstream",
        label = "Streaming LightGBM",
        param_set = ps,
        feature_types = c(
          "numeric", "integer", "logical", "factor", "ordered",
          "character", "POSIXct"
        ),
        predict_types = "response",
        properties = c("missings", "weights"),
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

      pv <- self$param_set$values
      nrounds <- pv$nrounds %||% 100L
      learning_rate <- pv$learning_rate %||% 0.05
      num_leaves <- pv$num_leaves %||% 127L
      min_data_in_leaf <- pv$min_data_in_leaf %||% 100L
      feature_fraction <- pv$feature_fraction %||% 0.8
      bagging_fraction <- pv$bagging_fraction %||% 0.8
      bagging_freq <- pv$bagging_freq %||% 1L
      max_bin <- pv$max_bin %||% 255L
      min_data_in_bin <- pv$min_data_in_bin %||% 3L
      num_threads <- pv$num_threads %||% 0L
      sample_size <- pv$sample_size %||% 50000
      batch_size <- pv$batch_size %||% 32768

      in_memory <- !inherits(backend, "DataBackendParquet") &&
        lgb_in_memory_allowed(task)

      converted_path <- NULL
      preprocess <- list(
        converted = FALSE,
        in_memory = FALSE,
        levels = NULL,
        types = NULL,
        backend = class(backend)[[1L]]
      )

      if (in_memory) {
        mem <- lgb_in_memory_dataset(
          task, task$feature_names,
          sample_size = sample_size,
          batch_size = batch_size,
          max_bin = max_bin,
          min_data_in_bin = min_data_in_bin,
          weights = weights
        )
        preprocess$in_memory <- TRUE
        preprocess$levels <- mem$levels
        preprocess$types <- mem$types
        ds_train <- mem$dataset
        n_total <- mem$n
      } else {
        needs_conversion <- !inherits(backend, "DataBackendParquet") ||
          any(task$feature_types$type %in% c(
            "factor", "ordered", "character", "logical", "POSIXct"
          ))
        if (needs_conversion) {
          converted <- as_task_parquet(task)
          preprocess <- attr(converted, "lgbstream")
          preprocess$converted <- TRUE
          preprocess$in_memory <- FALSE
          converted_path <- preprocess$parquet
          task <- converted
          backend <- task$backend
        }
        on.exit(
          if (!is.null(converted_path)) unlink(converted_path),
          add = TRUE
        )

        y <- backend$data(backend$rownames, task$target_names)[[1L]]
        obj <- lgb_stream_dataset(
          backend$path, task$feature_names, y,
          sample_rows = task$row_ids,
          weights = weights,
          sample_size = sample_size,
          batch_size = batch_size,
          max_bin = max_bin,
          min_data_in_bin = min_data_in_bin
        )
        n_total <- obj$n
        if (length(task$row_ids) == obj$n &&
            identical(sort(task$row_ids), seq_len(obj$n))) {
          ds_train <- obj$dataset
        } else {
          ds_train <- cpp_subset(obj$dataset, task$row_ids - 1L)
        }
      }

      booster_params <- sprintf(
        paste0(
          "objective=regression metric=rmse learning_rate=%s num_leaves=%d ",
          "min_data_in_leaf=%d feature_fraction=%s bagging_fraction=%s ",
          "bagging_freq=%d num_threads=%d verbosity=-1"
        ),
        learning_rate, num_leaves, min_data_in_leaf,
        feature_fraction, bagging_fraction, bagging_freq, num_threads
      )
      fit <- cpp_train(ds_train, nrounds, booster_params)
      list(
        booster = fit$booster,
        features = task$feature_names,
        n = n_total,
        preprocess = preprocess
      )
    },

    .predict = function(task) {
      rows <- task$row_ids
      predict_batch_size <- self$param_set$values$predict_batch_size %||% 250000
      chunk_size <- lgb_size(predict_batch_size, length(rows), "predict_batch_size")

      if (isTRUE(self$model$preprocess$in_memory) &&
          !inherits(task$backend, "DataBackendParquet")) {
        data <- lgb_in_memory_data(
          task, self$model$features,
          levels = self$model$preprocess$levels
        )
        chunks <- split(
          seq_len(nrow(data$x)),
          ceiling(seq_len(nrow(data$x)) / chunk_size)
        )
        pred <- numeric(nrow(data$x))
        for (ch in chunks) {
          pred[ch] <- cpp_predict(
            self$model$booster, data$x[ch, , drop = FALSE]
          )
        }
        return(mlr3::PredictionRegr$new(
          row_ids = rows,
          truth = task$truth(rows),
          response = pred
        ))
      }

      needs_conversion <- isTRUE(self$model$preprocess$converted) ||
        !inherits(task$backend, "DataBackendParquet") ||
        any(task$feature_types$type %in% c(
          "factor", "ordered", "character", "logical", "POSIXct"
        ))
      if (needs_conversion) {
        converted <- task_to_parquet(
          task,
          levels = self$model$preprocess$levels
        )
        path <- attr(converted, "lgbstream")$parquet
        on.exit(unlink(path), add = TRUE)
        chunks <- split(
          seq_len(converted$nrow),
          ceiling(seq_len(converted$nrow) / chunk_size)
        )
        pred <- numeric(converted$nrow)
        for (ch in chunks) {
          x <- as.matrix(converted$data(
            rows = ch, cols = converted$feature_names
          ))
          pred[ch] <- cpp_predict(self$model$booster, x)
        }
        return(mlr3::PredictionRegr$new(
          row_ids = rows,
          truth = task$truth(rows),
          response = pred
        ))
      }

      chunks <- split(rows, ceiling(seq_along(rows) / chunk_size))
      pred <- numeric(length(rows))
      for (ch in chunks) {
        x <- as.matrix(task$data(rows = ch, cols = task$feature_names))
        pred[match(ch, rows)] <- cpp_predict(self$model$booster, x)
      }
      mlr3::PredictionRegr$new(
        row_ids = rows,
        truth = task$truth(rows),
        response = pred
      )
    }
  )
)
