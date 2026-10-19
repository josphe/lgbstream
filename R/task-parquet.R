lgb_encode_columns <- function(data, levels = NULL) {
  out <- data
  enc <- list()
  typ <- list()
  for (nm in names(out)) {
    x <- out[[nm]]
    if (is.factor(x) || is.character(x)) {
      lev <- if (!is.null(levels) && !is.null(levels[[nm]])) {
        levels[[nm]]
      } else if (is.factor(x)) {
        levels(x)
      } else {
        sort(unique(x))
      }
      enc[[nm]] <- lev
      out[[nm]] <- match(as.character(x), lev)
    } else if (is.logical(x)) {
      out[[nm]] <- as.integer(x)
    } else if (inherits(x, c("Date", "POSIXct", "integer64"))) {
      typ[[nm]] <- class(x)[[1L]]
      out[[nm]] <- as.numeric(x)
    }
  }
  list(data = out, levels = enc, types = typ)
}

#' Convert an mlr3 task to a Parquet-backed task
#'
#' This is the bridge for mlr3 pipelines: the already-preprocessed task is
#' written to a temporary Parquet file, factor levels, column types and task
#' metadata are retained, and the result can be consumed by
#' [LearnerRegrLGBStream].
#'
#' @param task An [mlr3::Task].
#' @param path Optional Parquet output path.
#' @param levels Optional factor levels used for prediction encoding.
#'
#' @return An `mlr3` task with `lgbstream` metadata in its attributes.
#' @keywords internal
task_to_parquet <- function(task, path = tempfile(fileext = ".parquet"),
                            levels = NULL) {
  data <- task$data()
  if ("..row_id" %in% names(data)) {
    data[["..row_id"]] <- NULL
  }
  target <- task$target_names
  target_values <- data[[target]]
  unsupported <- vapply(
    data,
    function(x) is.list(x) && !is.data.frame(x),
    logical(1L)
  )
  if (any(unsupported)) {
    rlang::abort(sprintf(
      "Unsupported list column(s) after preprocessing: %s.",
      paste(names(data)[unsupported], collapse = ", ")
    ))
  }

  encoded <- lgb_encode_columns(data, levels = levels)
  data <- encoded$data
  data[[target]] <- if (identical(task$task_type, "classif")) {
    as.character(target_values)
  } else {
    target_values
  }
  target_levels <- if (identical(task$task_type, "classif")) {
    levels(target_values)
  } else {
    NULL
  }
  if (!identical(task$task_type, "classif") && !is.numeric(data[[target]])) {
    rlang::abort("The target must be numeric for regression.")
  }
  lgb_write_parquet(data, path)

  backend <- DataBackendParquet$new(
    path,
    target = target,
    features = intersect(task$feature_names, names(data)),
    target_levels = target_levels
  )
  converted <- if (identical(task$task_type, "classif")) {
    mlr3::TaskClassif$new(id = task$id, backend = backend, target = target)
  } else {
    mlr3::TaskRegr$new(id = task$id, backend = backend, target = target)
  }

  old_ids <- task$row_ids
  new_ids <- seq_along(old_ids)
  row_roles <- lapply(task$row_roles, function(ids) {
    out <- new_ids[match(ids, old_ids)]
    out[!is.na(out)]
  })
  try(converted$col_roles <- task$col_roles, silent = TRUE)
  try(converted$row_roles <- row_roles, silent = TRUE)
  try(backend$properties <- task$properties, silent = TRUE)

  info <- list(
    col_info = task$col_info,
    col_roles = task$col_roles,
    row_roles = task$row_roles,
    properties = task$properties,
    target = task$target_names,
    task_type = task$task_type,
    features = converted$feature_names,
    id = task$id,
    row_ids = task$row_ids,
    backend = class(task$backend),
    levels = encoded$levels,
    types = encoded$types,
    parquet = path
  )
  attr(converted, "lgbstream") <- info
  converted
}
