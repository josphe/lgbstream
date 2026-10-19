lgb_rows_where <- function(con, rid, rows) {
  if (length(rows) == 1L) {
    sprintf("%s = %d", rid, rows[[1L]])
  } else if (all(diff(rows) == 1L)) {
    sprintf("%s BETWEEN %d AND %d", rid, rows[[1L]], rows[[length(rows)]])
  } else {
    sprintf("%s IN (%s)", rid, paste(rows, collapse = ", "))
  }
}

#' Data backend for Parquet files
#'
#' A [mlr3::DataBackend] that keeps the data in DuckDB and reads only the
#' requested rows and columns.  Row ids are the physical row order of the
#' Parquet source (1-based).
#'
#' @export
DataBackendParquet <- R6::R6Class(
  "DataBackendParquet",
  inherit = mlr3::DataBackend,
  cloneable = FALSE,
  public = list(
    #' @field path Normalised Parquet path.
    path = NULL,
    #' @field feature_names Feature columns.
    feature_names = NULL,
    #' @field target Target column.
    target = NULL,
    #' @field target_levels Optional class levels for classification targets.
    target_levels = NULL,

    #' @description Create a new backend.
    #' @param path Parquet file or directory.
    #' @param target Target column.
    #' @param features Feature columns; defaults to all non-target columns.
    #' @param distinct_rows Rows or fraction used by `distinct()`.
    #' @param missings_rows Rows or fraction used by `missings()`.
    #' @param target_levels Optional class levels for classification targets.
    initialize = function(path, target, features = NULL,
                          distinct_rows = 10000L,
                          missings_rows = 10000L,
                          target_levels = NULL) {
      if (!file.exists(path) && !dir.exists(path)) {
        rlang::abort(sprintf("Parquet path does not exist: %s.", path))
      }
      path <- normalizePath(path, mustWork = TRUE)
      d <- lgb_duckdb_view(path)
      all_cols <- setdiff(d$schema$column_name, "..row_id")

      if (!target %in% all_cols) {
        rlang::abort(sprintf("Target column `%s` not found in %s.", target, path))
      }
      if (is.null(features)) {
        features <- setdiff(all_cols, target)
      }
      missing <- setdiff(features, all_cols)
      if (length(missing)) {
        rlang::abort(sprintf(
          "Unknown feature column(s): %s.",
          paste(missing, collapse = ", ")
        ))
      }

      private$.d <- d
      private$.distinct_rows <- distinct_rows
      private$.missings_rows <- missings_rows
      self$path <- path
      self$target <- target
      self$feature_names <- features
      self$target_levels <- target_levels
      super$initialize(data = NULL, primary_key = "..row_id")
    },

    #' @description Read a slice of the data.
    #' @param rows Row ids.
    #' @param cols Column names.
    data = function(rows, cols) {
      rows <- sort(unique(as.integer(rows)))
      cols <- intersect(cols, self$colnames)
      if (!length(cols)) {
        return(data.frame(row.names = seq_along(rows)))
      }
      if (!length(rows)) {
        out <- stats::setNames(rep(list(numeric()), length(cols)), cols)
        return(data.table::as.data.table(out))
      }

      con <- private$.d$con
      need <- setdiff(cols, "..row_id")
      select <- vapply(need, function(x) lgb_ident(con, x), character(1L))
      if ("..row_id" %in% cols) {
        select <- c(lgb_ident(con, "..row_id"), select)
      }
      where <- lgb_rows_where(con, lgb_ident(con, "..row_id"), rows)
      sql <- sprintf(
        "SELECT %s FROM %s WHERE %s",
        paste(select, collapse = ", "),
        lgb_ident(con, private$.d$view),
        where
      )
      out <- data.table::as.data.table(DBI::dbGetQuery(con, sql))
      data.table::setcolorder(out, cols)
      if (!is.null(self$target_levels) && self$target %in% cols) {
        out[[self$target]] <- factor(
          out[[self$target]], levels = self$target_levels
        )
      }
      out
    },

    #' @description First `n` rows.
    #' @param n Number of rows.
    head = function(n = 6L) {
      con <- private$.d$con
      sql <- sprintf(
        "SELECT * FROM %s LIMIT %d",
        lgb_ident(con, private$.d$view),
        as.integer(n)
      )
      out <- DBI::dbGetQuery(con, sql) |>
        data.table::as.data.table()
      if (!is.null(self$target_levels)) {
        out[[self$target]] <- factor(
          out[[self$target]], levels = self$target_levels
        )
      }
      out
    },

    #' @description Distinct values (sampled).
    #' @param rows Row ids.
    #' @param cols Column names.
    #' @param na_rm Remove missing values.
    distinct = function(rows, cols, na_rm = TRUE) {
      n <- if (is.null(rows)) private$.d$n else length(rows)
      limit <- lgb_size(private$.distinct_rows, n, "distinct_rows")
      rows <- if (is.null(rows)) {
        seq_len(min(private$.d$n, limit))
      } else {
        utils::head(rows, limit)
      }
      dat <- self$data(rows, cols)
      out <- lapply(cols, function(nm) {
        x <- dat[[nm]]
        if (is.factor(x)) x <- as.character(x)
        if (na_rm) x <- x[!is.na(x)]
        unique(x)
      })
      stats::setNames(out, cols)
    },

    #' @description Missing counts (sampled).
    #' @param rows Row ids.
    #' @param cols Column names.
    missings = function(rows, cols) {
      n <- if (is.null(rows)) private$.d$n else length(rows)
      limit <- lgb_size(private$.missings_rows, n, "missings_rows")
      rows <- utils::head(rows, limit)
      dat <- self$data(rows, cols)
      out <- vapply(cols, function(nm) sum(is.na(dat[[nm]])), integer(1L))
      stats::setNames(out, cols)
    }
  ),
  active = list(
    #' @field rownames All row ids.
    rownames = function() seq_len(private$.d$n),
    #' @field nrow Number of rows.
    nrow = function() private$.d$n,
    #' @field ncol Number of columns.
    ncol = function() length(self$colnames),
    #' @field colnames Column names, including the synthetic primary key.
    colnames = function() c("..row_id", self$target, self$feature_names)
  ),
  private = list(
    .d = NULL,
    .distinct_rows = NULL,
    .missings_rows = NULL,
    finalize = function() {
      if (!is.null(private$.d)) {
        try(DBI::dbDisconnect(private$.d$con, shutdown = TRUE), silent = TRUE)
        private$.d <- NULL
      }
    },
    .calculate_hash = function() {
      lgb_hash(self$path, self$target, self$feature_names)
    }
  )
)

#' Create an mlr3 task from a Parquet file
#'
#' @param path Parquet file, directory, or an [mlr3::Task].
#' @param target Target column; ignored when `path` is a task.
#' @param features Feature columns; defaults to all non-target columns.
#' @param id Task id.
#' @param distinct_rows Rows or fraction used by `distinct()`.
#' @param missings_rows Rows or fraction used by `missings()`.
#' @param task_type `"regr"` or `"classif"`.
#' @param positive Positive class for binary classification.
#'
#' @return An [mlr3::TaskRegr] or [mlr3::TaskClassif].
#' @export
as_task_parquet <- function(path, target = NULL, features = NULL, id = "parquet",
                            distinct_rows = 10000L,
                            missings_rows = 10000L,
                            task_type = c("regr", "classif"),
                            positive = NULL) {
  task_type <- rlang::arg_match(task_type)
  if (inherits(path, "Task")) {
    if (!is.null(target) || !is.null(features)) {
      rlang::abort("`target` and `features` are not used when `path` is a task.")
    }
    return(task_to_parquet(path))
  }
  if (is.null(target)) {
    rlang::abort("`target` is required when `path` is a Parquet path.")
  }

  target_levels <- NULL
  if (identical(task_type, "classif")) {
    d <- lgb_duckdb_view(path)
    on.exit(try(DBI::dbDisconnect(d$con, shutdown = TRUE), silent = TRUE),
            add = TRUE)
    target_levels <- sort(unique(DBI::dbGetQuery(
      d$con,
      sprintf(
        "SELECT DISTINCT %s FROM %s",
        lgb_ident(d$con, target),
        lgb_ident(d$con, d$view)
      )
    )[[1L]]))
    target_levels <- as.character(target_levels[!is.na(target_levels)])
    if (length(target_levels) < 2L) {
      rlang::abort("A classification target needs at least two classes.")
    }
  }

  backend <- DataBackendParquet$new(
    path, target, features,
    distinct_rows = distinct_rows,
    missings_rows = missings_rows,
    target_levels = target_levels
  )
  if (identical(task_type, "classif")) {
    mlr3::TaskClassif$new(
      id = id, backend = backend, target = target, positive = positive
    )
  } else {
    mlr3::TaskRegr$new(id = id, backend = backend, target = target)
  }
}
