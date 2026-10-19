lgb_env <- new.env(parent = emptyenv())

#' Locate the LightGBM shared library
#'
#' The library is loaded at run time, so the package does not need to be
#' linked against LightGBM.  The lookup order is: the `path` argument, the
#' `lgbstream.lightgbm` option, the `lgbstream_LIGHTGBM` environment
#' variable, and finally the shared library shipped with the `lightgbm`
#' package.
#'
#' @param path Optional path to `lightgbm.so` / `lightgbm.dll`.
#' @return A length-one character vector.
#' @keywords internal
lgb_lightgbm_path <- function(path = NULL) {
  path <- path %||%
    getOption("lgbstream.lightgbm") %||%
    Sys.getenv("lgbstream_LIGHTGBM", unset = "")

  if (nzchar(path)) {
    if (!file.exists(path)) {
      rlang::abort(sprintf("LightGBM library not found at %s.", path))
    }
    return(path)
  }

  libdir <- file.path(find.package("lightgbm"), "libs")
  candidates <- list.files(
    libdir,
    pattern = paste0("lightgbm", .Platform$dynlib.ext),
    recursive = TRUE,
    full.names = TRUE
  )
  if (!length(candidates)) {
    rlang::abort(paste(
      "Could not find the LightGBM shared library.",
      "Set `options(lgbstream.lightgbm = ...)` or `lgbstream_LIGHTGBM`.",
      sep = "\n"
    ))
  }
  candidates[[1L]]
}

#' Initialise the LightGBM C API
#'
#' @param path Optional path to the LightGBM shared library.
#' @return Invisibly `TRUE`.
#' @export
lgb_init <- function(path = NULL) {
  if (isTRUE(lgb_env$initialised)) {
    return(invisible(TRUE))
  }
  path <- lgb_lightgbm_path(path)
  cpp_init(path)
  lgb_env$initialised <- TRUE
  lgb_env$path <- path
  invisible(TRUE)
}

lgb_hash <- function(...) {
  rlang::hash(list(...))
}

lgb_check_flag <- function(x, name) {
  if (!is.logical(x) || length(x) != 1L || is.na(x)) {
    rlang::abort(sprintf("`%s` must be TRUE or FALSE.", name))
  }
  invisible(x)
}

lgb_as_data_frame <- function(x, name = "data") {
  if (data.table::is.data.table(x)) {
    return(as.data.frame(x))
  }
  if (!is.data.frame(x)) {
    rlang::abort(sprintf("`%s` must be a data frame or data.table.", name))
  }
  x
}

lgb_size <- function(size, n, name = "size", min = 1L) {
  if (is.null(size)) {
    return(NULL)
  }
  if (!is.numeric(size) || length(size) != 1L || !is.finite(size) || size <= 0) {
    rlang::abort(sprintf("`%s` must be a positive finite number.", name))
  }
  if (size < 1) {
    return(max(as.integer(min), as.integer(ceiling(n * size))))
  }
  as.integer(size)
}
