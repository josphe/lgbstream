.onLoad <- function(libname, pkgname) {
  if (requireNamespace("mlr3", quietly = TRUE)) {
    if (!"regr.lgbstream" %in% mlr3::mlr_learners$keys()) {
      mlr3::mlr_learners$add("regr.lgbstream", LearnerRegrLGBStream)
    }
    if (!"classif.lgbstream" %in% mlr3::mlr_learners$keys()) {
      mlr3::mlr_learners$add("classif.lgbstream", LearnerClassifLGBStream)
    }
  }
  path <- getOption("lgbstream.lightgbm")
  if (!is.null(path)) {
    try(lgb_init(path), silent = TRUE)
  }
}
