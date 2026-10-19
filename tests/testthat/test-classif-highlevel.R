test_that("Parquet classification task and high-level classifier work", {
  skip_if_not_installed("duckdb")
  path <- withr::local_tempfile(fileext = ".parquet")
  set.seed(1)
  df <- data.frame(
    x1 = rnorm(200),
    x2 = rnorm(200),
    y = factor(rep(c("a", "b"), each = 100))
  )
  lgbstream:::lgb_write_parquet(df, path)

  task <- as_task_parquet(path, "y", task_type = "classif")
  expect_s3_class(task, "TaskClassif")
  expect_setequal(task$class_names, c("a", "b"))

  fit <- lgb_fit_classif(
    path, "y",
    nrounds = 3L, num_threads = 1L, sample_size = 50L
  )
  expect_s3_class(fit, "lgb_classif_model")

  pred <- lgb_predict_classif(fit, path)
  expect_s3_class(pred, "PredictionClassif")
  expect_equal(length(pred$response), 200)
  expect_true(all(is.finite(pred$prob)))

  pred_df <- lgb_predict_classif(fit, df)
  expect_s3_class(pred_df, "PredictionClassif")
  expect_equal(length(pred_df$response), 200)

  pred_dt <- lgb_predict_classif(fit, data.table::as.data.table(df))
  expect_equal(pred_dt$response, pred_df$response)

  factor_path <- withr::local_tempfile(fileext = ".parquet")
  factor_df <- data.frame(
    x = rnorm(120),
    cat = factor(rep(c("a", "b", "c"), 40)),
    chr = rep(c("u", "v"), 60),
    y = factor(rep(c("no", "yes"), each = 60))
  )
  lgbstream:::lgb_write_parquet(factor_df, factor_path)
  factor_fit <- lgb_fit_classif(
    factor_path, "y",
    nrounds = 2L, num_threads = 1L, sample_size = 30L
  )
  factor_pred <- lgb_predict_classif(factor_fit, factor_path)
  expect_s3_class(factor_pred, "PredictionClassif")
  expect_equal(length(factor_pred$response), 120)

  res <- mlr3::resample(
    task,
    mlr3::lrn(
      "classif.lgbstream",
      nrounds = 2L, num_threads = 1L, sample_size = 50L
    ),
    mlr3::rsmp("holdout", ratio = 0.8),
    store_models = FALSE
  )
  expect_equal(length(res$prediction()$response), 40)
})
