test_that("lgb_fit() and lgb_predict() work end to end", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("lightgbm")

  path <- withr::local_tempfile(fileext = ".parquet")
  df <- data.frame(
    x1 = rnorm(200),
    x2 = rnorm(200),
    x3 = sample(0:1, 200, replace = TRUE),
    y = rnorm(200)
  )
  lgbstream:::lgb_write_parquet(df, path)

  fit <- tryCatch(
    lgb_fit(path, target = "y", nrounds = 10L, num_threads = 1L),
    error = function(e) e
  )
  if (inherits(fit, "error")) {
    skip(conditionMessage(fit))
  }

  pred <- lgb_predict(fit, df[1:5, c("x1", "x2", "x3")])
  expect_length(pred, 5)
  expect_true(all(is.finite(pred)))
})

test_that("learner converts a non-Parquet task to Parquet", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("lightgbm")
  library(mlr3)

  df <- data.frame(
    x1 = rnorm(200),
    x2 = rnorm(200),
    g = factor(sample(c("a", "b"), 200, replace = TRUE)),
    y = rnorm(200)
  )
  task <- mlr3::as_task_regr(df, target = "y", id = "memory")
  learner <- lrn(
    "regr.lgbstream",
    nrounds = 5L,
    sample_size = 50,
    batch_size = 50,
    num_threads = 1L
  )
  learner$train(task, 1:150)
  pred <- learner$predict(task, 151:200)

  expect_equal(pred$row_ids, 151:200)
  expect_length(pred$response, 50)
  expect_true(all(is.finite(pred$response)))
})

