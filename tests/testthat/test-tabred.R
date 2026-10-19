test_that("TabReD preprocess, split, scale, and metrics work", {
  skip_if_not_installed("duckdb")

  set.seed(1)
  n <- 600L
  df <- data.frame(
    fact_time = as.numeric(as.POSIXct("2023-01-01", tz = "UTC")) + seq_len(n) * 3600,
    x1 = rnorm(n),
    x2 = rnorm(n),
    y = rnorm(n)
  )
  path <- withr::local_tempfile(fileext = ".parquet")
  lgbstream:::lgb_write_parquet(df, path)

  cut1 <- as.numeric(as.POSIXct("2023-01-10", tz = "UTC"))
  cut2 <- as.numeric(as.POSIXct("2023-01-20", tz = "UTC"))

  fit <- lgb_fit_tabred(
    path, "y",
    split_col = "fact_time",
    split_val = c(cut1, cut2),
    scale = "standard",
    nrounds = 5L,
    sample_size = 100L,
    num_threads = 1L,
    seed = 42L
  )

  expect_s3_class(fit, "lgb_tabred_model")
  expect_true(fit$ntrain > 0)
  expect_true(fit$nval > 0)
  expect_true(fit$ntest > 0)
  expect_true(is.finite(fit$val_rmse))
  expect_true(is.finite(fit$test_rmse))

  pred <- lgb_predict_tabred(fit, df[1:5, ])
  expect_length(pred, 5)
  expect_true(all(is.finite(pred)))
})
