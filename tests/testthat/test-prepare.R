test_that("lgb_prepare_parquet streams a preprocessing transform", {
  skip_if_not_installed("duckdb")
  input <- withr::local_tempfile(fileext = ".parquet")
  output <- withr::local_tempfile(fileext = ".parquet")
  df <- data.frame(x = 1:20, y = 21:40)
  lgbstream:::lgb_write_parquet(df, input)

  lgb_prepare_parquet(
    input, output,
    preprocess = function(data) {
      data$z <- data$x * 2
      data
    },
    overwrite = TRUE
  )

  task <- as_task_parquet(output, target = "y")
  out <- task$data(cols = c("x", "y", "z"))
  expect_equal(out$x, 1:20)
  expect_equal(out$z, (1:20) * 2)
  expect_equal(out$y, 21:40)
})
