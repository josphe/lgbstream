test_that("DataBackendParquet reads slices on demand", {
  skip_if_not_installed("duckdb")
  path <- withr::local_tempfile(fileext = ".parquet")
  df <- data.frame(x1 = 1:10, x2 = 11:20, y = 21:30)
  lgbstream:::lgb_write_parquet(df, path)

  task <- as_task_parquet(path, target = "y")
  expect_equal(task$nrow, 10)
  expect_equal(task$ncol, 3)
  expect_equal(task$data(rows = 1:3, cols = c("x1", "y"))$x1, 1:3)
  expect_equal(task$truth(1:3), 21:23)
})

test_that("DataBackendParquet handles zero rows", {
  skip_if_not_installed("duckdb")
  path <- withr::local_tempfile(fileext = ".parquet")
  lgbstream:::lgb_write_parquet(data.frame(x = 1:3, y = 4:6), path)
  backend <- DataBackendParquet$new(path, target = "y")
  out <- backend$data(integer(), c("x", "y"))
  expect_equal(nrow(out), 0)
  expect_named(out, c("x", "y"))
})
