test_that("classification learner handles binary and multiclass tasks", {
  skip_if_not_installed("lightgbm")
  library(mlr3)
  set.seed(1)
  n <- 200L
  df <- data.frame(
    x1 = rnorm(n),
    x2 = rnorm(n),
    y = factor(sample(c("no", "yes"), n, replace = TRUE))
  )
  task <- mlr3::as_task_classif(df, target = "y", id = "binary")
  learner <- lrn(
    "classif.lgbstream",
    nrounds = 5L,
    sample_size = 50,
    batch_size = 50,
    num_threads = 1L
  )
  learner$train(task, 1:150)
  pred <- learner$predict(task, 151:200)
  expect_equal(pred$row_ids, 151:200)
  expect_equal(length(pred$response), 50)
  expect_equal(dim(pred$prob), c(50, 2))

  df$y <- factor(sample(c("a", "b", "c"), n, replace = TRUE))
  task2 <- mlr3::as_task_classif(df, target = "y", id = "multi")
  learner2 <- lrn(
    "classif.lgbstream",
    nrounds = 5L,
    sample_size = 50,
    batch_size = 50,
    num_threads = 1L
  )
  learner2$train(task2, 1:150)
  pred2 <- learner2$predict(task2, 151:200)
  expect_equal(dim(pred2$prob), c(50, 3))
})
