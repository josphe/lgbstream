# lgbstream

[![Lifecycle: experimental](https://img.shields.io/badge/lifecycle-experimental-orange.svg)](https://lifecycle.r-lib.org/articles/stages.html#experimental)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![R >= 4.1](https://img.shields.io/badge/R-%3E%3D%204.1-blue.svg)](https://www.r-project.org/)

`lgbstream` fits LightGBM models on Parquet data without materialising the
full feature matrix in R.  A C++ Arrow reader streams `RecordBatch`es into the
LightGBM C API.  DuckDB is used for random-access backends, directory sources,
and small Task/Parquet conversions.

## Installation

### Requirements

- R >= 4.1
- C++20 compiler
- Arrow C++ with `libarrow` and `libparquet`
- DuckDB
- LightGBM >= 4.0 with `LGBM_DatasetCreateFromSampledColumn()` and
  `LGBM_DatasetPushRowsWithMetadata()`

Set `ARROW_PREFIX` when Arrow C++ is not installed under `CONDA_PREFIX`.

### Install

```r
# install.packages("remotes")
remotes::install_github("josphe/lgbstream")
```

The mlr3 examples use the `mlr3verse` meta-package:

```r
install.packages("mlr3verse")
```

## Quick start

```r
library(lgbstream)

fit <- lgb_fit(
  "weather.parquet",
  target = "y",
  nrounds = 100,
  num_threads = 8
)

pred <- lgb_predict(fit, "weather_test.parquet")
```

## 1. Standard workflow

### Tabular regression

`lgb_fit_tabred()` combines preprocessing, a train/validation/test split, and
optional standard scaling:

```r
fit <- lgb_fit_tabred(
  "weather.parquet",
  target = "fact_temperature",
  single_pass = FALSE,
  sample_size = 50000,
  nrounds = 300
)
```

- `single_pass = FALSE` computes the scaler on all training rows.
- `single_pass = TRUE` uses `sample_size` rows for the scaler.
- `large_data = TRUE`, when `single_pass` is not supplied, selects the
  sample-scaler mode.

### Classification

```r
fit <- lgb_fit_classif(
  "weather.parquet",
  target = "rain",
  nrounds = 100,
  num_threads = 8
)

pred <- lgb_predict_classif(fit, "weather_test.parquet")
pred$prob
pred$response
```

### mlr3

```r
library(mlr3verse)

task <- as_task_parquet("weather.parquet", target = "y")
learner <- lrn("regr.lgbstream", nrounds = 100, num_threads = 8)

learner$train(task, row_ids = 1:340000)
pred <- learner$predict(task, row_ids = 340001:423795)
```

## 2. Large-data workflow

Preprocess once, write the final Parquet, then train only on that file.  The
base workflow needs only `lgbstream`; the mlr3verse workflow is optional for
CV and multiple learners.

### Base lgbstream

```r
library(lgbstream)

lgb_prepare_parquet(
  "raw.parquet",
  "prepared.parquet",
  preprocess = function(data) data,  # replace with your transform
  overwrite = TRUE
)

fit <- lgb_fit_tabred(
  "prepared.parquet",
  target = "y",
  large_data = TRUE,
  sample_size = 50000,
  nrounds = 300,
  num_threads = 16
)

fit$test_rmse
fit$test_mae
```

For a plain fit without the TabReD split/scaler wrapper:

```r
fit <- lgb_fit(
  "prepared.parquet",
  target = "y",
  nrounds = 300,
  num_threads = 16
)
```

### mlr3verse

Use `mlr3verse` when CV, holdout resampling, or multiple learners are needed.

```r
library(mlr3verse)

task <- as_task_parquet("prepared.parquet", target = "y")
learner <- lrn("regr.lgbstream", nrounds = 300, num_threads = 16)

# CV / holdout
resample(task, learner, rsmp("holdout", ratio = 0.8))
resample(task, learner, rsmp("cv", folds = 2))

# Multiple learners / ensemble base learners
l2 <- lrn("regr.lgbstream", nrounds = 300, num_threads = 16)
l2$id <- "regr.lgbstream2"
bmr <- benchmark(
  benchmark_grid(task, list(learner, l2), rsmp("holdout", ratio = 0.8)),
  store_models = FALSE
)
```

`resample()` replaces `learner_cv`, and `benchmark()` replaces a branching
`GraphLearner`.  A large `mlr3pipelines` graph, `branch`, or `learner_cv`
inside the training path materialises the full Task in R.

## API

```text
lgb_fit() / lgb_predict()
lgb_fit_classif() / lgb_predict_classif()
lgb_fit_tabred() / lgb_predict_tabred()
lgb_prepare_parquet()
as_task_parquet()
DataBackendParquet
LearnerRegrLGBStream / LearnerClassifLGBStream
lgb_init()
lgb_tabred_preprocess()
```

The low-level `lgb_stream_*` helpers are internal.

## Contributing

Bug reports and pull requests are welcome at
<https://github.com/josphe/lgbstream/issues>.  Please include a reproducible
example and `sessionInfo()` output.

## License

MIT.
