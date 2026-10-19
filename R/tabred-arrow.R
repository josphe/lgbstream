lgb_fit_tabred_arrow <- function(
    path, target,
    features = NULL,
    preprocess = lgb_tabred_preprocess,
    split_col = NULL,
    split_val = NULL,
    train_prop = 0.8,
    val_prop = 0,
    scale = c("standard", "none"),
    nrounds = 300L,
    learning_rate = 0.1,
    num_leaves = 31L,
    min_data_in_leaf = 20L,
    feature_fraction = 1,
    bagging_fraction = 1,
    bagging_freq = 0L,
    max_bin = 255L,
    min_data_in_bin = 3L,
    num_threads = 0L,
    sample_size = 50000L,
    min_sd = 1e-12,
    na_fill = 0,
    seed = 42L,
    single_pass = FALSE) {
  scale <- rlang::arg_match(scale)
  lgb_init()
  all_names <- cpp_arrow_schema(path)
  n_total <- as.numeric(attr(all_names, "nrow"))
  if (!is.finite(n_total) || n_total < 1) {
    rlang::abort("The Parquet source has no rows.")
  }
  if (!target %in% all_names) {
    rlang::abort(sprintf("Target column `%s` not found.", target))
  }
  if (!is.null(split_col) && !split_col %in% all_names) {
    rlang::abort(sprintf("Split column `%s` not found.", split_col))
  }
  if (!is.null(split_col) &&
      (!length(split_val) %in% 1:2 || any(!is.finite(split_val)))) {
    rlang::abort("`split_val` must contain one or two finite thresholds.")
  }
  if (is.null(split_val)) {
    if (!is.null(split_col)) {
      rlang::abort("`split_val` is required when `split_col` is set.")
    }
    split_val <- c(train_prop, val_prop)
  }
  split_val <- as.numeric(split_val)
  if (!is.null(split_col)) {
    split_val <- sort(split_val)
  }
  sample_size <- min(lgb_size(sample_size, n_total, "sample_size"), 1e6)

  split <- if (is.null(split_col)) {
    lgb_make_split(n_total, NULL, split_val, seed)
  } else {
    NULL
  }
  in_train <- if (is.null(split_col)) {
    out <- logical(n_total)
    out[split$train] <- TRUE
    out
  } else {
    NULL
  }

  y_parts <- list()
  split_parts <- if (is.null(split_col)) NULL else list()
  sample_parts <- list()
  collected <- 0L
  sums <- NULL
  sumsq <- NULL
  counts <- NULL
  feats <- features

  rd <- cpp_arrow_open(path, all_names)
  repeat {
    batch <- cpp_arrow_next(rd)
    if (is.null(batch)) break
    x <- batch$x
    ids <- batch$row_start + seq_len(nrow(x)) - 1L
    y_parts[[length(y_parts) + 1L]] <- x[, target]
    if (!is.null(split_col)) {
      sv <- x[, split_col]
      split_parts[[length(split_parts) + 1L]] <- sv
      train_batch <- sv < split_val[[1L]]
    } else {
      train_batch <- in_train[ids]
    }
    data <- lgb_apply_preprocess(as.data.frame(x), preprocess)
    if (collected < sample_size && any(train_batch)) {
      train_rows <- which(train_batch)
      take <- min(length(train_rows), sample_size - collected)
      sample_parts[[length(sample_parts) + 1L]] <-
        data[train_rows[seq_len(take)], , drop = FALSE]
      collected <- collected + take
    }
    if (is.null(feats) && length(sample_parts)) {
      sample_so_far <- do.call(rbind, sample_parts)
      feats <- setdiff(names(sample_so_far), c(target, split_col))
      feats <- feats[vapply(sample_so_far[feats], is.numeric, logical(1L))]
      rm(sample_so_far)
    }
    if (!single_pass && !is.null(feats) && any(train_batch) &&
        !is.null(sample_parts) && identical(scale, "standard")) {
      xb <- as.matrix(data[train_batch, feats, drop = FALSE])
      if (is.null(sums)) {
        sums <- numeric(length(feats))
        sumsq <- numeric(length(feats))
        counts <- numeric(length(feats))
      }
      sums <- sums + colSums(xb, na.rm = TRUE)
      sumsq <- sumsq + colSums(xb^2, na.rm = TRUE)
      counts <- counts + colSums(!is.na(xb))
    }
    rm(x, data, ids, train_batch)
    gc(FALSE)
  }
  rm(rd)
  gc(FALSE)

  y <- unlist(y_parts, use.names = FALSE)
  rm(y_parts)
  if (is.null(split_col)) {
    # split was precomputed
  } else {
    split_vector <- unlist(split_parts, use.names = FALSE)
    rm(split_parts)
    split <- lgb_make_split(n_total, split_vector, split_val, seed)
    rm(split_vector)
  }
  train <- split$train
  val <- split$val
  test <- split$test
  if (!length(train)) rlang::abort("The split produced an empty training set.")
  if (!length(sample_parts)) {
    rlang::abort("No training rows were available for the scaler sample.")
  }

  sample_df <- do.call(rbind, sample_parts)
  rm(sample_parts)
  gc(FALSE)
  sample_df <- sample_df[seq_len(min(sample_size, nrow(sample_df))), , drop = FALSE]
  if (is.null(feats)) {
    feats <- setdiff(names(sample_df), c(target, split_col))
    feats <- feats[vapply(sample_df[feats], is.numeric, logical(1L))]
  }
  if (!length(feats)) {
    rlang::abort("No numeric feature columns were found after preprocessing.")
  }
  sample_x <- as.matrix(sample_df[, feats, drop = FALSE])
  rm(sample_df)

  scaler <- NULL
  if (identical(scale, "standard")) {
    if (single_pass) {
      scaler <- lgb_scaler_from_x(sample_x, min_sd = min_sd)
    } else {
      if (is.null(sums) || is.null(sumsq) || is.null(counts)) {
        rlang::abort("Could not accumulate scaler statistics.")
      }
      mean <- sums / pmax(counts, 1)
      variance <- sumsq / pmax(counts, 1) - mean^2
      sd <- sqrt(pmax(variance, 0))
      sd[!is.finite(sd) | sd < min_sd] <- 1
      scaler <- list(mean = mean, sd = sd)
    }
  }
  sample_x <- lgb_scale_x(sample_x, scaler, na_fill = na_fill)
  dataset <- cpp_dataset_from_sampled(
    sample_x, seq_len(nrow(sample_x)) - 1L, n_total,
    sprintf(
      "max_bin=%d min_data_in_bin=%d verbosity=-1",
      as.integer(max_bin), as.integer(min_data_in_bin)
    )
  )
  rm(sample_x)
  gc(FALSE)

  rd <- cpp_arrow_open(path, all_names)
  offset <- 0L
  while (offset < n_total) {
    batch <- cpp_arrow_next(rd)
    if (is.null(batch)) break
    data <- lgb_apply_preprocess(as.data.frame(batch$x), preprocess)
    x <- lgb_scale_x(
      as.matrix(data[, feats, drop = FALSE]), scaler, na_fill = na_fill
    )
    cpp_push(dataset, x, y, offset)
    offset <- offset + nrow(x)
    rm(batch, data, x)
    gc(FALSE)
  }
  rm(rd)
  cpp_finish(dataset)

  train_dataset <- if (length(train) == n_total) {
    dataset
  } else {
    cpp_subset(dataset, train - 1L)
  }
  fit <- cpp_train(
    train_dataset, nrounds,
    sprintf(
      paste0(
        "objective=regression metric=rmse learning_rate=%s num_leaves=%d ",
        "min_data_in_leaf=%d feature_fraction=%s bagging_fraction=%s ",
        "bagging_freq=%d num_threads=%d seed=%d verbosity=-1"
      ),
      learning_rate, num_leaves, min_data_in_leaf, feature_fraction,
      bagging_fraction, bagging_freq, num_threads, seed
    )
  )

  in_pred <- logical(n_total)
  in_pred[c(val, test)] <- TRUE
  pred_all <- numeric(n_total)
  rd <- cpp_arrow_open(path, all_names)
  while (TRUE) {
    batch <- cpp_arrow_next(rd)
    if (is.null(batch)) break
    ids <- batch$row_start + seq_len(nrow(batch$x)) - 1L
    keep <- in_pred[ids]
    if (any(keep)) {
      data <- lgb_apply_preprocess(
        as.data.frame(batch$x[keep, , drop = FALSE]), preprocess
      )
      x <- lgb_scale_x(
        as.matrix(data[, feats, drop = FALSE]), scaler, na_fill = na_fill
      )
      pred_all[ids[keep]] <- cpp_predict(fit$booster, x)
    }
    rm(batch, ids, keep)
    gc(FALSE)
  }
  rm(rd)
  gc(FALSE)

  val_pred <- pred_all[val]
  test_pred <- pred_all[test]
  val_rmse <- if (length(val)) sqrt(mean((val_pred - y[val])^2)) else NA_real_
  val_mae <- if (length(val)) mean(abs(val_pred - y[val])) else NA_real_
  test_rmse <- if (length(test)) sqrt(mean((test_pred - y[test])^2)) else NA_real_
  test_mae <- if (length(test)) mean(abs(test_pred - y[test])) else NA_real_

  structure(
    list(
      booster = fit$booster, features = feats, target = target,
      n = n_total, nfeat = length(feats), ntrain = length(train),
      nval = length(val), ntest = length(test),
      val_rmse = val_rmse, val_mae = val_mae,
      test_rmse = test_rmse, test_mae = test_mae,
      rmse = test_rmse, mae = test_mae,
      scale = scale, na_fill = na_fill, preprocess = preprocess,
      scaler = scaler, split = split
    ),
    class = "lgb_tabred_model"
  )
}
