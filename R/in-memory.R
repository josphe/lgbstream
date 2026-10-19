lgb_in_memory_allowed <- function(task) {
  cells <- as.double(task$nrow) * length(task$feature_names)
  cells <= getOption("lgbstream.in_memory_cells", 5e7)
}

lgb_in_memory_data <- function(task, features, levels = NULL) {
  data <- as.data.frame(task$data())
  target <- task$target_names
  target_values <- data[[target]]
  encoded <- lgb_encode_columns(data, levels = levels)
  data <- encoded$data
  data[[target]] <- target_values
  missing <- setdiff(features, names(data))
  if (length(missing)) {
    rlang::abort(sprintf(
      "Unknown feature column(s) after preprocessing: %s.",
      paste(missing, collapse = ", ")
    ))
  }
  list(
    x = as.matrix(data[, features, drop = FALSE]),
    y = target_values,
    levels = encoded$levels,
    types = encoded$types
  )
}

lgb_in_memory_dataset <- function(task, features, levels = NULL,
                                  sample_size = 50000L,
                                  batch_size = 32768L,
                                  max_bin = 255L,
                                  min_data_in_bin = 3L,
                                  weights = NULL) {
  lgb_init()
  data <- lgb_in_memory_data(task, features, levels)
  x <- data$x
  n <- nrow(x)
  sample_n <- min(lgb_size(sample_size, n, "sample_size"), n)
  dataset <- cpp_dataset_from_sampled(
    x[seq_len(sample_n), , drop = FALSE],
    seq_len(sample_n) - 1L,
    n,
    sprintf(
      "max_bin=%d min_data_in_bin=%d verbosity=-1",
      as.integer(max_bin), as.integer(min_data_in_bin)
    )
  )
  offset <- 0L
  while (offset < n) {
    end <- min(n, offset + batch_size)
    cpp_push(
      dataset,
      x[(offset + 1L):end, , drop = FALSE],
      data$y,
      offset
    )
    offset <- end
  }
  if (!is.null(weights)) {
    cpp_set_weight(dataset, as.numeric(weights))
  }
  cpp_finish(dataset)
  list(
    dataset = dataset,
    n = n,
    levels = data$levels,
    types = data$types
  )
}
