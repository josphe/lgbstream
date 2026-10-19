// stream.cpp -- LightGBM C API bridge for lgbstream
//
// The LightGBM shared library is loaded at run time (no link-time
// dependency).  Two ingestion paths are exposed:
//   * cpp_dataset_from_arrow(): LGBM_DatasetCreateFromArrowStream()
//   * cpp_dataset_from_matrix() + cpp_push(): CreateFromMat()/CreateByReference()
//     followed by LGBM_DatasetPushRowsWithMetadata()
//
// Only the Arrow C data interface is used; the package does not link to
// Apache Arrow.

#include <Rcpp.h>

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <numeric>
#include <string>
#include <vector>

#ifdef _WIN32
#include <windows.h>
#else
#include <dlfcn.h>
#endif

extern "C" {
struct ArrowSchema;
struct ArrowArray;
struct ArrowArrayStream {
  int (*get_schema)(struct ArrowArrayStream *, struct ArrowSchema *);
  int (*get_next)(struct ArrowArrayStream *, struct ArrowArray *);
  const char *(*get_last_error)(struct ArrowArrayStream *);
  void (*release)(struct ArrowArrayStream *);
  void *private_data;
};
}

typedef int (*fn_create_arrow_stream)(ArrowArrayStream *, const char *, void *, void **);
typedef int (*fn_create_mat)(const void *, int, int32_t, int32_t, int,
                             const char *, void *, void **);
typedef int (*fn_create_by_ref)(void *, int64_t, void **);
typedef int (*fn_get_subset)(void *, const int32_t *, int32_t, const char *, void **);
typedef int (*fn_push_rows)(void *, const void *, int, int32_t, int32_t, int32_t);
typedef int (*fn_set_field)(void *, const char *, const void *, int, int);
typedef int (*fn_push_rows_meta)(void *, const void *, int, int32_t, int32_t, int32_t,
                                 const float *, const float *, const double *,
                                 const int32_t *, int32_t);
typedef int (*fn_init_streaming)(void *, int32_t, int32_t, int32_t, int32_t, int32_t, int32_t);
typedef int (*fn_mark_finished)(void *);
typedef int (*fn_create_sampled)(double **, int **, int32_t, const int *, int32_t,
                                 int32_t, int64_t, const char *, void **);
typedef int (*fn_get_num_data)(void *, int32_t *);
typedef int (*fn_ds_free)(void *);
typedef int (*fn_booster_create)(void *, const char *, void **);
typedef int (*fn_booster_load)(const char *, int *, void **);
typedef int (*fn_booster_update)(void *, int *);
typedef int (*fn_booster_eval)(void *, int, int *, double *);
typedef int (*fn_booster_predict)(void *, const void *, int, int32_t, int32_t, int,
                                  int, int, int, const char *, int64_t *, double *);
typedef int (*fn_booster_importance)(void *, int, int, double *);
typedef int (*fn_booster_num_feature)(void *, int *);
typedef int (*fn_booster_num_classes)(void *, int *);
typedef int (*fn_booster_save)(void *, int, int, int, const char *);
typedef int (*fn_booster_free)(void *);
typedef const char *(*fn_last_error)(void);

static void *g_lib = nullptr;
static std::string g_err;

static fn_create_arrow_stream p_create_arrow_stream = nullptr;
static fn_create_mat p_create_mat = nullptr;
static fn_create_by_ref p_create_by_ref = nullptr;
static fn_get_subset p_get_subset = nullptr;
static fn_push_rows p_push_rows = nullptr;
static fn_set_field p_set_field = nullptr;
static fn_push_rows_meta p_push_rows_meta = nullptr;
static fn_init_streaming p_init_streaming = nullptr;
static fn_mark_finished p_mark_finished = nullptr;
static fn_create_sampled p_create_sampled = nullptr;
static fn_get_num_data p_get_num_data = nullptr;
static fn_ds_free p_ds_free = nullptr;
static fn_booster_create p_booster_create = nullptr;
static fn_booster_load p_booster_load = nullptr;
static fn_booster_update p_booster_update = nullptr;
static fn_booster_eval p_booster_eval = nullptr;
static fn_booster_predict p_booster_predict = nullptr;
static fn_booster_importance p_booster_importance = nullptr;
static fn_booster_num_feature p_booster_num_feature = nullptr;
static fn_booster_num_classes p_booster_num_classes = nullptr;
static fn_booster_save p_booster_save = nullptr;
static fn_booster_free p_booster_free = nullptr;
static fn_last_error p_last_error = nullptr;

#ifdef _WIN32
#define lgb_LOAD(p) LoadLibraryA(p)
#define lgb_SYM(h, n) GetProcAddress((HMODULE) (h), n)
#else
#define lgb_LOAD(p) dlopen((p), RTLD_NOW | RTLD_LOCAL)
#define lgb_SYM(h, n) dlsym((h), (n))
#endif

struct DatasetHandle {
  void *h;
};

struct BoosterHandle {
  void *h;
};

void lgb_ds_finalizer(DatasetHandle *d) {
  if (d) {
    if (d->h && p_ds_free) p_ds_free(d->h);
    delete d;
  }
}

void lgb_booster_finalizer(BoosterHandle *b) {
  if (b) {
    if (b->h && p_booster_free) p_booster_free(b->h);
    delete b;
  }
}

void lgb_stream_finalizer(ArrowArrayStream *s) {
  if (s) {
    if (s->release) s->release(s);
    delete s;
  }
}

using DatasetXPtr = Rcpp::XPtr<DatasetHandle, Rcpp::PreserveStorage, lgb_ds_finalizer>;
using BoosterXPtr = Rcpp::XPtr<BoosterHandle, Rcpp::PreserveStorage, lgb_booster_finalizer>;
using StreamXPtr = Rcpp::XPtr<ArrowArrayStream, Rcpp::PreserveStorage, lgb_stream_finalizer>;

static void set_err(const char *what) {
  const char *msg = p_last_error ? p_last_error() : nullptr;
  g_err = std::string(what) + ": " + (msg ? msg : "unknown error");
}

static void check(int rc, const char *what) {
  if (rc != 0) {
    set_err(what);
    Rcpp::stop(g_err);
  }
}

static void require_loaded() {
  if (!g_lib) Rcpp::stop("LightGBM library is not loaded; call lgb_init() first.");
}

// [[Rcpp::export]]
bool cpp_init(std::string path) {
  if (g_lib) return true;
#ifdef _WIN32
  {
    std::string dir = path;
    auto pos = dir.find_last_of("\\/");
    if (pos != std::string::npos) SetDllDirectoryA(dir.substr(0, pos).c_str());
  }
#endif
  g_lib = lgb_LOAD(path.c_str());
  if (!g_lib) Rcpp::stop("failed to load LightGBM library: " + path);

  p_create_arrow_stream = (fn_create_arrow_stream) lgb_SYM(g_lib, "LGBM_DatasetCreateFromArrowStream");
  p_create_mat = (fn_create_mat) lgb_SYM(g_lib, "LGBM_DatasetCreateFromMat");
  p_create_by_ref = (fn_create_by_ref) lgb_SYM(g_lib, "LGBM_DatasetCreateByReference");
  p_get_subset = (fn_get_subset) lgb_SYM(g_lib, "LGBM_DatasetGetSubset");
  p_push_rows = (fn_push_rows) lgb_SYM(g_lib, "LGBM_DatasetPushRows");
  p_set_field = (fn_set_field) lgb_SYM(g_lib, "LGBM_DatasetSetField");
  p_push_rows_meta = (fn_push_rows_meta) lgb_SYM(g_lib, "LGBM_DatasetPushRowsWithMetadata");
  p_init_streaming = (fn_init_streaming) lgb_SYM(g_lib, "LGBM_DatasetInitStreaming");
  p_mark_finished = (fn_mark_finished) lgb_SYM(g_lib, "LGBM_DatasetMarkFinished");
  p_create_sampled = (fn_create_sampled) lgb_SYM(g_lib, "LGBM_DatasetCreateFromSampledColumn");
  p_get_num_data = (fn_get_num_data) lgb_SYM(g_lib, "LGBM_DatasetGetNumData");
  p_ds_free = (fn_ds_free) lgb_SYM(g_lib, "LGBM_DatasetFree");
  p_booster_create = (fn_booster_create) lgb_SYM(g_lib, "LGBM_BoosterCreate");
  p_booster_load = (fn_booster_load) lgb_SYM(g_lib, "LGBM_BoosterCreateFromModelfile");
  p_booster_update = (fn_booster_update) lgb_SYM(g_lib, "LGBM_BoosterUpdateOneIter");
  p_booster_eval = (fn_booster_eval) lgb_SYM(g_lib, "LGBM_BoosterGetEval");
  p_booster_predict = (fn_booster_predict) lgb_SYM(g_lib, "LGBM_BoosterPredictForMat");
  p_booster_importance = (fn_booster_importance) lgb_SYM(g_lib, "LGBM_BoosterFeatureImportance");
  p_booster_num_feature = (fn_booster_num_feature) lgb_SYM(g_lib, "LGBM_BoosterGetNumFeature");
  p_booster_num_classes = (fn_booster_num_classes) lgb_SYM(g_lib, "LGBM_BoosterGetNumClasses");
  p_booster_save = (fn_booster_save) lgb_SYM(g_lib, "LGBM_BoosterSaveModel");
  p_booster_free = (fn_booster_free) lgb_SYM(g_lib, "LGBM_BoosterFree");
  p_last_error = (fn_last_error) lgb_SYM(g_lib, "LGBM_GetLastError");

  if (!p_create_mat || !p_create_by_ref || !p_get_subset || !p_push_rows ||
      !p_ds_free || !p_booster_create || !p_booster_update || !p_booster_eval ||
      !p_booster_predict || !p_booster_save || !p_booster_free || !p_last_error) {
    Rcpp::stop("the LightGBM build is missing required C API symbols");
  }
  return true;
}

// [[Rcpp::export]]
bool cpp_has_arrow_stream() {
  return g_lib && p_create_arrow_stream != nullptr;
}

// [[Rcpp::export]]
std::string cpp_last_error() {
  return g_err;
}

// [[Rcpp::export]]
SEXP cpp_new_stream() {
  ArrowArrayStream *s = new ArrowArrayStream();
  std::memset(s, 0, sizeof(*s));
  StreamXPtr ptr(s, true);
  return ptr;
}

// [[Rcpp::export]]
SEXP cpp_dataset_from_arrow(SEXP stream, std::string params) {
  require_loaded();
  if (!p_create_arrow_stream) {
    Rcpp::stop("this LightGBM build has no LGBM_DatasetCreateFromArrowStream()");
  }
  ArrowArrayStream *s = (ArrowArrayStream *) R_ExternalPtrAddr(stream);
  if (!s || !s->release) Rcpp::stop("invalid ArrowArrayStream pointer");
  void *h = nullptr;
  check(p_create_arrow_stream(s, params.c_str(), nullptr, &h),
        "LGBM_DatasetCreateFromArrowStream");
  return DatasetXPtr(new DatasetHandle{h}, true);
}

// [[Rcpp::export]]
SEXP cpp_dataset_from_matrix(Rcpp::NumericMatrix sample, int n_total,
                            std::string params) {
  require_loaded();
  void *ref = nullptr;
  void *h = nullptr;
  check(p_create_mat(REAL(sample), 1 /* FLOAT64 */, sample.nrow(), sample.ncol(),
                     0 /* column-major */, params.c_str(), nullptr, &ref),
        "LGBM_DatasetCreateFromMat");
  check(p_create_by_ref(ref, (int64_t) n_total, &h),
        "LGBM_DatasetCreateByReference");
  p_ds_free(ref);
  if (p_init_streaming) p_init_streaming(h, 0, 0, 0, 1, 1, -1);
  return DatasetXPtr(new DatasetHandle{h}, true);
}

// [[Rcpp::export]]
SEXP cpp_dataset_from_sampled(Rcpp::NumericMatrix sample,
                             Rcpp::IntegerVector sample_indices,
                             int n_total, std::string params) {
  require_loaded();
  if (!p_create_sampled) {
    Rcpp::stop("this LightGBM build has no LGBM_DatasetCreateFromSampledColumn()");
  }
  int n = sample.nrow(), p = sample.ncol();
  if (sample_indices.size() != n) {
    Rcpp::stop("sample_indices must have one element per sample row");
  }
  std::vector<double *> cols((size_t) p);
  std::vector<int *> idxs((size_t) p);
  std::vector<int> npc((size_t) p, n);
  // LightGBM indexes the sample arrays by position (0 .. n-1), not by the
  // global row id.  Each column also needs its own indices because they may
  // be reordered in place.
  std::vector<int> base((size_t) n);
  std::iota(base.begin(), base.end(), 0);
  std::vector<std::vector<int>> idx_store((size_t) p, base);
  for (int j = 0; j < p; ++j) {
    cols[(size_t) j] = &sample(0, j);
    idxs[(size_t) j] = idx_store[(size_t) j].data();
  }
  void *h = nullptr;
  check(p_create_sampled(cols.data(), idxs.data(), p, npc.data(), n, n_total,
                         (int64_t) n_total, params.c_str(), &h),
        "LGBM_DatasetCreateFromSampledColumn");
  if (p_init_streaming) p_init_streaming(h, 0, 0, 0, 1, 1, -1);
  return DatasetXPtr(new DatasetHandle{h}, true);
}

// [[Rcpp::export]]
void cpp_finish(SEXP ds) {
  require_loaded();
  DatasetHandle *d = (DatasetHandle *) R_ExternalPtrAddr(ds);
  if (!d || !d->h) Rcpp::stop("invalid dataset pointer");
  if (p_mark_finished) {
    check(p_mark_finished(d->h), "LGBM_DatasetMarkFinished");
  }
}

// [[Rcpp::export]]
int cpp_num_data(SEXP ds) {
  require_loaded();
  DatasetHandle *d = (DatasetHandle *) R_ExternalPtrAddr(ds);
  if (!d || !d->h) Rcpp::stop("invalid dataset pointer");
  int32_t n = 0;
  if (p_get_num_data) p_get_num_data(d->h, &n);
  return (int) n;
}

// [[Rcpp::export]]
void cpp_push(SEXP ds, Rcpp::NumericMatrix x, Rcpp::NumericVector y, int start) {
  require_loaded();
  DatasetHandle *d = (DatasetHandle *) R_ExternalPtrAddr(ds);
  if (!d || !d->h) Rcpp::stop("invalid dataset pointer");
  if (!p_push_rows_meta) {
    Rcpp::stop("this LightGBM build has no LGBM_DatasetPushRowsWithMetadata()");
  }
  int n = x.nrow(), p = x.ncol();
  const bool batch_y = y.size() == n;
  if (!batch_y && y.size() < (R_xlen_t) start + n) {
    Rcpp::stop("label vector is neither batch-sized nor long enough for `start`");
  }
  std::vector<float> buf((size_t) n * p);
  std::vector<float> lab((size_t) n);
  for (int i = 0; i < n; ++i) {
    for (int j = 0; j < p; ++j) {
      buf[(size_t) i * p + j] = (float) x(i, j);
    }
    lab[i] = (float) (batch_y ? y[i] : y[start + i]);
  }
  check(p_push_rows_meta(d->h, buf.data(), 0, n, p, start, lab.data(),
                         nullptr, nullptr, nullptr, 0),
        "LGBM_DatasetPushRowsWithMetadata");
}

// [[Rcpp::export]]
void cpp_set_label(SEXP ds, Rcpp::NumericVector y) {
  require_loaded();
  DatasetHandle *d = (DatasetHandle *) R_ExternalPtrAddr(ds);
  if (!d || !d->h) Rcpp::stop("invalid dataset pointer");
  std::vector<float> lab((size_t) y.size());
  for (R_xlen_t i = 0; i < y.size(); ++i) lab[(size_t) i] = (float) y[i];
  check(p_set_field(d->h, "label", lab.data(), (int) y.size(), 0 /* FLOAT32 */),
        "LGBM_DatasetSetField(label)");
}

// [[Rcpp::export]]
void cpp_set_weight(SEXP ds, Rcpp::NumericVector w) {
  require_loaded();
  DatasetHandle *d = (DatasetHandle *) R_ExternalPtrAddr(ds);
  if (!d || !d->h) Rcpp::stop("invalid dataset pointer");
  std::vector<float> weight((size_t) w.size());
  for (R_xlen_t i = 0; i < w.size(); ++i) weight[(size_t) i] = (float) w[i];
  check(p_set_field(d->h, "weight", weight.data(), (int) w.size(), 0),
        "LGBM_DatasetSetField(weight)");
}

// [[Rcpp::export]]
SEXP cpp_subset(SEXP ds, Rcpp::IntegerVector rows) {
  require_loaded();
  DatasetHandle *d = (DatasetHandle *) R_ExternalPtrAddr(ds);
  if (!d || !d->h) Rcpp::stop("invalid dataset pointer");
  std::vector<int32_t> idx(rows.begin(), rows.end());
  std::sort(idx.begin(), idx.end());
  idx.erase(std::unique(idx.begin(), idx.end()), idx.end());
  if (!idx.empty() && p_get_num_data) {
    int32_t nrows = 0;
    p_get_num_data(d->h, &nrows);
    if (idx.front() < 0 || idx.back() >= nrows) {
      Rcpp::stop("subset indices must be 0-based and inside [0, num_data - 1]");
    }
  }
  void *h = nullptr;
  check(p_get_subset(d->h, idx.data(), (int32_t) idx.size(), "", &h),
        "LGBM_DatasetGetSubset");
  return DatasetXPtr(new DatasetHandle{h}, true);
}

// [[Rcpp::export]]
Rcpp::List cpp_train(SEXP ds, int nrounds, std::string params) {
  require_loaded();
  DatasetHandle *d = (DatasetHandle *) R_ExternalPtrAddr(ds);
  if (!d || !d->h) Rcpp::stop("invalid dataset pointer");
  void *b = nullptr;
  check(p_booster_create(d->h, params.c_str(), &b), "LGBM_BoosterCreate");
  int finished = 0;
  for (int i = 0; i < nrounds; ++i) {
    check(p_booster_update(b, &finished), "LGBM_BoosterUpdateOneIter");
  }
  int len = 16;
  std::vector<double> eval((size_t) len, NA_REAL);
  if (p_booster_eval) p_booster_eval(b, 0, &len, eval.data());
  Rcpp::NumericVector out(eval.begin(), eval.begin() + len);
  return Rcpp::List::create(
    Rcpp::Named("booster") = BoosterXPtr(new BoosterHandle{b}, true),
    Rcpp::Named("eval") = out);
}

// [[Rcpp::export]]
Rcpp::NumericVector cpp_predict(SEXP booster, Rcpp::NumericMatrix x,
                                int predict_type = 0L,
                                int num_class = 1L) {
  require_loaded();
  BoosterHandle *b = (BoosterHandle *) R_ExternalPtrAddr(booster);
  if (!b || !b->h) Rcpp::stop("invalid booster pointer");
  int n = x.nrow(), p = x.ncol();
  int cols = num_class > 1 ? num_class : 1;
  std::vector<float> buf((size_t) n * p);
  for (int i = 0; i < n; ++i) {
    for (int j = 0; j < p; ++j) buf[(size_t) i * p + j] = (float) x(i, j);
  }
  Rcpp::NumericVector out((size_t) n * cols);
  int64_t len = 0;
  check(p_booster_predict(b->h, buf.data(), 0, n, p, 1, predict_type, 0, -1,
                          "", &len, REAL(out)),
        "LGBM_BoosterPredictForMat");
  return out;
}

// [[Rcpp::export]]
int cpp_num_classes(SEXP booster) {
  require_loaded();
  if (!p_booster_num_classes) {
    Rcpp::stop("this LightGBM build lacks LGBM_BoosterGetNumClasses");
  }
  BoosterHandle *b = (BoosterHandle *) R_ExternalPtrAddr(booster);
  if (!b || !b->h) Rcpp::stop("invalid booster pointer");
  int n = 0;
  check(p_booster_num_classes(b->h, &n), "LGBM_BoosterGetNumClasses");
  return n;
}

// [[Rcpp::export]]
Rcpp::NumericVector cpp_importance(SEXP booster) {
  require_loaded();
  if (!p_booster_importance || !p_booster_num_feature) {
    Rcpp::stop("this LightGBM build lacks feature-importance symbols");
  }
  BoosterHandle *b = (BoosterHandle *) R_ExternalPtrAddr(booster);
  if (!b || !b->h) Rcpp::stop("invalid booster pointer");
  int nfeat = 0;
  check(p_booster_num_feature(b->h, &nfeat), "LGBM_BoosterGetNumFeature");
  Rcpp::NumericVector out(nfeat);
  check(p_booster_importance(b->h, -1, 1, REAL(out)),
        "LGBM_BoosterFeatureImportance");
  return out;
}

// [[Rcpp::export]]
void cpp_save(SEXP booster, std::string path) {
  require_loaded();
  BoosterHandle *b = (BoosterHandle *) R_ExternalPtrAddr(booster);
  if (!b || !b->h) Rcpp::stop("invalid booster pointer");
  check(p_booster_save(b->h, 0, -1, 0, path.c_str()), "LGBM_BoosterSaveModel");
}

// [[Rcpp::export]]
SEXP cpp_load(std::string path) {
  require_loaded();
  if (!p_booster_load) Rcpp::stop("this LightGBM build has no LGBM_BoosterCreateFromModelfile()");
  void *b = nullptr;
  int niter = 0;
  check(p_booster_load(path.c_str(), &niter, &b), "LGBM_BoosterCreateFromModelfile");
  return BoosterXPtr(new BoosterHandle{b}, true);
}
