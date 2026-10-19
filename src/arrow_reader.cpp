#include <Rcpp.h>
#include <arrow/api.h>
#include <arrow/io/file.h>
#include <parquet/arrow/reader.h>
#include <memory>
#include <string>
#include <vector>

struct ArrowStream {
  std::shared_ptr<arrow::io::ReadableFile> file;
  std::unique_ptr<parquet::arrow::FileReader> reader;
  std::shared_ptr<arrow::RecordBatchReader> batches;
  std::vector<std::string> names;
  int64_t offset = 0;
};

static Rcpp::NumericVector arrow_column(const std::shared_ptr<arrow::Array> &a) {
  const int64_t n = a->length();
  Rcpp::NumericVector out(n);
  switch (a->type_id()) {
  case arrow::Type::DOUBLE: {
    auto v = std::static_pointer_cast<arrow::DoubleArray>(a);
    for (int64_t i = 0; i < n; ++i) out[i] = v->IsNull(i) ? NA_REAL : v->Value(i);
    break;
  }
  case arrow::Type::FLOAT: {
    auto v = std::static_pointer_cast<arrow::FloatArray>(a);
    for (int64_t i = 0; i < n; ++i) out[i] = v->IsNull(i) ? NA_REAL : v->Value(i);
    break;
  }
  case arrow::Type::INT32: {
    auto v = std::static_pointer_cast<arrow::Int32Array>(a);
    for (int64_t i = 0; i < n; ++i) out[i] = v->IsNull(i) ? NA_REAL : v->Value(i);
    break;
  }
  case arrow::Type::INT64: {
    auto v = std::static_pointer_cast<arrow::Int64Array>(a);
    for (int64_t i = 0; i < n; ++i) out[i] = v->IsNull(i) ? NA_REAL : static_cast<double>(v->Value(i));
    break;
  }
  case arrow::Type::BOOL: {
    auto v = std::static_pointer_cast<arrow::BooleanArray>(a);
    for (int64_t i = 0; i < n; ++i) out[i] = v->IsNull(i) ? NA_REAL : (v->Value(i) ? 1.0 : 0.0);
    break;
  }
  default:
    Rcpp::stop("Arrow column type is not numeric: %s", a->type()->ToString());
  }
  return out;
}

// [[Rcpp::export]]
SEXP cpp_arrow_open(std::string path, Rcpp::CharacterVector columns) {
  auto fr = arrow::io::ReadableFile::Open(path);
  if (!fr.ok()) Rcpp::stop("Arrow open failed: %s", fr.status().ToString());
  auto file = *fr;
  auto rr = parquet::arrow::OpenFile(file, arrow::default_memory_pool());
  if (!rr.ok()) Rcpp::stop("Parquet open failed: %s", rr.status().ToString());
  auto reader = std::move(rr).ValueOrDie();
  std::shared_ptr<arrow::Schema> schema;
  auto st = reader->GetSchema(&schema);
  if (!st.ok()) Rcpp::stop("Parquet schema failed: %s", st.ToString());
  std::vector<std::string> names;
  for (R_xlen_t i = 0; i < columns.size(); ++i) {
    std::string name = Rcpp::as<std::string>(columns[i]);
    if (schema->GetFieldIndex(name) < 0) Rcpp::stop("Column not found: %s", name);
    names.push_back(name);
  }
  auto batches_result = reader->GetRecordBatchReader();
  if (!batches_result.ok()) {
    Rcpp::stop("Arrow batch reader failed: %s", batches_result.status().ToString());
  }
  std::shared_ptr<arrow::RecordBatchReader> batches =
    std::move(batches_result).ValueOrDie();
  ArrowStream *s = new ArrowStream();
  s->file = file;
  s->reader = std::move(reader);
  s->batches = batches;
  s->names = names;
  Rcpp::XPtr<ArrowStream> ptr(s, true);
  ptr.attr("class") = "lgbstream_arrow_reader";
  return ptr;
}

// [[Rcpp::export]]
Rcpp::CharacterVector cpp_arrow_schema(std::string path) {
  auto fr = arrow::io::ReadableFile::Open(path);
  if (!fr.ok()) Rcpp::stop("Arrow open failed: %s", fr.status().ToString());
  auto file = *fr;
  auto pq_reader = parquet::ParquetFileReader::Open(file);
  auto pq_metadata = pq_reader->metadata();
  auto rr = parquet::arrow::OpenFile(file, arrow::default_memory_pool());
  if (!rr.ok()) Rcpp::stop("Parquet open failed: %s", rr.status().ToString());
  auto reader = std::move(rr).ValueOrDie();
  std::shared_ptr<arrow::Schema> schema;
  auto st = reader->GetSchema(&schema);
  if (!st.ok()) Rcpp::stop("Parquet schema failed: %s", st.ToString());
  const int n = schema->num_fields();
  Rcpp::CharacterVector out(n);
  for (int i = 0; i < n; ++i) out[i] = schema->field(i)->name();
  out.attr("nrow") = pq_metadata->num_rows();
  return out;
}

// [[Rcpp::export]]
SEXP cpp_arrow_next(SEXP reader) {
  Rcpp::XPtr<ArrowStream> s(reader);
  std::shared_ptr<arrow::RecordBatch> batch;
  auto st = s->batches->ReadNext(&batch);
  if (!st.ok()) Rcpp::stop("Arrow read failed: %s", st.ToString());
  if (!batch) return R_NilValue;
  const int n = batch->num_rows();
  const int p = static_cast<int>(s->names.size());
  Rcpp::NumericMatrix out(n, p);
  Rcpp::CharacterVector names(p);
  for (int j = 0; j < p; ++j) {
    int idx = batch->schema()->GetFieldIndex(s->names[j]);
    if (idx < 0) Rcpp::stop("Column missing from batch: %s", s->names[j]);
    out.column(j) = arrow_column(batch->column(idx));
    names[j] = s->names[j];
  }
  Rcpp::colnames(out) = names;
  Rcpp::List result = Rcpp::List::create(
    Rcpp::Named("x") = out,
    Rcpp::Named("row_start") = s->offset + 1
  );
  s->offset += n;
  return result;
}
