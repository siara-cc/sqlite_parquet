#include <iostream>
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/file_reader.h>
#include <parquet/properties.h>
#include <sys/stat.h>
#include <stdio.h>

double time_taken_in_secs(clock_t t) {
  t = clock() - t;
  return ((double)t)/CLOCKS_PER_SEC;
}

clock_t print_time_taken(clock_t t, const char *msg) {
  double time_taken = time_taken_in_secs(t); // in seconds
  std::cout << msg << time_taken << std::endl;
  return clock();
}

int main(int argc, char** argv) {
    if (argc != 3) {
        std::cerr << "Usage: " << argv[0] << " <file> <column_name>" << std::endl;
        return 1;
    }

    std::string file_path = argv[1];
    std::string column_name = argv[2];

    clock_t t = clock();

    std::shared_ptr<arrow::io::ReadableFile> infile;
    PARQUET_ASSIGN_OR_THROW(
        infile,
        arrow::io::ReadableFile::Open(file_path));

    std::unique_ptr<parquet::arrow::FileReader> reader;
    PARQUET_THROW_NOT_OK(
        parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader));

    std::shared_ptr<arrow::Table> table;
    PARQUET_THROW_NOT_OK(reader->ReadTable(&table));

    auto schema = table->schema();
    int col_idx = schema->GetFieldIndex(column_name);
    if (col_idx == -1) {
        std::cerr << "Column not found: " << column_name << std::endl;
        return 1;
    }

    t = print_time_taken(t, "Time taken for load: ");

    std::shared_ptr<arrow::ChunkedArray> column = table->column(col_idx);

    double sum = 0;
    for (const auto& chunk : column->chunks()) {
        switch (column->type()->id()) {
          case arrow::Type::INT8:
          case arrow::Type::INT16:
          case arrow::Type::INT32:
          case arrow::Type::INT64: {
        auto array = std::static_pointer_cast<arrow::Int32Array>(chunk);
        for (int64_t i = 0; i < array->length(); ++i) {
            if (!array->IsNull(i)) {
                sum += array->Value(i);
            }
        } }
           break;
          case arrow::Type::DOUBLE: {
        auto array = std::static_pointer_cast<arrow::DoubleArray>(chunk);
        for (int64_t i = 0; i < array->length(); ++i) {
            if (!array->IsNull(i)) {
                sum += array->Value(i);
            }
        } }
        }
    }

    t = print_time_taken(t, "Time taken for sum: ");
    printf("Sum of column %s: %.2f\n", column_name.c_str(), sum);

    return 0;
}

