#include <iostream>
#include <parquet/column_reader.h>
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

template <typename T, typename ReaderType>
double SumColumn(ReaderType* reader, int rowGroupNumRows) {
    double sum = 0;
    while (reader->HasNext()) {
        std::vector<T> values(10000);
        int16_t definition_level;
        int16_t repetition_level;
        int64_t values_read = 0;
        int64_t rows_read = reader->ReadBatch(10000, nullptr, nullptr, values.data(), &values_read);
        // int64_t rows_read = reader->ReadBatch(100, &definition_level, &repetition_level, values.data(), &values_read);
        // if (rows_read > 0 && definition_level >= reader->descr()->max_definition_level()) {
            for (int i = 0; i < rows_read; i++)
              sum += values[i];
        // }
    }
    return sum;
}

int main(int argc, char** argv) {
    if (argc != 3) {
        std::cerr << "Usage: " << argv[0] << " <file> <column_name>" << std::endl;
        return 1;
    }

    std::string file_path = argv[1];
    std::string column_name = argv[2];

    try {

        clock_t t = clock();

        std::shared_ptr<parquet::ParquetFileReader> parquet_reader =
            parquet::ParquetFileReader::OpenFile(file_path, false);

        std::shared_ptr<parquet::FileMetaData> file_metadata = parquet_reader->metadata();
        int num_row_groups = file_metadata->num_row_groups();

        int column_index = -1;
        for (int i = 0; i < file_metadata->schema()->group_node()->field_count(); ++i) {
            if (file_metadata->schema()->Column(i)->name() == column_name) {
                column_index = i;
                break;
            }
        }

        t = print_time_taken(t, "Time taken for load: ");

        if (column_index == -1) {
            std::cerr << "Column not found: " << column_name << std::endl;
            return 1;
        }

        double total_sum = 0;

        for (int r = 0; r < num_row_groups; ++r) {
            std::shared_ptr<parquet::RowGroupReader> row_group_reader = parquet_reader->RowGroup(r);
            std::shared_ptr<parquet::ColumnReader> column_reader = row_group_reader->Column(column_index);
            auto rgMetaData = row_group_reader->metadata();
            int rowGroupNumRows = rgMetaData->num_rows();

            parquet::Type::type column_type = column_reader->descr()->physical_type();

            switch (column_type) {
                case parquet::Type::INT32: {
                    auto* int32_reader = dynamic_cast<parquet::Int32Reader*>(column_reader.get());
                    total_sum += SumColumn<int32_t, parquet::Int32Reader>(int32_reader, rowGroupNumRows);
                    break;
                }
                case parquet::Type::INT64: {
                    auto* int64_reader = dynamic_cast<parquet::Int64Reader*>(column_reader.get());
                    total_sum += SumColumn<int64_t, parquet::Int64Reader>(int64_reader, rowGroupNumRows);
                    break;
                }
                case parquet::Type::FLOAT: {
                    auto* float_reader = dynamic_cast<parquet::FloatReader*>(column_reader.get());
                    total_sum += SumColumn<float, parquet::FloatReader>(float_reader, rowGroupNumRows);
                    break;
                }
                case parquet::Type::DOUBLE: {
                    auto* double_reader = dynamic_cast<parquet::DoubleReader*>(column_reader.get());
                    total_sum += SumColumn<double, parquet::DoubleReader>(double_reader, rowGroupNumRows);
                    break;
                }
                default:
                    std::cerr << "Column is not of a numeric type: " << column_name << std::endl;
                    return 1;
            }
        }

        t = print_time_taken(t, "Time taken for sum: ");
        printf("Sum of column %s: %.2f\n", column_name.c_str(), total_sum);

    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}

