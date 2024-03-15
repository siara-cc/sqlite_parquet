package cc.siara.data_export;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.column.ParquetProperties;
import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_2_0;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.GZIP;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.SNAPPY;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.UNCOMPRESSED;

import java.io.File;
import java.io.IOException;
import java.sql.*;

public class SQLiteToParquet {

    static String getColType(int colType) {
        switch (colType) {
            case Types.INTEGER:
                return "int32";
            case Types.VARCHAR:
                return "binary";
            case Types.FLOAT:
                return "float";
        }
        return "";
    }

    static String getEncoding(int colType) {
        switch (colType) {
            case Types.INTEGER:
                return "INT32";
            case Types.VARCHAR:
                return "DELTA_BYTE_ARRAY";
            case Types.FLOAT:
                return "float";
        }
        return "";
    }

    public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {

        if (args.length == 0) {
            System.out.println("Usage: java -jar SqliteToParquet <sqlite database> <table name> <storage types>");
            return;
        }

        // Load SQLite JDBC driver
        Class.forName("org.sqlite.JDBC");

        // JDBC connection URL
        String connectionUrl = "jdbc:sqlite:" + args[0];

        // SQL query to fetch data
        String query = "SELECT * FROM " + args[1];

        // Connect to SQLite database
        try (Connection connection = DriverManager.getConnection(connectionUrl);
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(query)) {

            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();

            // Define schema with column encodings
            StringBuilder schemaString = new StringBuilder("message " + args[1] + " {\n");

            // Print column names
            for (int i = 1; i <= columnCount; i++) {
                //String enc = getEncoding(metaData.getColumnType(i));
                String colType = getColType(metaData.getColumnType(i));
                schemaString.append("  required ")
                    .append(colType).append(" ")
                    .append(metaData.getColumnName(i))
                    //.append(" (").append(enc).append(")")
                    .append(";\n");
            }
            schemaString.append("}");
            System.out.println(schemaString);
            MessageType schema = MessageTypeParser.parseMessageType(schemaString.toString());

            // Output Parquet file path
            File file = new File("data.parquet");
            file.delete();
            Path outputPath = new Path("data.parquet");

            // Create Parquet writer with specific encodings
            Configuration conf = new Configuration();
            GroupWriteSupport.setSchema(schema, conf);
            SimpleGroupFactory f = new SimpleGroupFactory(schema);
            ParquetWriter<Group> writer = new ParquetWriter<Group>(
                outputPath,
                new GroupWriteSupport(),
                UNCOMPRESSED,
                256 * 1024 * 1024,
                4 * 1024 * 1024,
                512,
                false,
                true,
                PARQUET_2_0,
                conf);

            // Iterate over the result set and write data to Parquet file
            while (resultSet.next()) {
                Group group = new SimpleGroup(schema);
                for (int i = 1; i <= columnCount; i++) {
                    switch (metaData.getColumnType(i)) {
                      case Types.INTEGER:
                        group.add(metaData.getColumnName(i), resultSet.getInt(i));
                        break;
                      case Types.VARCHAR:
                        String s = resultSet.getString(i);
                        if (s == null)
                            group.add(metaData.getColumnName(i), "");
                        else
                            group.add(metaData.getColumnName(i), s);
                        break;
                      case Types.FLOAT:
                        group.add(metaData.getColumnName(i), resultSet.getFloat(i));
                        break;
                    }
                }
                writer.write(group);
            }

            // Close the Parquet writer
            writer.close();

        }

    }
}
