import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class CsvWriteService {

    private final String encoding;

    public CsvWriteService(final String encoding) {
        this.encoding = encoding;
    }

    public void save(final Dataset<Row> dataset,
                     final String path) {
        dataset.coalesce(1)
                .write()
                .option("encoding", encoding)
                .option("header", "true")
                .mode("Overwrite")
                .csv(path);
    }
}
