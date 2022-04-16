import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;

public class SparkPipeline {

    public static void main(final String[] args) {
        if (args.length != 2) {
            throw new IllegalArgumentException("Invalid input parameters");
        }
        final String inputPath = args[0];
        final String outputPath = args[1];

        final SparkSession sparkSession = SparkSession.builder()
                .appName("SparkPipeline")
                .config(getSparkConf(true))
                .getOrCreate();

        final Dataset<Row> supplierData = sparkSession.read()
                .option("inferSchema", "true")
                .option("encoding", "UTF-8")
                .json(inputPath);

        final CsvWriteService csvWriteService = new CsvWriteService("UTF-8");

        // 1. Pre-processing
        final Dataset<Row> preProcessedData = new PreProcessingService(sparkSession)
                .process(supplierData)
                .cache();
        csvWriteService.save(preProcessedData, outputPath + "/preprocessed");

        // 2. Normalisation
        final Dataset<Row> normalisedData = new NormalisationService(sparkSession)
                .process(preProcessedData)
                .cache();
        csvWriteService.save(normalisedData, outputPath + "/normalised");

        // 3. Extraction
        final Dataset<Row> extractedData = new ExtractionService(sparkSession)
                .process(normalisedData)
                .cache();
        csvWriteService.save(extractedData, outputPath + "/extracted");

        // 4. Integration
        final Dataset<Row> integratedData = new IntegrationService(sparkSession)
                .process(extractedData)
                // reorder columns as in the .csv
                .select(col("carType"),
                        col("color"),
                        col("condition"),
                        col("currency"),
                        col("drive"),
                        col("city"),
                        col("country"),
                        col("make"),
                        col("manufacture_year"),
                        col("mileage"),
                        col("mileage_unit"),
                        col("model"),
                        col("model_variant"),
                        col("price_on_request"),
                        col("type"),
                        col("zip"),
                        col("manufacture_month"),
                        col("fuel_consumption_unit"));
        csvWriteService.save(integratedData, outputPath + "/integrated");
    }

    private static SparkConf getSparkConf(boolean runLocal) {
        final SparkConf result = new SparkConf();
        if (runLocal) {
            result.setMaster("local[*]");
        }
        return result;
    }
}
