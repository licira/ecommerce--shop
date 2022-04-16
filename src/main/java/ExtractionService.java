import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.split;

public class ExtractionService {

    private final SparkSession sparkSession;

    public ExtractionService(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

    public Dataset<Row> process(final Dataset<Row> dataset) {
        return dataset.withColumn("ConsumptionTotalTextArray",
                split(col("ConsumptionTotalText"), StringUtils.SPACE))
                .withColumn("extracted-value-ConsumptionTotalText", col("ConsumptionTotalTextArray").apply(0))
                .withColumn("extracted-unit-ConsumptionTotalText", col("ConsumptionTotalTextArray").apply(1))
                .drop("ConsumptionTotalTextArray", "ConsumptionTotalText");
    }
}
