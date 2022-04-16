import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class IntegrationService {

    private final SparkSession sparkSession;

    public IntegrationService(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

    public Dataset<Row> process(final Dataset<Row> dataset) {
        return dataset
                // drop unnecessary comments
                .drop("id", "TypeNameFull", "properties", "ModelTypeText", "Seats", "InteriorColorText",
                        "DriveTypeText", "ccm", "FuelTypeText", "Co2EmissionText", "Hp", "Doors",
                        "FirstRegYear", "FirstRegMonth", "ConsumptionRatingText",
                        "extracted-value-ConsumptionTotalText")
                // replace ModelText with properly cased mode
                .withColumn("model",
                        concat(expr("substring(ModelText, 0, 1)"),
                                lower(expr("substring(ModelText, 2, length(ModelText))"))))
                .drop("ModelText")
                // rename columns
                .withColumnRenamed("TypeName", "model_variant")
                .withColumnRenamed("City", "city")
                .withColumnRenamed("ConditionTypeText", "carType")
                .withColumnRenamed("extracted-unit-ConsumptionTotalText", "fuel_consumption_unit")
                .withColumn("fuel_consumption_unit",
                        when(col("fuel_consumption_unit").equalTo("l/100km"), "l_km_consumption")
                                .otherwise(null))
                //.withColumnRenamed("Km", "mileage")
                .withColumn("mileage", col("Km").cast("double"))
                .drop("Km")
                // add column with literal "kilometer" - given that milleage is initially named "km"
                .withColumn("mileage_unit", lit("kilometer"))
                // add column with literal "car" - given that all records from the expected csv have this value
                .withColumn("type", lit("car"))
                // add column with literal "false" - default value
                .withColumn("price_on_request", lit("false"))
                // add target values
                .withColumn("condition", lit(null).cast("string"))
                .withColumn("currency", lit(null).cast("string"))
                .withColumn("drive", lit(null).cast("string"))
                .withColumn("country", lit(null).cast("string"))
                .withColumn("zip", lit(null).cast("string"))
                .withColumn("manufacture_year", lit(null).cast("string"))
                .withColumn("manufacture_month", lit(null).cast("string"))
                .drop("TransmissionTypeText");
    }
}
