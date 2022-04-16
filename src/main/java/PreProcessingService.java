import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import scala.collection.mutable.WrappedArray;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.lit;

public class PreProcessingService {

    final MapFunction<Row, Row> PREPROCESSING_FUNC = row -> {

        final WrappedArray.ofRef attributes = (WrappedArray.ofRef) row.apply(6);

        String properties = null;
        String interiorColorText = null;
        String consumptionRatingText = null;
        String km = null;
        String bodyColorText = null;
        String transmissionTypeText = null;
        String seats = null;
        String driveTypeText = null;
        String city = null;
        String ccm = null;
        String firstRegMonth = null;
        String firstRegYear = null;
        String fuelTypeText = null;
        String consumptionTotalText = null;
        String co2EmissionText = null;
        String hp = null;
        String doors = null;
        String bodyTextType = null;
        String bodyTypeText = null;
        String conditionTypeText = null;

        for (int i = 0; i < attributes.length(); i++) {

            final Row attribute = (Row) attributes.apply(i);

            final String attributeName = attribute.getString(0);

            switch (attributeName) {
                case "Properties":
                    properties = attribute.getString(1);
                    break;
                case "InteriorColorText":
                    interiorColorText = attribute.getString(1);
                    break;
                case "ConsumptionRatingText":
                    consumptionRatingText = attribute.getString(1);
                    break;
                case "Km":
                    km = attribute.getString(1);
                    break;
                case "BodyColorText":
                    bodyColorText = attribute.getString(1);
                    break;
                case "TransmissionTypeText":
                    transmissionTypeText = attribute.getString(1);
                    break;
                case "Seats":
                    seats = attribute.getString(1);
                    break;
                case "DriveTypeText":
                    driveTypeText = attribute.getString(1);
                    break;
                case "City":
                    city = attribute.getString(1);
                    break;
                case "Ccm":
                    ccm = attribute.getString(1);
                    break;
                case "FirstRegMonth":
                    firstRegMonth = attribute.getString(1);
                    break;
                case "FirstRegYear":
                    firstRegYear = attribute.getString(1);
                    break;
                case "FuelTypeText":
                    fuelTypeText = attribute.getString(1);
                    break;
                case "ConsumptionTotalText":
                    consumptionTotalText = attribute.getString(1);
                    break;
                case "Co2EmissionText":
                    co2EmissionText = attribute.getString(1);
                    break;
                case "Hp":
                    hp = attribute.getString(1);
                    break;
                case "Doors":
                    doors = attribute.getString(1);
                    break;
                case "BodyTypeText":
                    bodyTypeText = attribute.getString(1);
                    break;
                case "ConditionTypeText":
                    conditionTypeText = attribute.getString(1);
                    break;
            }
        }

        return RowFactory.create(row.apply(0),
                row.apply(1),
                row.apply(2),
                row.apply(3),
                row.apply(4),
                row.apply(5),
                attributes,
                properties,
                interiorColorText,
                consumptionRatingText,
                km,
                bodyColorText,
                transmissionTypeText,
                seats,
                driveTypeText,
                city,
                ccm,
                firstRegMonth,
                firstRegYear,
                fuelTypeText,
                consumptionTotalText,
                co2EmissionText,
                hp,
                doors,
                bodyTextType,
                bodyTypeText,
                conditionTypeText);
    };

    private final SparkSession sparkSession;

    public PreProcessingService(final SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

    public Dataset<Row> process(final Dataset<Row> supplierData) {
        Dataset<Row> preProcessedData = supplierData.groupBy(col("id"))
                .agg(first("MakeText").as("MakeText"),
                        first("TypeName").as("TypeName"),
                        first("TypeNameFull").as("TypeNameFull"),
                        first("ModelText").as("ModelText"),
                        first("ModelTypeText").as("ModelTypeText"),
                        collect_list(struct(col("Attribute Names").as("attribute_names"),
                                col("Attribute Values").as("attribute_values"),
                                col("entity_id")))
                                .as("Attributes"));

        preProcessedData = preProcessedData.withColumn("Properties", lit(null).cast("string"))
                .withColumn("InteriorColorText", lit(null).cast("string"))
                .withColumn("ConsumptionRatingText", lit(null).cast("string"))
                .withColumn("Km", lit(null).cast("string"))
                .withColumn("BodyColorText", lit(null).cast("string"))
                .withColumn("TransmissionTypeText", lit(null).cast("string"))
                .withColumn("Seats", lit(null).cast("string"))
                .withColumn("DriveTypeText", lit(null).cast("string"))
                .withColumn("City", lit(null).cast("string"))
                .withColumn("Ccm", lit(null).cast("string"))
                .withColumn("FirstRegMonth", lit(null).cast("string"))
                .withColumn("FirstRegYear", lit(null).cast("string"))
                .withColumn("FuelTypeText", lit(null).cast("string"))
                .withColumn("ConsumptionTotalText", lit(null).cast("string"))
                .withColumn("Co2EmissionText", lit(null).cast("string"))
                .withColumn("Hp", lit(null).cast("string"))
                .withColumn("Doors", lit(null).cast("string"))
                .withColumn("BodyTypeText", lit(null).cast("string"))
                .withColumn("ConditionTypeText", lit(null).cast("string"));

        return preProcessedData.map(PREPROCESSING_FUNC, RowEncoder.apply(preProcessedData.schema()))
                .drop("attributes");
    }
}
