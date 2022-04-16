import io.netty.resolver.dns.DnsCache;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;

import java.util.HashMap;
import java.util.Map;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.types.DataTypes.StringType;

public class NormalisationService {

    private static final Map<String, String> COLOR_TRANSLATION_MAP = new HashMap<String, String>() {{
        put("schwarz mét.", "black met.");
        put("orange", "orange");
        put("bordeaux", "bordeaux");
        put("grün", "green");
        put("schwarz", "black");
        put("braun mét.", "brown met.");
        put("rot mét.", "red met");
        put("grau", "gray");
        put("gelb", "yellow");
        put("braun", "brown");
        put("weiss", "white");
        put("gelb mét.", "yellow met.");
        put("gold mét.", "gold met.");
        put("anthrazit mét.", "anthrazit met.");
        put("blau", "blue");
        put("gold", "gold");
        put("beige", "beige");
        put("grün mét.", "green met.");
        put("weiss mét.", "white met.");
        put("grau mét.", "gray met.");
        put("violett mét.", "violet met.");
        put("blau mét.", "blue met.");
        put("silber mét.", "silver met.");
        put("orange mét.", "orange met.");
        put("silber", "silver");
        put("beige mét.", "beige met.");
        put("anthrazit", "anthrazit");
        put("bordeaux mét.", "bordeaux met.");
        put("rot", "red");
    }};

    private static final UDF1<String, String> COLOR_TRANSLATION_UDF =
            (UDF1<String, String>) bodyColorText -> COLOR_TRANSLATION_MAP.getOrDefault(bodyColorText, "null");


    private static final Map<String, String> TRANSMISSION_TYPE_TRANSLATION_MAP = new HashMap<String, String>() {{
        put("Schaltgetriebe", "manual");
        put("Automatisiertes Schaltgetriebe", "automatic");
        put("Schaltgetriebe manuell", "manual");
        put("Automat", "automatic");
    }};

    final UDF1<String, String> TRANSMISSION_TYPE_TRANSLATION_UDF =
            (UDF1<String, String>) transmissionTypeText -> TRANSMISSION_TYPE_TRANSLATION_MAP.getOrDefault(transmissionTypeText, "null");

    private final SparkSession sparkSession;

    public NormalisationService(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
        this.sparkSession.sqlContext()
                .udf()
                .register("colorTranslationUdf", COLOR_TRANSLATION_UDF, StringType);
        this.sparkSession.sqlContext()
                .udf()
                .register("transmissionTypeTranslationUdf", TRANSMISSION_TYPE_TRANSLATION_UDF, StringType);
    }

    public Dataset<Row> process(final Dataset<Row> dataset) {
        return dataset.withColumn("color",
                callUDF("colorTranslationUdf", col("BodyColorText")))
                .drop("BodyColorText")
                .withColumn("transmissionType",
                        callUDF("transmissionTypeTranslationUdf", col("TransmissionTypeText")))
                .withColumn("make",
                        concat(expr("substring(MakeText, 0, 1)"),
                                lower(expr("substring(MakeText, 2, length(MakeText))"))))
                .drop("MakeText");
    }
}
