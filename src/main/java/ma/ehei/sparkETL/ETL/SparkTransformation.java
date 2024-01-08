package ma.ehei.sparkETL.ETL;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class SparkTransformation {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("SparkEtlApplication")
                .config("spark.master", "local[*]")
                .getOrCreate();

        // Read CSV data into a DataFrame
        String csvFilePath = "D:\\EHEI\\5EME_ANNEE\\NoSQL\\sparkETL\\src\\main\\resources\\data\\Employee.csv";
        Dataset<Row> rawData = spark.read().csv(csvFilePath);

        // Apply transformations to create new calculated fields
        Dataset<Row> processedData = rawData
                .withColumn("SalaryWithBonus", rawData.col("Salary").plus(10000))
                .withColumn("DepartmentUpperCase", functions.upper(rawData.col("Department")))
                .withColumn("HiringDate", functions.to_date(rawData.col("HiringDate"), "yyyy-MM-dd"))
                .withColumn("Experience", rawData.col("HiringDate")
                        .divide(365.0)  // Divide by a double value
                        .cast("double"));  // Ensure the result is cast to double

        // Show the processed data
        processedData.show();
    }
}
