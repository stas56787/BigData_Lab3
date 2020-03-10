import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import scala.Function1;
import scala.Serializable;

import java.util.function.Function;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.sum;

public class Runner {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir","C:\\hadoop" );
        String path = "input/sample.txt";

        LogProcessing logProcessing = new LogProcessing(path);

//          Реализация на основе dataset
//        SparkConf conf = new SparkConf().setAppName("Request reader")
//                .setMaster("local[*]").set("spark.driver.bindAddress", "127.0.0.1");
//
//        SparkSession sqlContext = SparkSession.builder()
//                .master("local[*]")
//                .appName("Request reader")
//                .sparkContext(new SparkContext(conf))
//                .getOrCreate();
//
//        Dataset<String> dataset = sqlContext.read().textFile(path);
//        dataset.show();
//        Dataset<RequestInformation> requests
//                = dataset.map((Function1<String, RequestInformation> & Serializable) RequestInformation::new, Encoders.bean(RequestInformation.class));
//        requests.groupBy("id").agg(sum("byteSum"), avg("byteSum")).show();
        JavaRDD<RequestInformation> requestsData = logProcessing.getRequestsInfo();
        JavaPairRDD<String, Integer> countData = logProcessing.getBrowserInfo();

        countData.foreach(data1 -> {
            System.out.println("Browser: "+ data1._1() + " count: " + data1._2());
        });

        try {
            requestsData.coalesce(1).saveAsTextFile("Requests info");
        }
        catch (Exception ex) {
            System.out.println(ex.getMessage());
        }

    }
}
