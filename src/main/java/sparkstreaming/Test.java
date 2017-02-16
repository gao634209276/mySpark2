package sparkstreaming;

import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.*;

/**
 */
public class Test {
	public static void main(String[] args) throws InterruptedException {
		SparkConf conf = new SparkConf();
		conf.setMaster("local[2]").setAppName("Kafka2SparkStreaming");
		JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(2));
		/*Map<String, Integer> topicConsumerConcurrency = new HashMap<String, Integer>();
		topicConsumerConcurrency.put("message", 1);
		JavaPairReceiverInputDStream<String, String> lines = KafkaUtils.createStream(jsc,
				"10.40.33.11:2181/kafka", "message", topicConsumerConcurrency);*/


	Map<String, String> kafkaParameters = new HashMap<String, String>();
	kafkaParameters.put("metadata.broker.list", "10.40.33.11:9092,10.40.33.12:9092");
		kafkaParameters.put("bootstrap.servers","10.40.33.11:9092,10.40.33.12:9092");
	Set<String> topics = new HashSet<String>();
	topics.add("message");
	JavaPairInputDStream<String, String> lines = KafkaUtils.createDirectStream(jsc,
			String.class, String.class,
			StringDecoder.class, StringDecoder.class,
			kafkaParameters, topics);

		JavaDStream<String> words = lines.flatMap(new FlatMapFunction<Tuple2<String, String>, String>() {

			public Iterator<String> call(Tuple2<String, String> stringStringTuple2) throws Exception {
				return Arrays.asList(stringStringTuple2._2.split(" ")).iterator();
			}
		});
		words.print();
	/*	words.foreachRDD(new VoidFunction<JavaRDD<String>>() {
			public void call(JavaRDD<String> stringJavaRDD) throws Exception {
				Iterator<String> it = stringJavaRDD.collect().iterator();
				while (it.hasNext()){
					System.out.println(it.next());
				}
			}
		});*/
		jsc.start();
		jsc.awaitTermination();
		jsc.close();

	}
}