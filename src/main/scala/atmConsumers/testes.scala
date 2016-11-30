package atmConsumers;
import org.apache.spark._
import org.apache.spark.streaming.kafka._
import kafka.serializer.StringDecoder
import kafka.serializer.DefaultDecoder
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.avro.Schema
import com.twitter.bijection.Injection
import org.apache.avro.generic.GenericRecord
import com.twitter.bijection.avro.GenericAvroCodecs
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.avro.generic.GenericData
import java.util.Properties
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.hbase.client.Result
import org.apache.spark.rdd.RDD




/**
 * @author ${user.name}
 */
object testes {

  def main(args: Array[String]) {
    
    val sparkConf = new SparkConf().setAppName("AtmConsumer").setMaster("local[*]");
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("ERROR");
    val dAtmOperT1 = List((1,23),(2,42),(6,29))
    val dAtmOperT2 = List((1,-3),(5,42),(4,29))
    val dHbase = List((1,10),(2,-12),(3,4),(4,15))
    
    val atmoperT1 = sc.parallelize(dAtmOperT1)
    val atmoperT2 = sc.parallelize(dAtmOperT2)
    var noHbase = sc.parallelize(dHbase)
    
    println("Tabela de totais")
    noHbase.foreach(println)   
    
    
    noHbase = atmoperT1.union(noHbase).reduceByKey((a,b)=>(a+b))    
    println("Vai para a fila")
    noHbase.subtractByKey(noHbase.subtractByKey(atmoperT1)).foreach(println)
    println("Tabela de totais")
    noHbase.foreach(println)
    
    
    noHbase = atmoperT2.union(noHbase).reduceByKey((a,b)=>(a+b))
    println("Vai para a fila")
    noHbase.subtractByKey(noHbase.subtractByKey(atmoperT2)).foreach(println)    
    println("Tabela de totais")
    noHbase.foreach(println)


  }


}
