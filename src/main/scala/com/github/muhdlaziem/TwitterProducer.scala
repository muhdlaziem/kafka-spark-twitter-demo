package com.github.muhdlaziem

import com.twitter.hbc.ClientBuilder
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint
import com.twitter.hbc.core.processor.StringDelimitedProcessor
import com.twitter.hbc.core.{Client, Constants, Hosts, HttpHosts}
import com.twitter.hbc.httpclient.auth.{Authentication, OAuth1}
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.{Logger, LoggerFactory}

import java.util.Properties
import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue, TimeUnit}
import scala.jdk.CollectionConverters._

object TwitterProducer {
  private val consumerKey: String = "KC0TwUuxW6LHynIyVrg5lUPbx"
  private val consumerSecret: String = "RYz65GZYBfAbFvxdIMrkkiZ0X4oSLhMZkwepWjXzb53PJFHhCe"
  private val token: String = "413815311-tfVFxgSQstKlsP99lQgDeFcmLRR84S0tKvHAuj1L"
  private val secret: String = "t4uov6DTFxWuhiIB2YB1zJ6IVfh8HZUpcEk71sUvmNKK5"
  private val interestList: List[String] = List("Ronaldo")
  val log: Logger = LoggerFactory.getLogger(TwitterProducer.getClass.getName)

  def main(args: Array[String]): Unit = {
    TwitterProducer.run()
  }
  def run(): Unit ={
    log.info("Setup")

    val msgQueue: BlockingQueue[String] = new LinkedBlockingQueue[String](1000)

    val client: Client = createTwitterClient(msgQueue)

    client.connect()

    val producer: KafkaProducer[String, String] = createKafkaProducer()

    sys.ShutdownHookThread {
      log.info("Stopping Application")
      log.info("Shutting down client from twitter...")
      client.stop()
      log.info("Closing producer...")
      producer.close()
      log.info("Done !.")
    }

    while (!client.isDone) {

      try {
        val msg: String = msgQueue.poll(5, TimeUnit.SECONDS)
         if(msg != null) {

           log.info(msg)

           producer.send(new ProducerRecord[String, String]("tweets", null, msg), new Callback {
             override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {

               if (exception != null) {
                 log.error("Something bad happened", exception)
               }
             }
           })
         }
      }
      catch {
        case exception: InterruptedException => {
          exception.printStackTrace()
          client.stop()
        }
      }
    }
    log.info("End of application")
  }

  def createTwitterClient(msgQueue: BlockingQueue[String]): Client ={
    /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
    val hoseBirdHosts: Hosts = new HttpHosts(Constants.STREAM_HOST)
    val hoseBirdEndPoint: StatusesFilterEndpoint = new StatusesFilterEndpoint()

    hoseBirdEndPoint.trackTerms(interestList.asJava)

    // These secrets should be read from a config file
    val hoseBirdAuth: Authentication = new OAuth1(consumerKey,consumerSecret, token, secret)

    val builder: ClientBuilder = new ClientBuilder()
      .name("Hosebird-Client-01") // optional: mainly for the logs
      .hosts(hoseBirdHosts)
      .authentication(hoseBirdAuth)
      .endpoint(hoseBirdEndPoint)
      .processor(new StringDelimitedProcessor(msgQueue))

    builder.build()
  }

  def createKafkaProducer(): KafkaProducer[String, String] = {

    // create producer properties
    val config: Properties = new Properties()
    config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

    // create safe Producer
    config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
    config.put(ProducerConfig.ACKS_CONFIG, "all")
    config.put(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE))
    config.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5") // kafka 2.0 >= 1.1 so we can keep this as 5. Use 1 otherwise.

    // high throughput producer (at the expense of a bit latency and CPU Usage)
    config.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy")
    config.put(ProducerConfig.LINGER_MS_CONFIG, "20")
    config.put(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024))  // 32KB batch size

    new KafkaProducer[String, String](config)
  }
}
