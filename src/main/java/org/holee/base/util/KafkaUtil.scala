package org.holee.base.util

import java.util
import java.util.Properties
import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConverters._
import org.apache.flink.api.common.serialization.{AbstractDeserializationSchema, SimpleStringSchema}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.apache.flink.util.StringUtils
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.holee.base.common.Consts

import scala.collection.JavaConversions.mapAsScalaMap

/**
 * @author qimian.huang 黄奇冕
 * @create 2021-02-04 17:55
 * @Description
 */

object KafkaUtil {
    var kafkaProMap = new ConcurrentHashMap[String, util.HashMap[String,String]]    // <生产者代号，<配置项，value>>
    var kafkaConMap = new ConcurrentHashMap[String, util.HashMap[String,String]]    // <消费者代号，<配置项，value>>
    @volatile private var initTag = false
    private val lock = new Object

    // Byte 序列化器
    class ByteArrayDeserializationSchema[T] extends AbstractDeserializationSchema[Array[Byte]]{
        override def deserialize(message: Array[Byte]): Array[Byte] = message
    }

    def init(conf: ParameterTool): Unit = {
        if(!initTag){
            lock.synchronized({
                if(!initTag){
                    for((k,v) <- conf.toMap){
                        if (k.startsWith(Consts.KAFKA_PRODUCER_START) && !StringUtils.isNullOrWhitespaceOnly(v)) {
                            val kafkaNm = k.substring(Consts.KAFKA_PRODUCER_START.length).split("\\.")(0)
                            val confNm = k.substring(Consts.KAFKA_PRODUCER_START.length + kafkaNm.length + 1)
                            kafkaProMap.putIfAbsent(kafkaNm, new util.HashMap[String, String])
                            kafkaProMap.get(kafkaNm).put(confNm, v)
                        }
                        if (k.startsWith(Consts.KAFKA_CONSUMER_START) && !StringUtils.isNullOrWhitespaceOnly(v)) {
                            val kafkaNm = k.substring(Consts.KAFKA_CONSUMER_START.length).split("\\.")(0)
                            val confNm = k.substring(Consts.KAFKA_CONSUMER_START.length + kafkaNm.length + 1)
                            kafkaConMap.putIfAbsent(kafkaNm, new util.HashMap[String, String])
                            kafkaConMap.get(kafkaNm).put(confNm, v)
                        }
                    }
                    initTag = true
                }
            })
        }
    }

    def getFlinkConsumerByte(kafkaNm: String): FlinkKafkaConsumer[Array[Byte]] = {
        val consumerMap = kafkaConMap.get(kafkaNm)
        val topicList = consumerMap.get(Consts.KAFKA_TOPIC).split(",").toList.asJava
        val offset = consumerMap.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)
        val consumer = new FlinkKafkaConsumer[Array[Byte]](topicList, new ByteArrayDeserializationSchema(), getConsumerProp(kafkaNm))
        offset match {
            case "earliest" => consumer.setStartFromEarliest()
            case "latest" => consumer.setStartFromLatest()
            case _ => consumer.setStartFromGroupOffsets()
        }
        consumer
    }

    def getFlinkConsumerString(kafkaNm: String): FlinkKafkaConsumer[String] = {
        val consumerMap = kafkaConMap.get(kafkaNm)
        val topicList = consumerMap.get(Consts.KAFKA_TOPIC).split(",").toList.asJava
        val offset = consumerMap.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)
        val consumer = new FlinkKafkaConsumer[String](topicList, new SimpleStringSchema, getConsumerProp(kafkaNm))
        offset match {
            case "earliest" => consumer.setStartFromEarliest()
            case "latest" => consumer.setStartFromLatest()
            case _ => consumer.setStartFromGroupOffsets()
        }
        consumer
    }

    def getFlinkProducer(kafkaNm: String): FlinkKafkaProducer[String] = {
        null
    }

    def getTopic(kafkaNm: String): String = {
        kafkaProMap.get(kafkaNm).getOrDefault(Consts.KAFKA_TOPIC,"")
    }

    private def getConsumerProp(kafkaNm: String): Properties = {
        val consumerMap = kafkaConMap.get(kafkaNm)
        val prop = new Properties()
        prop.putAll(consumerMap)
        prop.remove(Consts.KAFKA_TOPIC)
        prop.remove(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)
        if (!prop.containsKey(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG)){
            prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
        }
        if(!prop.containsKey(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG)){
            prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
        }

        // 认证信息
        if(prop.containsKey(Consts.USER_NAME) && prop.containsKey(Consts.PASSWORD)) {
            val userName = prop.getProperty(Consts.USER_NAME)
            val password = prop.getProperty(Consts.PASSWORD)
            prop.remove(Consts.USER_NAME)
            prop.remove(Consts.PASSWORD)

            prop.put("security.protocol", "SASL_PLAINTEXT")
            prop.put("sasl.mechanism", "PLAIN")
            prop.put("sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";".format(userName, password))
        }
        prop
    }

    private def getProducerProp(kafkaNm: String): Properties = {
        null
    }

}
