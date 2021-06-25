package org.holee.base.common

/**
 * @author qimian.huang 黄奇冕
 * @create 2021-02-04 11:33
 * @Description
 */

object Consts {

    //-------------------------------- flink --------------------------------
    val TASK_PARALLELISM = "flink.task.parallelism"         // 并行度
    val CHECKPOINT_SECOND = "flink.checkpoint.second"       // checkpoint 间隔
    val CHECKPOINT_MAX_NUM = "flink.checkpoint.maxnum"      // 同一时间允许的检查点数量
    val JOB_NANE = "flink.job.name"                         // 作业名
    val KRYO_CLASS = "flink.kryo.class"                     // 需要 kryo 序列化注册类，支持逗号分割
    val JOB_ATTEMPTS = "flink.delay.attempts"               // 重启次数
    val JOB_DELAY = "flink.delay.second"                    // 重启延迟秒数
    val JOB_SERVICE = "flink.task.service"                  // 业务逻辑处理类
    val LOCAL_DEBUG = "local.debug"                         // debug 标示，为 true 则固定 8081 ui 端口
    val APP_NAME    = "FlinkJob By Boussole"                // 默认应用名
    val STATE_PATH  = "flink.state.path"                    // stateBackend 地址
    val CONFIG      = "config"                              // 生产环境配置文件脚本参数 key


    //-------------------------------- kafka --------------------------------
    val KAFKA_PRODUCER_START = "kafka.producer."
    val KAFKA_CONSUMER_START = "kafka.consumer."
    val KAFKA_TOPIC = "topic"                               // 消费者 topic 支持逗号分割
    val USER_NAME = "username"
    val PASSWORD = "password"


    //-------------------------------- hbase --------------------------------
    val HBASE_START = "hbase."

    //-------------------------------- mysql --------------------------------
    val DS_START = "datasource."
    val MYSQL_XML = "xml"



}
