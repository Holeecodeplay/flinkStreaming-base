package org.holee.base.util

import java.util
import java.util.concurrent.ConcurrentHashMap
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.util.StringUtils
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}
import org.holee.base.common.Consts
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions.mapAsScalaMap

/**
 * @author qimian.huang 黄奇冕
 * @create 2021-02-24 21:03
 * @Description
 */

object HbaseUtil {
    private val hbaseMap = new ConcurrentHashMap[String, util.HashMap[String,String]]        // <hbase代号，<配置项，value>>
    private val connectionMap = new java.util.concurrent.ConcurrentHashMap[String, Connection]
    @volatile private var initTag = false
    private val lock = new Object

    def init(conf: ParameterTool): Unit = {
        if (!initTag) {
            lock.synchronized({
                if(!initTag){
                    for((k,v) <- conf.toMap){
                        if (k.startsWith(Consts.HBASE_START) && !StringUtils.isNullOrWhitespaceOnly(v)) {
                            val hbaseNm = k.substring(Consts.HBASE_START.length).split("\\.")(0)
                            val confNm = k.substring(Consts.HBASE_START.length + hbaseNm.length + 1)
                            hbaseMap.putIfAbsent(hbaseNm, new util.HashMap[String, String])
                            hbaseMap.get(hbaseNm).put(confNm, v)
                        }
                    }
                    initTag = true
                }
            })
        }
    }

    def getConnect(hbaseNm: String): Connection = {
        initConnect(hbaseNm)
        connectionMap.get(hbaseNm)
    }

    def initConnect(hbaseNm: String): Unit = {
        if(null == connectionMap.get(hbaseNm)) {
            this.synchronized {
                if (null == connectionMap.get(hbaseNm)) {
                    try {
                        val configuration = HBaseConfiguration.create
                        for((k,v) <- hbaseMap.get(hbaseNm)){
                            configuration.set(k, v)
                        }
                        val connect = ConnectionFactory.createConnection(configuration)
                        connectionMap.putIfAbsent(hbaseNm, connect)
                        println(s"init hbase connection [$hbaseNm] successfully")
                    } catch {
                        case e: Exception => println(s"Error init hbase connection [$hbaseNm] !!!", e.getMessage)
                    }
                }
            }
        }
    }
}
