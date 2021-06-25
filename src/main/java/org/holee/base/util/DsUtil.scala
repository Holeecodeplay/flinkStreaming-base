package org.holee.base.util

import java.util
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource

import java.util.Properties
import java.util.concurrent.ConcurrentHashMap
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.util.StringUtils
import org.apache.ibatis.builder.xml.XMLMapperBuilder
import org.apache.ibatis.mapping.Environment
import org.apache.ibatis.transaction.jdbc.JdbcTransactionFactory
import org.apache.ibatis.session.{Configuration, SqlSession, SqlSessionFactory, SqlSessionFactoryBuilder}
import org.holee.base.common.Consts

import scala.collection.JavaConversions.mapAsScalaMap

/**
 * @author qimian.huang 黄奇冕
 * @create 2021-02-18 16:09
 * @Description 数据源工具类，使用 hicariCP + mybatis，配置项相关参考官网 HikariConfig，支持多数据源
 */

object DsUtil {
    private val dsMap = new ConcurrentHashMap[String, util.HashMap[String,String]]           // <数据源代号，<配置项，value>>
    private val sessionMap = new ConcurrentHashMap[String, SqlSessionFactory]
    @volatile private var initTag = false
    private val lock = new Object

    def init(conf: ParameterTool): Unit = {
        if(!initTag){
            lock.synchronized({
                if(!initTag){
                    for((k,v) <- conf.toMap){
                        if (k.startsWith(Consts.DS_START) && !StringUtils.isNullOrWhitespaceOnly(v)) {
                            val dsName = k.substring(Consts.DS_START.length).split("\\.")(0)
                            val confNm = k.substring(Consts.DS_START.length + dsName.length + 1)
                            dsMap.putIfAbsent(dsName, new util.HashMap[String, String])
                            dsMap.get(dsName).put(confNm, v)
                        }
                    }
                    initTag = true
                }
            })
        }
    }

    // 使用完成 session 需要手动关闭，session 线程不安全
    def getSqlSesison(dsName: String): SqlSession = {
        initSession(dsName)
        sessionMap.get(dsName).openSession()
    }

    def closeSource(dsName: String): Unit = {
        if(null != sessionMap.get(dsName)) {
            this.synchronized  {
                if (null != sessionMap.get(dsName)) {
                    sessionMap.remove(dsName)
                }
            }
        }
    }

    // 锁类，不做变量锁
    def initSession(dsName: String): Unit = {
        if(null == sessionMap.get(dsName)) {
            this.synchronized {
                if (null == sessionMap.get(dsName)) {
                    val mysqlConfMap = dsMap.get(dsName)
                    val prop = new Properties()
                    val xmlPath = mysqlConfMap.get(Consts.MYSQL_XML)
                    mysqlConfMap.remove(Consts.MYSQL_XML)
                    for((k,v) <- mysqlConfMap){
                        prop.put(k, v)
                    }
                    val ds = new HikariDataSource(new HikariConfig(prop))
                    val environment = new Environment("Flink-DB", new JdbcTransactionFactory, ds)
                    val configuration = new Configuration(environment)

                    // 注册外部 xml
                    if(null != xmlPath){
                        val inputStream = StringUtil.getInputStream(xmlPath)
                        new XMLMapperBuilder(inputStream,configuration, xmlPath, configuration.getSqlFragments).parse()
                    }

                    val sqlSessionFactory = new SqlSessionFactoryBuilder().build(configuration)
                    sessionMap.putIfAbsent(dsName, sqlSessionFactory)
                }
                println(s"init datasource [$dsName] successfully")
            }
        }
    }

}
