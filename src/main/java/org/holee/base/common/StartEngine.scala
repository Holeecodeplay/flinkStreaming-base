package org.holee.base.common

import com.esotericsoftware.kryo.serializers.DefaultSerializers.KryoSerializableSerializer
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.StringUtils
import org.holee.base.util.StringUtil

import scala.collection.JavaConversions.mapAsScalaMap

/**
 * @author qimian.huang 黄奇冕
 * @create 2021-03-08 15:09
 * @Description
 */

class StartEngine {
    def run(confPath: String): Unit = {
        try {
            if(confPath == null || confPath.isEmpty) {
                println("ERROR: config file path should not be null !!!")
                sys.exit(-1)
            }

            val env = StreamExecutionEnvironment.getExecutionEnvironment

            // 加载并初始化配置
            println(s"configPath : $confPath")
            println("-" * 30)
            val conf = ParameterTool.fromPropertiesFile(StringUtil.getInputStream(confPath))
            conf.toMap.foreach(println)
            println("-" * 30)
            env.getConfig.setGlobalJobParameters(conf)

            // 本地调试默认开启 8081 ui 端口
            if(conf.getBoolean(Consts.LOCAL_DEBUG, false)){
                val field = classOf[org.apache.flink.streaming.api.environment.StreamExecutionEnvironment].getDeclaredField("configuration")
                field.setAccessible(true)
                import org.apache.flink.configuration.Configuration
                val configuration: Configuration = field.get(env.getJavaEnv).asInstanceOf[Configuration]
                configuration.setString("rest.bind-port", "8081")
            }
            if (conf.has(Consts.STATE_PATH) && !StringUtils.isNullOrWhitespaceOnly(conf.get(Consts.STATE_PATH))) {
                env.setStateBackend(new FsStateBackend(conf.get(Consts.STATE_PATH), false))
            }

            env.setParallelism(conf.getInt(Consts.TASK_PARALLELISM,1))
            env.enableCheckpointing(conf.getLong(Consts.CHECKPOINT_SECOND, 5) * 1000, CheckpointingMode.EXACTLY_ONCE)
            env.getCheckpointConfig.setMaxConcurrentCheckpoints(conf.getInt(Consts.CHECKPOINT_MAX_NUM, 1)) // 同一时间只允许进行一个检查点
            env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)    // 每次 checkpoint 结果都将会保留不会物理清除


            if(null != conf.get(Consts.KRYO_CLASS) && conf.get(Consts.KRYO_CLASS).nonEmpty){
                val needKryoClassList = conf.get(Consts.KRYO_CLASS).split(",")
                for( c <- needKryoClassList){
                    env.getConfig.registerTypeWithKryoSerializer(Class.forName(c),classOf[KryoSerializableSerializer])
                }
            }

            if(conf.has(Consts.JOB_SERVICE) && !StringUtils.isNullOrWhitespaceOnly(conf.get(Consts.JOB_SERVICE))){
                Class.forName(conf.get(Consts.JOB_SERVICE)).newInstance().asInstanceOf[BaseService].doService(env, conf)
            } else {
                println("ERROR: business service should not be null !!!")
                sys.exit(-1)
            }

            env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                conf.getInt(Consts.JOB_ATTEMPTS,2),
                conf.getInt(Consts.JOB_DELAY,10) * 1000))

            env.execute(conf.get(Consts.JOB_NANE, Consts.APP_NAME))
        } catch {
            case e: Any =>
                println("Flink Start Error !!")
                println(e.printStackTrace())
        }
    }
}
