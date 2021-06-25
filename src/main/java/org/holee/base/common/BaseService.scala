package org.holee.base.common

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * @author qimian.huang 黄奇冕
 * @create 2021-03-08 11:36
 * @Description
 */

trait BaseService extends Serializable{
    def doService(env: StreamExecutionEnvironment, conf: ParameterTool)
}
