package org.holee.base.util

import java.io.InputStream
import java.security.MessageDigest
import org.apache.commons.codec.binary.Hex
import org.apache.flink.core.fs.Path
import org.apache.flink.core.fs.local.LocalFileSystem
import org.apache.flink.runtime.fs.hdfs.HadoopFileSystem
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem

import java.util.UUID

/**
 * @author qimian.huang 黄奇冕
 * @create 2021-02-23 21:18
 * @Description
 */

object StringUtil {
    /** Initial seed for 64-bit hashes. */
    val FNV1_64_INIT = 0xcbf29ce484222325L

    def encryptMD5(str: String): String = {
        val md = MessageDigest.getInstance("MD5")
        md.update(str.getBytes())
        new String(Hex.encodeHex(md.digest))
    }

    def encryptFNV1(str: String): Int = {
        val p = 16777619
        var hash = 2166136261L.toInt
        for (b <- str.toCharArray) {
            hash = (hash ^ b) * p
        }
        hash += hash << 13
        hash ^= hash >> 7
        hash += hash << 3
        hash ^= hash >> 17
        hash += hash << 5
        if (hash < 0) hash = Math.abs(hash)
        hash
    }

    def encryptFNV64a(str: String): Long = {
        val buf = str.getBytes()
        var seed = FNV1_64_INIT
        for (i <- buf.indices) {
            seed ^= buf(i)
            seed += (seed << 1) + (seed << 4) + (seed << 5) + (seed << 7) + (seed << 8) + (seed << 40)
        }
        seed
        //String.format("%16s", seed.toHexString).replace(' ', '0')
    }

    def getUUID: String = {
        UUID.randomUUID().toString.replace("-","")
    }



    // ------------------- File 操作相关 -------------------
    // 根据路径获取 inputstream
    def getInputStream(path: String): InputStream = {
        var inputStream: InputStream = null
        if(path.startsWith("hdfs")){
            inputStream = new HadoopFileSystem(FileSystem.get(new Configuration)).open(new Path(path))
        } else {
            //读取本地配置
            inputStream = new LocalFileSystem().open(new Path(path))
        }
        inputStream
    }
}

