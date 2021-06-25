package org.holee.base.util

import okhttp3.Interceptor.Chain

import java.util.concurrent.TimeUnit
import okhttp3.{Interceptor, MediaType, OkHttpClient, Request, RequestBody, Response}

import scala.util.control.Breaks.{break, breakable}

/**
 * @author qimian.huang 黄奇冕
 * @create 2021-03-30 15:42
 * @Description 使用 okHttp
 */

object HttpUtil {
    private lazy val okHttpClient: OkHttpClient = new OkHttpClient.Builder()
      .connectTimeout(3L, TimeUnit.SECONDS)
      .readTimeout(5L, TimeUnit.SECONDS)
      .writeTimeout(5L, TimeUnit.SECONDS)
      .build()

    def getClient: OkHttpClient = {
        okHttpClient
    }

    // 同步请求
    def getResponse(request: Request): Response = {
        this.okHttpClient.newCall(request).execute()
    }
}

