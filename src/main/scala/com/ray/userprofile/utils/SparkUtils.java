package com.ray.userprofile.utils;

import org.apache.spark.sql.SparkSession;

public class SparkUtils {
    // 定义会话池
    private static ThreadLocal<SparkSession> sessionPool = new ThreadLocal<>();

    public static SparkSession initSession() {
        if (sessionPool.get() != null) {
            return sessionPool.get();
        }
        SparkSession session = SparkSession.builder().appName("etl")
                .master("local[*]")
                .config("es.nodes", "localhost")
                .config("es.port", "9200")
                .config("es.index.auto.create", "false") 
                .enableHiveSupport()
                .getOrCreate();
        sessionPool.set(session);
        return session;
    }
}