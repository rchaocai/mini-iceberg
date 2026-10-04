package com.iceberglearn;

/**
 * 简单的用户记录类
 * 用于演示 Parquet 文件的读写
 */
public record User(long id, String name, int age, String email) {
}