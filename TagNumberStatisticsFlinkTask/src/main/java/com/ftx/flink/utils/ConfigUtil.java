package com.ftx.flink.utils;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * 读取properties配置文件的工具类
 */
public class ConfigUtil {
    public static Map<String,String> readConfig_properties(String absolutePath){
        Properties properties = new Properties();
        try {
            properties.load(new FileInputStream(new File(absolutePath)));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new HashMap<String, String>((Map) properties);
    }
}
