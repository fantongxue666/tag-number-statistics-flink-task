package com.ftx.flink.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

public class ConnectionUtil {
    /**
     * 数据湖连接
     */
    public static Connection getIcebergConnection(String appPropertiesPath){
        Map<String, String> propFromFile = ConfigUtil.readConfig_properties(appPropertiesPath);
        String dlHost = propFromFile.get("datalake.host");
        String dlCatalog = propFromFile.get("datalake.catalog");
        String dlSchema = propFromFile.get("datalake.schema");
        String dlTable = propFromFile.get("datalake.table");
        String sDate = propFromFile.get("datalake.startDate");
        String eDate = propFromFile.get("datalake.endDate");
        if (dlHost.isEmpty()) {
            System.out.println("datalake.host can't be null");
            return null;
        }
        if (dlCatalog.isEmpty()) {
            System.out.println("datalake.catalog can't be null");
            return null;
        }
        if (dlSchema.isEmpty()) {
            System.out.println("datalake.schema can't be null");
            return null;
        }
        if (dlTable.isEmpty()) {
            System.out.println("datalake.table can't be null");
            return null;
        }
        if (sDate.isEmpty()) {
            System.out.println("startDate can't be null");
            return null;
        }
        if (eDate.isEmpty()) {
            System.out.println("endDate can't be null");
            return null;
        }

        // properties
        String url = "jdbc:trino://" + dlHost + "/" + dlCatalog + "/" + dlSchema;
        Properties propJDBC = new Properties();
        propJDBC.setProperty("user", "admin");
        Connection connection = null;
        try {
            connection = DriverManager.getConnection(url, propJDBC);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return connection;
    }
}
