/**
 * FileName: MySQLJdbcConfig
 * Author: SiXiang
 * Date: 2019/6/4 23:55
 * Description:
 * History:
 * <author> <time> <version> <desc>
 * Sixiang 修改时间  版本号    描述
 */
package com.utils;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * MySQL jdbc Utils
 */
public class MySQLJdbcConfig {
    private static final Logger LOGGER = Logger.getLogger(MySQLJdbcConfig.class);

    private String table;

    private String url;

    private Properties connectionProperties;

    public void init(){
        Properties properties = new Properties();
        InputStream resourceAsStream = this.getClass().getClassLoader().getResourceAsStream("jdbc.properties");
        try {
            properties.load(resourceAsStream);
            setUrl(properties.getProperty("db.url"));
            //考虑多数据源的情况，另外创建properties传入
            Properties connectionProperties = new Properties();
            connectionProperties.setProperty("user",properties.getProperty("db.user"));
            connectionProperties.setProperty("password",properties.getProperty("db.password"));
            connectionProperties.setProperty("url",properties.getProperty("db.url"));
            setConnectionProperties(connectionProperties);
        } catch (IOException e) {
            LOGGER.info("读取配置文件失败");
        }

    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public Properties getConnectionProperties() {
        return connectionProperties;
    }

    public void setConnectionProperties(Properties connectionProperties) {

        this.connectionProperties = connectionProperties;
    }

}
