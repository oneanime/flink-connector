package com.fs.utils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertiesUtil {
    private static PropertiesUtil propertiesUtil = null;
    private Properties properties = null;

    private PropertiesUtil(String filePath) {
        readPropertiesFile(filePath);
    }

    private void readPropertiesFile(String filePath) {
        properties = new Properties();
        InputStream in = null;
        try {
            in = this.getClass().getClassLoader().getResourceAsStream(filePath);
            properties.load(in);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                assert in != null;
                in.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static synchronized PropertiesUtil load(String filePath) {
        if (propertiesUtil == null) {
            propertiesUtil = new PropertiesUtil(filePath);
        }
        return propertiesUtil;
    }

    public Properties getProperties() {
        return propertiesUtil.properties;
    }

    public int getInt(String key,int defaultValue) {
        return Integer.parseInt(properties.getProperty(key, String.valueOf(defaultValue)));
    }

    public String getString(String key, String defaultValue) {
        return properties.getProperty(key, defaultValue);
    }

    public Boolean getBoolean(String key, Boolean defaultValue) {
        return Boolean.parseBoolean(properties.getProperty(key, String.valueOf(defaultValue)));
    }
}
