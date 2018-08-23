package com.facebook.presto.metadata;

import com.google.common.base.Joiner;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.reloading.FileChangedReloadingStrategy;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class PropertiesUtil {
    private static PropertiesConfiguration propConfig;
    private static final PropertiesUtil CONFIG = new PropertiesUtil();
    /**
     * 自动保存
     */
    private static boolean autoSave = true;

    private PropertiesUtil() {
    }

    public static PropertiesUtil getInstance(File propertiesFile) {
        //执行初始化
        init(propertiesFile);
        return CONFIG;
    }

    /**
     * 初始化
     *
     * @param propertiesFile
     * @see
     */
    private static void init(File propertiesFile) {
        try {
            propConfig = new PropertiesConfiguration(propertiesFile);
            //自动重新加载
            propConfig.setReloadingStrategy(new FileChangedReloadingStrategy());
            //自动保存
            propConfig.setAutoSave(autoSave);
        } catch (ConfigurationException e) {
            e.printStackTrace();
        }
    }

    /**
     * 根据Key获得对应的value
     *
     * @param key
     * @return
     * @see
     */
    public String getValue(String key) {
        Object obj = propConfig.getProperty(key);
        if (obj instanceof ArrayList) {
            List<String> objs = (List<String>) obj;
            return Joiner.on(',').join(objs);
        }
        return (String) obj;
    }

    /**
     * 设置属性
     *
     * @param key
     * @param value
     * @see
     */
    public void setProperty(String key, String value) {
        propConfig.setProperty(key, value);
    }
}