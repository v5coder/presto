/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.util;

import com.google.common.base.Joiner;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.reloading.FileChangedReloadingStrategy;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.google.common.collect.Maps.fromProperties;

public final class PropertiesUtil
{
    private static PropertiesConfiguration propConfig;
    private static final PropertiesUtil CONFIG = new PropertiesUtil();
    /**
     * 自动保存
     */
    private static boolean autoSave = true;

    private PropertiesUtil() {}

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

    public static Map<String, String> loadProperties(File file)
            throws IOException
    {
        Properties properties = new Properties();
        try (InputStream in = new FileInputStream(file)) {
            properties.load(in);
        }
        return fromProperties(properties);
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
