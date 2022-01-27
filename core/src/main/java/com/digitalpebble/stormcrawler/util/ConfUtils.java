/**
 * Licensed to DigitalPebble Ltd under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership.
 * DigitalPebble licenses this file to You under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.digitalpebble.stormcrawler.util;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.storm.Config;
import org.jetbrains.annotations.NotNull;
import org.yaml.snakeyaml.Yaml;

public class ConfUtils {

    private ConfUtils() {}

    public static int getInt(Map<String, Object> conf, String key, int defaultValue) {
        Object ret = conf.get(key);
        if (ret == null) {
            ret = defaultValue;
        }
        return ((Number) ret).intValue();
    }

    public static long getLong(Map<String, Object> conf, String key, long defaultValue) {
        Object ret = conf.get(key);
        if (ret == null) {
            ret = defaultValue;
        }
        return ((Number) ret).longValue();
    }

    public static float getFloat(Map<String, Object> conf, String key, float defaultValue) {
        Object ret = conf.get(key);
        if (ret == null) {
            ret = defaultValue;
        }
        return ((Number) ret).floatValue();
    }

    public static boolean getBoolean(Map<String, Object> conf, String key, boolean defaultValue) {
        Object ret = conf.get(key);
        if (ret == null) {
            ret = defaultValue;
        }
        return (Boolean) ret;
    }

    public static String getString(Map<String, Object> conf, String key) {
        return (String) conf.get(key);
    }

    public static String getString(Map<String, Object> conf, String key, String defaultValue) {
        Object ret = conf.get(key);
        if (ret == null) {
            ret = defaultValue;
        }
        return (String) ret;
    }

    /**
     * Return one or more Strings regardless of whether they are represented as a single String or a
     * list in the config or an empty List if no value could be found for that key.
     */
    public static List<String> loadListFromConf(String paramKey, Map<String, Object> stormConf) {
        Object obj = stormConf.get(paramKey);
        List<String> list = new LinkedList<>();

        if (obj == null) return list;

        if (obj instanceof Collection) {
            list.addAll((Collection<String>) obj);
        } else { // single value?
            list.add(obj.toString());
        }
        return list;
    }

    /**
     * Loads the resource at {@code pathToResource} into the given {@code targetConfig}.
     *
     * @throws FileNotFoundException Thrown when {@code pathToResource} does not point to a file.
     */
    public static void loadConfInto(@NotNull String pathToResource, @NotNull Config targetConfig)
            throws FileNotFoundException {
        Yaml yaml = new Yaml();
        Map<String, Object> ret =
                yaml.load(
                        new InputStreamReader(
                                new FileInputStream(pathToResource), Charset.defaultCharset()));

        if (ret == null) {
            return;
        }

        // contains a single config element ?
        ret = extractConfigElement(ret);
        targetConfig.putAll(ret);
    }

    /** If the config consists of a single key 'config', its values are used instead */
    public static Map<String, Object> extractConfigElement(Map<String, Object> conf) {
        if (conf.size() == 1) {
            Object confNode = conf.get("config");
            if (confNode instanceof Map) {
                conf = (Map<String, Object>) confNode;
            }
        }
        return conf;
    }
}
