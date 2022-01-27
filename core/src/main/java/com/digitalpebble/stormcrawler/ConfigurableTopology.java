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
package com.digitalpebble.stormcrawler;

import com.digitalpebble.stormcrawler.persistence.Status;
import com.digitalpebble.stormcrawler.util.ConfUtils;
import java.io.FileNotFoundException;
import java.util.*;
import org.apache.commons.lang.StringUtils;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.streams.Pair;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ConfigurableTopology {

    private static final Logger LOG = LoggerFactory.getLogger(ConfigurableTopology.class);

    private static final String CONFIG_ARG_PARAM_NAME = "-conf";

    /** Instance of the config. */
    protected final Config conf = new Config();

    public static void start(ConfigurableTopology topology, String[] args) {
        // loads the default configuration file
        Map<String, Object> defaultSCConfig =
                Utils.findAndReadConfigFile("crawler-default.yaml", false);
        topology.conf.putAll(ConfUtils.extractConfigElement(defaultSCConfig));
        String[] remainingArgs = topology.extractAndSetConfFromArgs(args);
        topology.run(remainingArgs);
    }

    /** @deprecated use direct field accessor. */
    @Deprecated
    protected Config getConf() {
        return conf;
    }

    protected abstract int run(@NotNull String[] args);

    /** Submits the topology with the name taken from the configuration * */
    protected int submit(Config conf, TopologyBuilder builder) {
        String name = ConfUtils.getString(conf, Config.TOPOLOGY_NAME);
        if (StringUtils.isBlank(name))
            throw new RuntimeException("No value found for " + Config.TOPOLOGY_NAME);
        return submit(name, conf, builder);
    }

    /** Submits the topology under a specific name * */
    protected int submit(String name, Config conf, TopologyBuilder builder) {

        // register for serialization with Kryo
        Config.registerSerialization(conf, Metadata.class);
        Config.registerSerialization(conf, Status.class);

        try {
            StormSubmitter.submitTopology(name, conf, builder.createTopology());
        } catch (Exception e) {
            e.printStackTrace();
            return -1;
        }
        return 0;
    }

    /**
     * Search for the {@value CONFIG_ARG_PARAM_NAME} in the given {@code args} and return a pair of
     * values, containing the found path and the cleaned args. </br> The values of the pair are
     * either (null, args) or (List(paths to Conf-files), cleanedArgs).
     *
     * @return a pair containing the path to the resource and the filtered args.
     */
    @Contract(pure = true)
    static @NotNull Pair<List<String>, String[]> extractConfFromArgs(@NotNull String[] args) {
        final List<String> newArgs = new ArrayList<>(Arrays.asList(args));
        final List<String> paths = new ArrayList<>();

        final ListIterator<String> iter = newArgs.listIterator();
        while (iter.hasNext()) {
            String param = iter.next();
            if (param.equals(CONFIG_ARG_PARAM_NAME)) {
                if (!iter.hasNext()) {
                    throw new RequiredArgumentValueMissingException(args, param);
                }
                iter.remove();
                String resource = iter.next();
                paths.add(resource);
                iter.remove();
            }
        }

        if (paths.isEmpty()) {
            return Pair.of(null, args);
        }

        return Pair.of(paths, newArgs.toArray(new String[0]));
    }

    /* Processes the given args and return them without -conf option. */
    private @NotNull String[] extractAndSetConfFromArgs(@NotNull String[] args) {
        final Pair<List<String>, String[]> result = extractConfFromArgs(args);
        final List<String> paths = result.value1;
        if (paths != null) {
            HashSet<String> distinctPaths = new HashSet<>(paths);
            if (distinctPaths.size() != paths.size()) {
                for (String entry : distinctPaths) {
                    int freq = Collections.frequency(paths, entry);
                    if (freq > 1) {
                        LOG.warn("The path \"{}\" is contained {}-times in the args.", entry, freq);
                    }
                }
            }
            for (String path : paths) {
                try {
                    ConfUtils.loadConfInto(path, conf);
                } catch (FileNotFoundException e) {
                    throw new RuntimeException("File not found : " + path, e);
                }
            }
        }
        return result.value2;
    }

    public static class RequiredArgumentValueMissingException extends RuntimeException {
        RequiredArgumentValueMissingException(String[] args, String argValue) {
            super(
                    String.format(
                            "The argument %s is missing the required subsequent argument value in the args %s.",
                            argValue, Arrays.toString(args)));
        }
    }
}
