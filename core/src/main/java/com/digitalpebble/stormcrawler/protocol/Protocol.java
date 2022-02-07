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
package com.digitalpebble.stormcrawler.protocol;

import com.digitalpebble.stormcrawler.Metadata;
import crawlercommons.robots.BaseRobotRules;
import org.apache.storm.Config;
import org.jetbrains.annotations.NotNull;

public interface Protocol {

    /** Configures the protocol with the given config */
    void configure(@NotNull Config conf);

    /**
     * Fetches the content and additional metadata
     *
     * <p>IMPORTANT: the metadata returned within the response should only be new <i>additional</i>,
     * no need to return the metadata passed in.
     *
     * @param url the location of the content
     * @param metadata extra information
     * @return the content and optional metadata fetched via this protocol
     */
    @NotNull
    ProtocolResponse getProtocolOutput(@NotNull String url, @NotNull Metadata metadata)
            throws Exception;

    @NotNull
    BaseRobotRules getRobotRules(@NotNull String url);

    void cleanup();
}
