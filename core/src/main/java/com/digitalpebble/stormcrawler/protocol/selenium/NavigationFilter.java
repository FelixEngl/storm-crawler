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
package com.digitalpebble.stormcrawler.protocol.selenium;

import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.protocol.ProtocolResponse;
import com.digitalpebble.stormcrawler.util.Configurable;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.Map;
import org.apache.storm.thrift.annotation.Nullable;
import org.jetbrains.annotations.NotNull;
import org.openqa.selenium.remote.RemoteWebDriver;

public abstract class NavigationFilter implements Configurable {
    /**
     * Called when this filter is being initialised
     *
     * @param stormConf The Storm configuration used for the parsing bolt
     * @param filterParams the filter specific configuration. Never null
     */
    public void configure(@NotNull Map<String, Object> stormConf, @NotNull JsonNode filterParams) {}

    /**
     * The end result comes from the first filter to return non-null.
     *
     * @return null if the filter does not fit
     */
    public abstract @Nullable ProtocolResponse filter(
            @NotNull RemoteWebDriver driver, @NotNull Metadata metadata);
}
