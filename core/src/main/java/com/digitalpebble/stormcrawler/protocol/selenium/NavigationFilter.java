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
import org.apache.storm.thrift.annotation.Nullable;
import org.jetbrains.annotations.NotNull;
import org.openqa.selenium.remote.RemoteWebDriver;

/**
 * Implementations of NavigationFilter are responsible for extracting the page content and
 * additional metadata via the provided {@link RemoteWebDriver}. They are used exclusively by {@link
 * NavigationFilters}.
 *
 * @see NavigationFilters for more information.
 */
public interface NavigationFilter extends Configurable {

    /**
     * The end result comes from the first filter to return non-null.
     *
     * @return null if the filter does not fit
     */
    @Nullable
    ProtocolResponse filter(@NotNull RemoteWebDriver driver, @NotNull Metadata metadata);
}
