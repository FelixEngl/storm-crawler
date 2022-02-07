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

import java.io.IOException;
import java.io.InputStream;
import org.jetbrains.annotations.NotNull;

/**
 * Defines a generic behaviour for ParseFilters or URLFilters to load resources from a JSON file.
 */
public interface JSONResource {

    /** @return filename of the JSON resource */
    @NotNull
    String getResourceFile();

    /** Load the resources from an input stream */
    void loadJSONResources(@NotNull InputStream inputStream) throws IOException;

    /** Load the resources from the JSON file in the uber jar */
    default void loadJSONResources() throws Exception {
        try (InputStream inputStream =
                getClass().getClassLoader().getResourceAsStream(getResourceFile())) {
            if (inputStream == null) {
                throw new RuntimeException(
                        String.format("Ressource at %s was not found.", getResourceFile()));
            }
            loadJSONResources(inputStream);
        }
    }
}
