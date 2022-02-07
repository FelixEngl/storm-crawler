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
package com.digitalpebble.stormcrawler.parse;

import com.digitalpebble.stormcrawler.Metadata;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class Outlink {

    private @NotNull String targetURL;
    private @Nullable String anchor;
    private @Nullable Metadata metadata;

    public Outlink(@NotNull String url) {
        targetURL = url;
        anchor = null;
    }

    public Outlink(@NotNull String url, @NotNull String a) {
        targetURL = url;
        anchor = a;
    }

    public @NotNull String getTargetURL() {
        return targetURL;
    }

    public void setTargetURL(@NotNull String targetURL) {
        this.targetURL = targetURL;
    }

    public @Nullable String getAnchor() {
        return anchor;
    }

    public void setAnchor(@NotNull String anchor) {
        this.anchor = anchor;
    }

    public @Nullable Metadata getMetadata() {
        return metadata;
    }

    public void setMetadata(@Nullable Metadata metadata) {
        this.metadata = metadata;
    }

    public String toString() {
        return targetURL;
    }
}
