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

public class ParseData {
    private byte[] content;
    private @Nullable String text;
    private @NotNull Metadata metadata;

    public ParseData() {
        this.metadata = new Metadata();
    }

    public ParseData(@NotNull Metadata metadata) {
        this.metadata = metadata;
    }

    public ParseData(@Nullable String text, @NotNull Metadata metadata) {
        this.text = text;
        this.metadata = metadata;
        this.content = new byte[0];
    }

    public @Nullable String getText() {
        return text;
    }

    public @NotNull Metadata getMetadata() {
        return metadata;
    }

    public void setText(@Nullable String text) {
        this.text = text;
    }

    public void setMetadata(@NotNull Metadata metadata) {
        this.metadata = metadata;
    }

    public byte[] getContent() {
        return content;
    }

    public void setContent(byte[] content) {
        this.content = content;
    }

    public void put(String key, String value) {
        metadata.addValue(key, value);
    }

    public String get(String key) {
        return metadata.getFirstValue(key);
    }

    public String[] getValues(String key) {
        return metadata.getValues(key);
    }
}
