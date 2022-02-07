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
package com.digitalpebble.stormcrawler.filtering.basic;

import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.filtering.URLFilter;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import java.net.IDN;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang.StringUtils;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BasicURLNormalizer implements URLFilter {

    private static final Logger LOG = LoggerFactory.getLogger(BasicURLNormalizer.class);
    /** Nutch 1098 - finds URL encoded parts of the URL */
    private static final Pattern unescapeRulePattern = Pattern.compile("%([0-9A-Fa-f]{2})");

    /** https://github.com/DigitalPebble/storm-crawler/issues/401 * */
    private static final Pattern illegalEscapePattern = Pattern.compile("%u([0-9A-Fa-f]{4})");

    // charset used for encoding URLs before escaping
    private static final Charset utf8 = StandardCharsets.UTF_8;

    /** look-up table for characters which should not be escaped in URL paths */
    private static final boolean[] unescapedCharacters = new boolean[128];

    private static final Pattern thirtytwobithash = Pattern.compile("[a-fA-F\\d]{32}");

    static {
        for (int c = 0; c < 128; c++) {
            /*
             * https://tools.ietf.org/html/rfc3986#section-2.2 For consistency,
             * percent-encoded octets in the ranges of ALPHA (%41-%5A and
             * %61-%7A), DIGIT (%30-%39), hyphen (%2D), period (%2E), underscore
             * (%5F), or tilde (%7E) should not be created by URI producers and,
             * when found in a URI, should be decoded to their corresponding
             * unreserved characters by URI normalizers.
             */
            unescapedCharacters[c] =
                    (0x41 <= c && c <= 0x5A)
                            || (0x61 <= c && c <= 0x7A)
                            || (0x30 <= c && c <= 0x39)
                            || c == 0x2D
                            || c == 0x2E
                            || c == 0x5F
                            || c == 0x7E;
        }
    }

    boolean removeAnchorPart = true;
    boolean unmangleQueryString = true;
    boolean checkValidURI = true;
    boolean removeHashes = false;
    private boolean hostIDNtoASCII = false;
    final Set<String> queryElementsToRemove = new TreeSet<>();

    @Override
    public @Nullable String filter(
            @Nullable URL sourceUrl,
            @Nullable Metadata sourceMetadata,
            @NotNull String urlToFilter) {

        urlToFilter = urlToFilter.trim();

        final String originalURL = urlToFilter;

        if (removeAnchorPart) {
            try {
                URL theURL = new URL(urlToFilter);
                String anchor = theURL.getRef();
                if (anchor != null) urlToFilter = urlToFilter.replace("#" + anchor, "");
            } catch (MalformedURLException e) {
                return null;
            }
        }

        if (unmangleQueryString) {
            urlToFilter = unmangleQueryString(urlToFilter);
        }

        if (!queryElementsToRemove.isEmpty() || removeHashes) {
            urlToFilter = processQueryElements(urlToFilter);
        }

        if (urlToFilter == null) return null;

        try {
            URL theURL = new URL(urlToFilter);
            String file = theURL.getFile();
            String protocol = theURL.getProtocol();
            String host = theURL.getHost();
            boolean hasChanged = !urlToFilter.startsWith(protocol); // lowercased protocol

            if (host != null) {
                String newHost = host.toLowerCase(Locale.ROOT);
                if (hostIDNtoASCII && !isAscii(newHost)) {
                    try {
                        newHost = IDN.toASCII(newHost);
                    } catch (IllegalArgumentException ex) {
                        // eg. if the input string contains non-convertible
                        // Unicode codepoints
                        LOG.error("Failed to convert IDN host {} in {}", newHost, urlToFilter);
                        return null;
                    }
                }
                if (!host.equals(newHost)) {
                    host = newHost;
                    hasChanged = true;
                }
            }

            int port = theURL.getPort();
            // properly encode characters in path/file using percent-encoding
            String file2 = unescapePath(file);
            file2 = escapePath(file2);
            if (!file.equals(file2)) {
                hasChanged = true;
            }
            if (hasChanged) {
                urlToFilter = new URL(protocol, host, port, file2).toString();
            }
        } catch (MalformedURLException e) {
            return null;
        }

        if (checkValidURI) {
            try {
                URI uri = URI.create(urlToFilter);
                urlToFilter = uri.normalize().toString();
            } catch (java.lang.IllegalArgumentException e) {
                LOG.info("Invalid URI {} from {} ", urlToFilter, originalURL);
                return null;
            }
        }

        return urlToFilter;
    }

    @Override
    public void configure(@NotNull Map<String, Object> stormConf, @NotNull JsonNode paramNode) {
        JsonNode node = paramNode.get("removeAnchorPart");
        if (node != null) {
            removeAnchorPart = node.booleanValue();
        }

        node = paramNode.get("unmangleQueryString");
        if (node != null) {
            unmangleQueryString = node.booleanValue();
        }

        node = paramNode.get("queryElementsToRemove");
        if (node != null) {
            if (!node.isArray()) {
                LOG.warn("Failed to configure queryElementsToRemove.  Not an array: {}", node);
            } else {
                ArrayNode array = (ArrayNode) node;
                for (JsonNode element : array) {
                    queryElementsToRemove.add(element.asText());
                }
            }
        }

        node = paramNode.get("checkValidURI");
        if (node != null) {
            checkValidURI = node.booleanValue();
        }

        node = paramNode.get("removeHashes");
        if (node != null) {
            removeHashes = node.booleanValue();
        }

        node = paramNode.get("hostIDNtoASCII");
        if (node != null) {
            hostIDNtoASCII = node.booleanValue();
        }
    }

    /**
     * Basic filter to remove query parameters from urls so parameters that don't change the content
     * of the page can be removed. An example would be a google analytics query parameter like
     * "utm_campaign" which might have several different values for a url that points to the same
     * content. This is also called when removing attributes where the value is a hash.
     */
    @Nullable
    private String processQueryElements(String urlToFilter) {
        try {
            // Handle illegal characters by making a url first
            // this will clean illegal characters like |
            URL url = new URL(urlToFilter);

            String query = url.getQuery();
            String path = url.getPath();

            // check if the last element of the path contains parameters
            // if so convert them to query elements
            if (path.contains(";")) {
                String[] pathElements = path.split("/");
                String last = pathElements[pathElements.length - 1];
                // replace last value by part without params
                int semicolon = last.indexOf(";");
                if (semicolon != -1) {
                    pathElements[pathElements.length - 1] = last.substring(0, semicolon);
                    String params = last.substring(semicolon + 1).replaceAll(";", "&");
                    if (query == null) {
                        query = params;
                    } else {
                        query += "&" + params;
                    }
                    // rebuild the path
                    StringBuilder newPath = new StringBuilder();
                    for (String p : pathElements) {
                        if (StringUtils.isNotBlank(p)) {
                            newPath.append("/").append(p);
                        }
                    }
                    path = newPath.toString();
                }
            }

            if (StringUtils.isEmpty(query)) {
                return urlToFilter;
            }

            List<NameValuePair> pairs = URLEncodedUtils.parse(query, StandardCharsets.UTF_8);
            Iterator<NameValuePair> pairsIterator = pairs.iterator();
            while (pairsIterator.hasNext()) {
                NameValuePair param = pairsIterator.next();
                if (queryElementsToRemove.contains(param.getName())) {
                    pairsIterator.remove();
                } else if (removeHashes && param.getValue() != null) {
                    Matcher m = thirtytwobithash.matcher(param.getValue());
                    if (m.matches()) {
                        pairsIterator.remove();
                    }
                }
            }

            StringBuilder newFile = new StringBuilder();
            if (StringUtils.isNotBlank(path)) {
                newFile.append(path);
            }
            if (!pairs.isEmpty()) {
                pairs.sort(comp);
                String newQueryString = URLEncodedUtils.format(pairs, StandardCharsets.UTF_8);
                newFile.append('?').append(newQueryString);
            }
            if (url.getRef() != null) {
                newFile.append('#').append(url.getRef());
            }

            return new URL(url.getProtocol(), url.getHost(), url.getPort(), newFile.toString())
                    .toString();
        } catch (MalformedURLException e) {
            LOG.warn("Invalid urlToFilter {}. {}", urlToFilter, e);
            return null;
        }
    }

    Comparator<NameValuePair> comp = Comparator.comparing(NameValuePair::getName);

    /**
     * A common error to find is a query string that starts with an & instead of a ? This will fix
     * that error. So http://foo.com&a=b will be changed to http://foo.com?a=b.
     *
     * @param urlToFilter the url to filter
     * @return corrected url
     */
    private String unmangleQueryString(String urlToFilter) {
        int firstAmp = urlToFilter.indexOf('&');
        if (firstAmp > 0) {
            int firstQuestionMark = urlToFilter.indexOf('?');
            if (firstQuestionMark == -1) {
                return urlToFilter.replaceFirst("&", "?");
            }
        }
        return urlToFilter;
    }

    /**
     * Remove % encoding from path segment in URL for characters which should be unescaped according
     * to <a href="https://tools.ietf.org/html/rfc3986#section-2.2">RFC3986</a> as well as
     * non-standard implementations of percent encoding, see <https://en.
     * wikipedia.org/wiki/Percent-encoding#Non-standard_implementations>.
     */
    private String unescapePath(String path) {
        Matcher matcher = illegalEscapePattern.matcher(path);

        StringBuilder sb = null;
        int end = 0;

        while (matcher.find()) {
            if (sb == null) {
                sb = new StringBuilder();
            }
            // Append everything up to this group
            sb.append(path, end, matcher.start());
            String group = matcher.group(1);
            int letter = Integer.valueOf(group, 16);
            sb.append((char) letter);
            end = matcher.end();
        }

        // we got a replacement
        if (sb != null) {
            // append whatever is left
            sb.append(path.substring(end));
            path = sb.toString();
            end = 0;
        }

        matcher = unescapeRulePattern.matcher(path);

        if (!matcher.find()) {
            return path;
        }

        sb = new StringBuilder();

        // Traverse over all encoded groups
        do {
            // Append everything up to this group
            sb.append(path, end, matcher.start());

            // Get the integer representation of this hexadecimal encoded
            // character
            int letter = Integer.valueOf(matcher.group(1), 16);
            if (letter < 128 && unescapedCharacters[letter]) {
                // character should be unescaped in URLs
                sb.append((char) letter);
            } else {
                // Append the whole sequence as uppercase
                sb.append(matcher.group().toUpperCase(Locale.ROOT));
            }

            end = matcher.end();
        } while (matcher.find());

        // Append the rest if there's anything left
        sb.append(path.substring(end));

        return sb.toString();
    }

    /**
     * Convert path segment of URL from Unicode to UTF-8 and escape all characters which should be
     * escaped according to <a href="https://tools.ietf.org/html/rfc3986#section-2.2">RFC3986</a>..
     */
    private String escapePath(String path) {
        StringBuilder sb = new StringBuilder(path.length());

        // Traverse over all bytes in this URL
        for (byte b : path.getBytes(utf8)) {
            // Is this a control character?
            if (b < 33 || b == 91 || b == 92 || b == 93 || b == 124) {
                // Start escape sequence
                sb.append('%');

                // Get this byte's hexadecimal representation
                String hex = Integer.toHexString(b & 0xFF).toUpperCase(Locale.ROOT);

                // Do we need to prepend a zero?
                if (hex.length() % 2 != 0) {
                    sb.append('0');
                    sb.append(hex);
                } else {
                    // No, append this hexadecimal representation
                    sb.append(hex);
                }
            } else {
                // No, just append this character as-is
                sb.append((char) b);
            }
        }

        return sb.toString();
    }

    private boolean isAscii(String str) {
        char[] chars = str.toCharArray();
        for (char c : chars) {
            if (c > 127) {
                return false;
            }
        }
        return true;
    }
}
