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
package com.digitalpebble.stormcrawler.persistence;

import com.digitalpebble.stormcrawler.Constants;
import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.util.ConfUtils;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Schedules a nextFetchDate based on the configuration */
public class DefaultScheduler extends Scheduler {

    /**
     * Key used to pass a custom delay via metadata. Used by the sitemaps to stagger the scheduling
     * of URLs.
     */
    public static final String DELAY_METADATA = "scheduler.delay.mins";

    // fetch intervals in minutes
    private int defaultfetchInterval;
    private int fetchErrorFetchInterval;
    private int errorFetchInterval;

    private CustomInterval[] customIntervals;

    /*
     * (non-Javadoc)
     *
     * @see
     * com.digitalpebble.stormcrawler.persistence.Scheduler#init(java.util.Map)
     */
    @Override
    public void init(Map<String, Object> stormConf) {
        defaultfetchInterval =
                ConfUtils.getInt(stormConf, Constants.defaultFetchIntervalParamName, 1440);
        fetchErrorFetchInterval =
                ConfUtils.getInt(stormConf, Constants.fetchErrorFetchIntervalParamName, 120);
        errorFetchInterval =
                ConfUtils.getInt(stormConf, Constants.errorFetchIntervalParamName, 44640);

        // loads any custom key values
        // must be of form fetchInterval(.STATUS)?.keyname=value
        // e.g. fetchInterval.isFeed=true
        // e.g. fetchInterval.FETCH_ERROR.isFeed=true
        Map<String, CustomInterval> intervals = new HashMap<>();
        Pattern pattern = Pattern.compile("^fetchInterval(\\..+?)?\\.(.+)=(.+)");
        for (String key : stormConf.keySet()) {
            Matcher m = pattern.matcher(key);
            if (!m.matches()) {
                continue;
            }
            Status status = null;
            // was a status specified?
            if (m.group(1) != null) {
                status = Status.valueOf(m.group(1).substring(1));
            }
            String mdname = m.group(2);
            String mdvalue = m.group(3);
            int customInterval = ConfUtils.getInt(stormConf, key, Integer.MIN_VALUE);
            if (customInterval != Integer.MIN_VALUE) {
                CustomInterval interval = intervals.get(mdname + mdvalue);
                if (interval == null) {
                    interval = new CustomInterval(mdname, mdvalue, status, customInterval);
                } else {
                    interval.setDurationForStatus(status, customInterval);
                }
                // specify particular interval for this status
                intervals.put(mdname + mdvalue, interval);
            }
        }
        customIntervals = intervals.values().toArray(new CustomInterval[0]);
    }

    /*
     * (non-Javadoc)
     *
     * @see com.digitalpebble.stormcrawler.persistence.Scheduler#schedule(com.
     * digitalpebble. stormcrawler.persistence .Status,
     * com.digitalpebble.stormcrawler.Metadata)
     */
    @Override
    public Optional<Date> schedule(Status status, Metadata metadata) {

        int minutesIncrement = 0;

        Optional<Integer> customInterval = Optional.empty();

        // try with a value set in the metadata
        String customInMetadata = metadata.getFirstValue(DELAY_METADATA);
        if (customInMetadata != null) {
            customInterval = Optional.of(Integer.parseInt(customInMetadata));
        }
        // try with the rules from the configuration
        if (!customInterval.isPresent()) {
            customInterval = checkCustomInterval(metadata, status);
        }

        if (customInterval.isPresent()) {
            minutesIncrement = customInterval.get();
        } else {
            switch (status) {
                case FETCHED:
                    minutesIncrement = defaultfetchInterval;
                    break;
                case FETCH_ERROR:
                    minutesIncrement = fetchErrorFetchInterval;
                    break;
                case ERROR:
                    minutesIncrement = errorFetchInterval;
                    break;
                case REDIRECTION:
                    minutesIncrement = defaultfetchInterval;
                    break;
                default:
                    // leave it to now e.g. DISCOVERED
            }
        }

        // a value of -1 means never fetch
        // we return null
        if (minutesIncrement == -1) {
            return Optional.empty();
        }

        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.MINUTE, minutesIncrement);

        return Optional.of(cal.getTime());
    }

    /** Returns the first matching custom interval */
    protected final Optional<Integer> checkCustomInterval(Metadata metadata, Status s) {
        if (customIntervals == null) return Optional.empty();

        for (CustomInterval customInterval : customIntervals) {
            String[] values = metadata.getValues(customInterval.key);
            if (values == null) {
                continue;
            }
            for (String v : values) {
                if (v.equals(customInterval.value)) {
                    return customInterval.getDurationForStatus(s);
                }
            }
        }

        return Optional.empty();
    }

    private static class CustomInterval {
        private final String key;
        private final String value;
        private final Map<Status, Integer> durationPerStatus;
        private Integer defaultDuration = null;

        private CustomInterval(String key, String value, Status status, int minutes) {
            this.key = key;
            this.value = value;
            this.durationPerStatus = new HashMap<>();
            setDurationForStatus(status, minutes);
        }

        private void setDurationForStatus(Status s, int minutes) {
            if (s == null) {
                defaultDuration = minutes;
            } else {
                this.durationPerStatus.put(s, minutes);
            }
        }

        private Optional<Integer> getDurationForStatus(Status s) {
            // do we have a specific value for this status?
            Integer customD = durationPerStatus.get(s);
            if (customD != null) {
                return Optional.of(customD);
            }
            // is there a default one set?
            if (defaultDuration != null) {
                return Optional.of(defaultDuration);
            }
            // no default value or custom one for that status
            return Optional.empty();
        }
    }
}
