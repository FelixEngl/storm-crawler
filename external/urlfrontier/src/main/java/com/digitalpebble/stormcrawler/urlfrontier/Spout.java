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
package com.digitalpebble.stormcrawler.urlfrontier;

import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.persistence.AbstractQueryingSpout;
import com.digitalpebble.stormcrawler.util.ConfUtils;
import crawlercommons.urlfrontier.URLFrontierGrpc;
import crawlercommons.urlfrontier.URLFrontierGrpc.URLFrontierStub;
import crawlercommons.urlfrontier.Urlfrontier.GetParams;
import crawlercommons.urlfrontier.Urlfrontier.URLInfo;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;
import org.apache.storm.Config;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;

public class Spout extends AbstractQueryingSpout {

    private static final Logger LOG = LogManager.getLogger(Spout.class);

    private static final Marker URL_FRONTIER_SPOUT_CHANNEL_MARKER =
            MarkerManager.getMarker("UrlFrontier_Spout_Channel");
    private static final Marker STATE_CHANGE_MARKER =
            MarkerManager.getMarker("UFSC_StateChanged")
                    .addParents(URL_FRONTIER_SPOUT_CHANNEL_MARKER);

    private ManagedChannel channel;

    private URLFrontierStub frontier;

    private int maxURLsPerBucket;

    private int maxBucketNum;

    private int delayRequestable;

    @Override
    public void open(
            Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        super.open(conf, context, collector);

        // host and port of URL Frontier(s)
        List<String> addresses = ConfUtils.loadListFromConf(conf, "urlfrontier.address");

        String address = null;

        // check that we have the same number of tasks and frontier nodes
        if (addresses.size() > 1) {
            int totalTasks = context.getComponentTasks(context.getThisComponentId()).size();
            if (totalTasks != addresses.size()) {
                String message =
                        "Not one task per frontier node. " + totalTasks + " vs " + addresses.size();
                LOG.error(message);
                throw new RuntimeException();
            }
            int nodeIndex = context.getThisTaskIndex();
            Collections.sort(addresses);
            address = addresses.get(nodeIndex);
        } else if (addresses.size() == 1) {
            LOG.warn("urlfrontier.address with a size of one is not used!");
        }

        if (address == null) {
            String host = ConfUtils.getString(conf, "urlfrontier.host", "localhost");
            int port = ConfUtils.getInt(conf, "urlfrontier.port", 7071);
            address = host + ":" + port;
        }

        maxURLsPerBucket = ConfUtils.getInt(conf, "urlfrontier.max.urls.per.bucket", 10);

        maxBucketNum = ConfUtils.getInt(conf, "urlfrontier.max.buckets", 10);

        // initialise the delay requestable with the timeout for messages
        delayRequestable =
                ConfUtils.getInt(conf, Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, delayRequestable);

        // then override with any user specified config
        delayRequestable =
                ConfUtils.getInt(conf, "urlfrontier.delay.requestable", delayRequestable);

        LOG.info("Initialisation of connection to URLFrontier service on {}", address);

        // add the default port if missing
        if (!address.contains(":")) {
            address += ":7071";
        }

        channel = ChannelManager.getChannel(address);
        frontier = URLFrontierGrpc.newStub(channel).withWaitForReady();
        LOG.debug("State of Channel: {}", channel.getState(false));
    }

    @Override
    protected void populateBuffer() {

        LOG.debug(
                "Populating buffer - max queues {} - max URLs per queues {}",
                maxBucketNum,
                maxURLsPerBucket);

        GetParams request =
                GetParams.newBuilder()
                        .setMaxUrlsPerQueue(maxURLsPerBucket)
                        .setMaxQueues(maxBucketNum)
                        .setDelayRequestable(delayRequestable)
                        .build();

        final AtomicInteger atomicint = new AtomicInteger();
        final long start = System.currentTimeMillis();

        StreamObserver<URLInfo> responseObserver =
                new StreamObserver<>() {

                    @Override
                    public void onNext(URLInfo item) {
                        Metadata m = new Metadata();
                        item.getMetadataMap()
                                .forEach(
                                        (k, v) -> {
                                            for (int index = 0;
                                                 index < v.getValuesCount();
                                                 index++) {
                                                m.addValue(k, v.getValues(index));
                                            }
                                        });
                        buffer.add(item.getUrl(), m, item.getKey());
                        atomicint.addAndGet(1);
                    }

                    @Override
                    public void onError(Throwable t) {

                        if (t instanceof io.grpc.StatusRuntimeException) {
                            io.grpc.StatusRuntimeException e = (io.grpc.StatusRuntimeException) t;

                            StringBuilder sb =
                                    new StringBuilder("StatusRuntimeException: ")
                                            .append(e.getStatus().toString());

                            io.grpc.Metadata trailers = e.getTrailers();
                            if (trailers != null) {
                                sb.append(" ").append(trailers);
                            }
                            LOG.error(sb.toString(), e);
                        } else {
                            LOG.error("Exception caught", t);
                        }

                        markQueryReceivedNow();
                    }

                    @Override
                    public void onCompleted() {
                        final long end = System.currentTimeMillis();
                        LOG.debug(
                                "Got {} URLs from the frontier in {} msec",
                                atomicint.get(),
                                (end - start));
                        markQueryReceivedNow();
                    }
                };

        LOG.trace("isInquery set to true");
        isInQuery.set(true);

        frontier.getURLs(request, responseObserver);
    }

    @Override
    public void ack(Object msgId) {
        LOG.debug("Ack for {}", msgId);
        super.ack(msgId);
    }

    @Override
    public void fail(Object msgId) {
        LOG.info("Fail for {}", msgId);
        super.fail(msgId);
    }

    @Override
    public void close() {
        super.close();
        LOG.info("Shutting down connection to URLFrontier service");
        ChannelManager.returnChannel(channel);
    }
}
