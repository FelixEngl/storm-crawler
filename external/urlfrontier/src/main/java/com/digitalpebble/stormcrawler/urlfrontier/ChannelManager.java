package com.digitalpebble.stormcrawler.urlfrontier;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.jetbrains.annotations.NotNull;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Manages the connections on a single machine.
 */
final class ChannelManager {

    private ChannelManager(){}

    private final static Object lock = new Object();

    private final static HashMap<String, ManagedChannelTuple> channels = new HashMap<>();

    @NotNull
    static ManagedChannel getChannel(@NotNull String target){
        ManagedChannelTuple result = channels.get(target);
        if (result != null){
            result.inc();
            return result.channel;
        }
        synchronized (lock){
            result = channels.get(target);
            if (result != null) {
                result.inc();
                return result.channel;
            }
            result = new ManagedChannelTuple(
                    ManagedChannelBuilder.forTarget(target).usePlaintext().build()
            );
            channels.put(
                    target,
                    result
            );
            result.inc();
            return result.channel;
        }
    }

    static void returnChannel(@NotNull ManagedChannel channel){
        for (var value : channels.entrySet()) {
            if (channel == value.getValue().channel){
                if (value.getValue().dec() == 0){
                    synchronized (lock){
                        if (value.getValue().get() == 0){
                            value.getValue().channel.shutdown();
                            channels.remove(value.getKey());
                        }
                    }
                }
                return;
            }
        }
    }

    private static class ManagedChannelTuple {
        @NotNull ManagedChannel channel;
        @NotNull private final AtomicInteger usedBy = new AtomicInteger(0);

        public ManagedChannelTuple(@NotNull ManagedChannel channel) {
            this.channel = channel;
        }

        int get() {
            return usedBy.get();
        }

        int inc(){
            return usedBy.incrementAndGet();
        }

        int dec(){
            return usedBy.decrementAndGet();
        }
    }


}
