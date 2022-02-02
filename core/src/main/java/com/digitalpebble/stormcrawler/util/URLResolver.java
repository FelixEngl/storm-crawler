package com.digitalpebble.stormcrawler.util;

import java.net.*;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.LoggerFactory;

/**
 * Describes the strategies used by the RemoteDriverProtocol to resolve the given addresses. This is
 * sometimes necessary, when running in a docker container or special environment.
 */
// TODO: Right now it only resolves to the first possible target. Maybe we need some kind of
//  regex-pattern-matching for multiple resolved addresses at some point?
public enum URLResolver {
    NOTHING {
        @NotNull
        @Override
        public URL resolve(@NotNull URL url) {
            return url;
        }

        @Contract("_ -> fail")
        @NotNull
        @Override
        protected String getNewAddressName(@NotNull URL url) throws UnsupportedOperationException {
            throw new UnsupportedOperationException(
                    "Nothing does literally nothing else than returning the same value.");
        }
    },
    IP {
        @NotNull
        @Override
        protected String getNewAddressName(@NotNull URL url) throws UnknownHostException {
            final InetAddress byName = InetAddress.getByName(url.getHost());
            return byName.getHostAddress();
        }
    },
    IPv4 {
        @NotNull
        @Override
        protected String getNewAddressName(@NotNull URL url) throws UnknownHostException {
            final InetAddress byName = Inet4Address.getByName(url.getHost());
            return byName.getHostAddress();
        }
    },
    /** Only resolves if IPv6 is possible. Otherwise, uses IPv4. */
    IPv6 {
        @NotNull
        @Override
        protected String getNewAddressName(@NotNull URL url) throws UnknownHostException {
            final InetAddress byName = Inet6Address.getByName(url.getHost());
            return byName.getHostAddress();
        }
    },
    HOSTNAME {
        @NotNull
        @Override
        protected String getNewAddressName(@NotNull URL url) throws UnknownHostException {
            final InetAddress byName = InetAddress.getByName(url.getHost());
            return byName.getHostName();
        }
    },
    CANONICAL_HOSTNAME {
        @NotNull
        @Override
        protected String getNewAddressName(@NotNull URL url) throws UnknownHostException {
            final InetAddress byName = InetAddress.getByName(url.getHost());
            return byName.getCanonicalHostName();
        }
    };

    protected static final org.slf4j.Logger LOG = LoggerFactory.getLogger(URLResolver.class);

    /**
     * Tries to resolve the given {@code url} to a specific target. Returns {@code null} if it can
     * not resolve the {@code url}.
     */
    @Nullable
    public URL resolve(@NotNull URL url) {
        String replacement = null;
        try {
            replacement = getNewAddressName(url);
            return new URL(url.getProtocol(), replacement, url.getPort(), url.getFile());
        } catch (UnknownHostException e) {
            LOG.warn("Was not able to resolve the address {} to {}.", url.toExternalForm(), name());
            return null;
        } catch (MalformedURLException e) {
            LOG.warn(
                    "Was not able to create the new URL from {} with {} in {}.",
                    url.toExternalForm(),
                    replacement,
                    name());
            return null;
        }
    }

    @NotNull
    protected abstract String getNewAddressName(@NotNull URL url) throws UnknownHostException;
}
