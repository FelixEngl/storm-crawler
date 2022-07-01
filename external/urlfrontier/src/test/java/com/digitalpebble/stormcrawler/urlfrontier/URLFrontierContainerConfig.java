package com.digitalpebble.stormcrawler.urlfrontier;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.Range;

public interface URLFrontierContainerConfig {
    void setRocksPath(@NotNull String path);

    @Nullable String getRocksPath();

    void setPrometheusPort(@Range(from = 0, to = 65535) int port);

    @Range(from = 0, to = 65535)
    int getPrometheusPort();
}
