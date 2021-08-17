package com.google.cloud.hadoop.fs.gcs;

public enum GHFSStatisticTypeEnum {
    /**
     * Counter.
     */
    TYPE_COUNTER,

    /**
     * Duration.
     */
    TYPE_DURATION,

    /**
     * Gauge.
     */
    TYPE_GAUGE,

    /**
     * Quantile.
     */
    TYPE_QUANTILE,
}
