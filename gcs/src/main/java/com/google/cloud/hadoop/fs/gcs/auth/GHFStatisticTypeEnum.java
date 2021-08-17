package com.google.cloud.hadoop.fs.gcs.auth;

public enum GHFStatisticTypeEnum {
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
