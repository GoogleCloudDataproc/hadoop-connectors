package com.google.cloud.hadoop.fs.gcs;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public final class Constants {

    private Constants() {
    }

    /**
     * Gauge name for the input policy : {@value}.
     * This references an enum currently exclusive to the GS stream.
     */
    public static final String STREAM_READ_GAUGE_INPUT_POLICY =
            "stream_read_gauge_input_policy";
}
