package com.google.cloud.hadoop.fs.gcs;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceStability;
import com.google.cloud.hadoop.fs.gcs.GHFSStatisticTypeEnum;
import org.apache.hadoop.fs.statistics.StoreStatisticNames;
import org.apache.hadoop.fs.statistics.StreamStatisticNames;

import static com.google.cloud.hadoop.fs.gcs.GHFSStatisticTypeEnum.TYPE_COUNTER;
import static com.google.cloud.hadoop.fs.gcs.GHFSStatisticTypeEnum.TYPE_DURATION;
import static com.google.cloud.hadoop.fs.gcs.GHFSStatisticTypeEnum.TYPE_GAUGE;
import static com.google.cloud.hadoop.fs.gcs.GHFSStatisticTypeEnum.TYPE_QUANTILE;


@InterfaceStability.Unstable
public enum GHFSStatistic {
    /* Low-level duration counters */

    ACTION_HTTP_HEAD_REQUEST(
            StoreStatisticNames.ACTION_HTTP_HEAD_REQUEST,
            "HEAD request.",
            TYPE_DURATION),
    ACTION_HTTP_GET_REQUEST(
            StoreStatisticNames.ACTION_HTTP_GET_REQUEST,
            "GET request.",
            TYPE_DURATION);



    /**
     * A map used to support the {@link #fromSymbol(String)} call.
     */
    private static final Map<String, GHFSStatistic> SYMBOL_MAP =
            new HashMap<>(GHFSStatistic.values().length);
    static {
        for (GHFSStatistic stat : values()) {
            SYMBOL_MAP.put(stat.getSymbol(), stat);
        }
    }


    /**
     * Statistic definition.
     * @param symbol name
     * @param description description.
     * @param type type
     */
    GHFSStatistic(String symbol, String description, GHFSStatisticTypeEnum type) {
        this.symbol = symbol;
        this.description = description;
        this.type = type;
    }

    /** Statistic name. */
    private final String symbol;

    /** Statistic description. */
    private final String description;

    /** Statistic type. */
    private final GHFSStatisticTypeEnum type;

    public String getSymbol() {
        return symbol;
    }

    /**
     * Get a statistic from a symbol.
     * @param symbol statistic to look up
     * @return the value or null.
     */
    public static GHFSStatistic fromSymbol(String symbol) {
        return SYMBOL_MAP.get(symbol);
    }

    public String getDescription() {
        return description;
    }

    /**
     * The string value is simply the symbol.
     * This makes this operation very low cost.
     * @return the symbol of this statistic.
     */
    @Override
    public String toString() {
        return symbol;
    }

    /**
     * What type is this statistic?
     * @return the type.
     */
    public GHFSStatisticTypeEnum getType() {
        return type;
    }
}
