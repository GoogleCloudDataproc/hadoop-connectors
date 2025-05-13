package com.google.cloud.hadoop.util;

import com.google.cloud.hadoop.util.interceptors.LoggingInterceptor;
import com.google.cloud.logging.LogEntry;
import com.google.cloud.logging.Logging;
import com.google.cloud.logging.Payload.StringPayload;
import com.google.cloud.logging.Severity;
import java.util.Collections;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.*;

public class LoggingInterceptorTest {

    private Logging mockLogging;
    private LoggingInterceptor loggingInterceptor;

    @Before
    public void setUp() {
        mockLogging = mock(Logging.class);
        loggingInterceptor = new LoggingInterceptor() {
            @Override
            protected Logging createLoggingService() {
                return mockLogging;
            }
        };
    }

    @Test
    public void publishesLogEntryWithCorrectSeverity() {
        LogRecord record = new LogRecord(Level.SEVERE, "Critical error occurred");
        loggingInterceptor.publish(record);

        LogEntry expectedEntry = LogEntry.newBuilder(StringPayload.of("Critical error occurred"))
                .setSeverity(Severity.ERROR)
                .setLogName("gcs-connector")
                .build();

        verify(mockLogging).write(Collections.singleton(expectedEntry));
    }

    @Test
    public void doesNotPublishNonLoggableRecord() {
        LoggingInterceptor nonLoggableInterceptor = new LoggingInterceptor() {
            @Override
            public boolean isLoggable(LogRecord record) {
                return false; // Force isLoggable() to return false
            }

            @Override
            protected Logging createLoggingService() {
                return mockLogging;
            }
        };

        LogRecord record = new LogRecord(Level.FINE, "Debug message");
        nonLoggableInterceptor.publish(record);

        verify(mockLogging, never()).write(any());
    }

    @Test
    public void flushesLoggingService() {
        loggingInterceptor.flush();
        verify(mockLogging).flush();
    }

    @Test
    public void mapsUnknownLogLevelToDefaultSeverity() {
        LogRecord record = new LogRecord(Level.CONFIG, "Configuration message");
        loggingInterceptor.publish(record);

        LogEntry expectedEntry = LogEntry.newBuilder(StringPayload.of("Configuration message"))
                .setSeverity(Severity.DEFAULT)
                .setLogName("gcs-connector")
                .build();

        verify(mockLogging).write(Collections.singleton(expectedEntry));
    }
}