package com.google.cloud.hadoop.fs.gcs.benchmarking.util;

import com.google.common.flogger.GoogleLogger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A utility class to parse JVM arguments from the HADOOP_OPTS environment variable for use in JMH
 * benchmark runners.
 */
public final class JMHArgs {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  // Private constructor to prevent instantiation of this utility class.
  private JMHArgs() {}

  /**
   * Extracts JVM arguments from the HADOOP_OPTS environment variable and intelligently modifies the
   * YourKit profiler's agent path to include a benchmark-specific session name.
   *
   * @param benchmarkSessionName The session name to add to the YourKit agent path.
   * @return An array of JVM arguments for the JMH OptionsBuilder.
   */
  public static String[] fromEnv(String benchmarkSessionName) {
    List<String> jvmArgs = new ArrayList<>();
    String agentPathArg = null;
    String hadoopOptsEnv = System.getenv("HADOOP_OPTS");

    if (hadoopOptsEnv != null && !hadoopOptsEnv.isEmpty()) {
      List<String> rawJvmArgs = Arrays.asList(hadoopOptsEnv.split("\\s+"));
      for (String arg : rawJvmArgs) {
        if (arg.startsWith("-agentpath:")) {
          agentPathArg = arg;
        } else {
          jvmArgs.add(arg); // Add other non-agent args directly
        }
      }
    }

    // If a YourKit agent path was found, modify it.
    if (agentPathArg != null) {
      String modifiedAgentPath = getModifiedAgentPath(agentPathArg, benchmarkSessionName);
      jvmArgs.add(modifiedAgentPath);
      logger.atInfo().log("Modified YourKit agent path: %s", modifiedAgentPath);
    } else {
      logger.atWarning().log("YourKit agent not found in HADOOP_OPTS. Profiling will not occur.");
    }

    return jvmArgs.toArray(new String[0]);
  }

  /**
   * Modifies a YourKit agent path string to include or append to the 'sessionname' option.
   *
   * @param agentPath The original -agentpath string.
   * @param sessionName The name to append to the session.
   * @return The modified agent path string.
   */
  private static String getModifiedAgentPath(String agentPath, String sessionName) {
    Pattern pattern = Pattern.compile("(sessionname=)([^,]+)");
    Matcher matcher = pattern.matcher(agentPath);

    if (matcher.find()) {
      // If sessionname exists, append to it.
      return matcher.replaceFirst("$1" + matcher.group(2) + "_" + sessionName);
    }

    // If sessionname does not exist, add it.
    if (agentPath.contains("=")) {
      return agentPath + ",sessionname=" + sessionName;
    } else {
      return agentPath + "=sessionname=" + sessionName;
    }
  }
}
