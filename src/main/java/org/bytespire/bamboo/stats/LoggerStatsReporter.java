package org.bytespire.bamboo.stats;

import static org.apache.logging.log4j.LogManager.getLogger;

import java.util.List;
import org.apache.logging.log4j.Logger;
import org.bytespire.bamboo.BambooReader;
import org.bytespire.bamboo.Column;
import org.bytespire.bamboo.ColumnFamily;

public class LoggerStatsReporter implements StatsReporter {
  private static final Logger logger = getLogger(BambooReader.class.getName());

  @Override
  public void report(
      String datasetPath, List<Column> columnsRequested, List<ColumnFamily> columnFamiliesRead)
      throws ReporterException {
    logger.info(
        "Read operation for dataset: {}, requested columns: {}, column families read: {}",
        datasetPath,
        columnsRequested,
        columnFamiliesRead);
  }
}
