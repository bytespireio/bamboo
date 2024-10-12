package org.bytespire.bamboo.stats;

import java.util.List;
import org.bytespire.bamboo.Column;
import org.bytespire.bamboo.ColumnFamily;

public interface StatsReporter {

  public void report(
      String datasetPath, List<Column> columnsRequested, List<ColumnFamily> columnFamiliesRead)
      throws ReporterException;
}
