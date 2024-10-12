package org.bytespire.bamboo.stats;

import java.util.List;
import org.bytespire.bamboo.ColumnFamily;
import org.bytespire.bamboo.Kolumn;

public class BlackHoleReporter implements StatsReporter {
  @Override
  public void report(
      String datasetPath, List<Kolumn> columnsRequested, List<ColumnFamily> columnFamiliesRead)
      throws ReporterException {
    // do nothing :)
  }
}
