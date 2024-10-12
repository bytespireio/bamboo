package org.bytespire.bamboo;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.*;
import org.apache.spark.sql.Column;

public class Utils {

  public static boolean isZionColumnFamily(ColumnFamily cf) {
    return Constants.ZION_CF.equals(cf.getName());
  }

  public static void debugDataFrame(Dataset<Row> df, String dfName) {
    if (System.getProperty("DEBUG_DATAFRAMES", "no").equals("yes")) {
      df.withColumn("df_" + dfName, org.apache.spark.sql.functions.lit("")).show(20, false);
    }
  }

  public static Column hashOfColumn(Column column) {
    return functions.xxhash64(column);
  }

  public static String getPathId(String path) {
    return DigestUtils.md5Hex(path);
  }

  public static boolean IsPathNotFound(Throwable e) {
    // org.apache.spark.sql.AnalysisException: [PATH_NOT_FOUND] Path does not exist:
    return e instanceof AnalysisException
        && StringUtils.isNotBlank(e.getMessage())
        && e.getMessage().toLowerCase().contains("path_not_found");
  }
}
