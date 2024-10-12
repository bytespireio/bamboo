package org.bytespire.bamboo;

import static org.apache.logging.log4j.LogManager.getLogger;

import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import org.bytespire.bamboo.stats.LoggerStatsReporter;
import org.bytespire.bamboo.stats.StatsReporter;

public class Bamboo {

  private static final Logger logger = getLogger(Bamboo.class.getName());

  static StatsReporter statsReporter = new LoggerStatsReporter();

  static String METADATA_FORMAT = "json";
  private static final StructType bambooMetaSchema =
      new StructType()
          .add(Constants.PATH, DataTypes.StringType, false)
          .add(Constants.FORMAT, DataTypes.StringType, false)
          .add(Constants.COLUMN_FAMILY_NAME, DataTypes.StringType, false)
          .add(Constants.TOTAL_COLUMNS, DataTypes.IntegerType, false)
          .add(Constants.COLUMNS, DataTypes.createArrayType(DataTypes.StringType), false)
          .add(Constants.PARENT_PATH, DataTypes.StringType, false)
          .add(Constants.PARENT_HASH, DataTypes.StringType, false)
          .add(Constants.COLUMN_TYPES, DataTypes.createArrayType(DataTypes.StringType), false);

  private static String storePath = "file:///tmp/bamboo";

  /** The default cache strategy for the source dataset while writing column families to disk. */
  private static StorageLevel writingCacheStrategy = StorageLevel.NONE();

  static {
    logger.info("Bamboo initialized with default store path: " + storePath);
    logger.info(
        "Bamboo initialized with default writing cache strategy: "
            + writingCacheStrategy.toString());
  }

  public static void setStorePath(String path) {
    logger.info("Setting bamboo store path to: " + path);
    storePath = path;
  }

  public static StructType getBambooMetaSchema() {
    return bambooMetaSchema;
  }

  public static String getStorePath() {
    return storePath;
  }

  public static StorageLevel getWritingCacheStrategy() {
    return writingCacheStrategy;
  }

  public static void setWritingCacheStrategy(StorageLevel writingCacheStrategy) {
    logger.info("Setting bamboo writing cache strategy to: " + writingCacheStrategy.toString());
    Bamboo.writingCacheStrategy = writingCacheStrategy;
  }

  public static BambooReader read(SparkSession spark) {
    return new BambooReader(spark);
  }

  public static BambooWriter write(SparkSession spark, Dataset<Row> input) {
    return new BambooWriter(spark, input);
  }

  public static StatsReporter setStatsReporter(StatsReporter sr) {
    return statsReporter = sr;
  }
}
