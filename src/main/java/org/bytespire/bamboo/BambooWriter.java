package org.bytespire.bamboo;

import static org.apache.logging.log4j.LogManager.getLogger;
import static org.bytespire.bamboo.Bamboo.METADATA_FORMAT;
import static org.bytespire.bamboo.Constants.ROW_ID;
import static org.bytespire.bamboo.Kolumn.ROW_ID_COLUMN;
import static org.bytespire.bamboo.Utils.hashOfColumn;

import java.io.File;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;

public class BambooWriter {
  private final SparkSession spark;
  private final Set<Kolumn> srcDfColumns = new LinkedHashSet<>();
  private SaveMode saveMode = SaveMode.Overwrite;
  private Dataset<Row> dataToWrite;
  private boolean writeZionCf = false;
  private final List<ColumnFamily> columnFamiliesToWrite = new ArrayList<>(10);
  private String format = "parquet";

  private static final Logger logger = getLogger(BambooWriter.class.getName());

  BambooWriter(SparkSession spark, Dataset<Row> dataToWrite) {
    this.dataToWrite = dataToWrite;
    this.spark = spark;
    StructType schema = dataToWrite.schema();
    for (String columnName : dataToWrite.columns()) {
      DataType columnType = schema.apply(columnName).dataType();
      srcDfColumns.add(new Kolumn(columnName, columnType));
    }
  }

  public BambooWriter mode(SaveMode saveMode) {
    if (saveMode == null) {
      saveMode = SaveMode.Overwrite;
      logger.info("SaveMode not specified. Defaulting to SaveMode.Overwrite");
    }
    this.saveMode = saveMode;
    return this;
  }

  public BambooWriter format(String format) {
    if (StringUtils.isBlank(format)) {
      format = "parquet";
      logger.info("Format not specified. Defaulting to parquet");
    }
    this.format = format;
    return this;
  }

  public BambooWriter writeZionCf(boolean writeZionCf) {
    this.writeZionCf = writeZionCf;
    return this;
  }

  public BambooWriter columnFamily(ColumnFamily columnFamily) {
    if (columnFamily == null) {
      return this;
    }
    if (columnFamily.getColumns().isEmpty()) {
      throw new IllegalArgumentException(
          "Column family " + columnFamily.getName() + " has no columns to write.");
    }

    for (Kolumn kolumn : columnFamily.getColumns()) {
      if (!srcDfColumns.contains(kolumn)) {
        throw new IllegalArgumentException("Column " + kolumn + " not found in the input dataset.");
      }
    }
    List<Kolumn> copy = new ArrayList<>(columnFamily.getColumns());
    copy.add(ROW_ID_COLUMN);

    columnFamiliesToWrite.add(
        new ColumnFamily(columnFamily.getName(), Collections.unmodifiableList(copy)));
    return this;
  }

  private boolean handleExistingData(String outputPath) {
    Dataset<Row> existingMetaDataDf = null;
    try {
      existingMetaDataDf = spark.read().format(METADATA_FORMAT).load(outputPath);
    } catch (Exception e) {
      if (!Utils.IsPathNotFound(e)) {
        throw new RuntimeException(e);
      }
    }
    if (existingMetaDataDf == null) {
      return false;
    }

    if (saveMode == SaveMode.Append) {
      if (!columnFamiliesToWrite.isEmpty()) {
        throw new IllegalArgumentException(
            "Data allready exists in the path provided. You have specified columnFamilies for the writer to save in Append mode. Append mode is allowed along with specified columnFamilies, when writing for the first time.");
      }
      existingMetaDataDf.createOrReplaceTempView("existingMetaData");
      Dataset<Row> colFams =
          spark.sql(
              String.format(
                  "select %s, %s, %s, %s, %s, %s from existingMetaData",
                  Constants.COLUMN_FAMILY_NAME,
                  Constants.COLUMNS,
                  Constants.COLUMN_TYPES,
                  Constants.PATH,
                  Constants.FORMAT,
                  Constants.PARENT_PATH));

      List<ColumnFamily> existingColumnFamilies = new ArrayList<>();
      List<Row> rows = colFams.collectAsList();
      for (Row row : rows) {
        String cfName = row.getString(0);
        List<String> columnNames = scala.collection.JavaConverters.seqAsJavaList(row.getSeq(1));
        List<String> columnTypes = scala.collection.JavaConverters.seqAsJavaList(row.getSeq(2));
        List<Kolumn> kolumns = new ArrayList<>();
        for (int i = 0; i < columnNames.size(); i++) {
          kolumns.add(new Kolumn(columnNames.get(i), DataType.fromJson(columnTypes.get(i))));
        }
        ColumnFamily cf = new ColumnFamily(cfName, kolumns);
        cf.setPath(row.getString(3));
        cf.setFormat(row.getString(4));
        cf.setParentPath(row.getString(5));
        existingColumnFamilies.add(cf);
      }
      columnFamiliesToWrite.addAll(existingColumnFamilies);
    }
    return true;
  }

  public void save(String outputPath) {
    if (StringUtils.isBlank(outputPath)) {
      throw new IllegalArgumentException("Output path cannot be null or empty.");
    }
    final boolean dataExists = handleExistingData(outputPath);
    if (dataExists && saveMode == SaveMode.Ignore) {
      logger.warn(
          "You have specified 'Ignore' saveMode. Data allready exists at the path: "
              + outputPath
              + ". Ignoring the write operation.");
      return;
    }

    if (dataExists && saveMode == SaveMode.ErrorIfExists) {
      throw new IllegalStateException(
          "Using ErrorIfExists saveMode. Data allready exists at the path: "
              + outputPath
              + ". hence the error");
    }

    if (dataExists && saveMode == SaveMode.Append) {
      // dont entertain this, use existing cfs.
      writeZionCf = false;
    }

    final String pathId = Utils.getPathId(outputPath);
    logger.info("Writing data to path: {}, pathId: {}", outputPath, pathId);

    prepareColumnFamiliesForWriting(outputPath);
    writeMetaData(outputPath, columnFamiliesToWrite);

    dataToWrite = addRowId(dataToWrite);
    logger.info(
        "Persisting input data to cache with strategy: {}",
        Bamboo.getWritingCacheStrategy().toString());
    dataToWrite.persist(Bamboo.getWritingCacheStrategy());
    logInputDataFrameWithRowIds(dataToWrite);

    final String viewName = "bamboo_" + pathId;
    logger.info("Creating view for the input data: {}", viewName);
    dataToWrite.createOrReplaceTempView(viewName);

    for (ColumnFamily cf : columnFamiliesToWrite) {
      List<String> columnNames =
          cf.getColumns().stream().map(Kolumn::getName).collect(Collectors.toList());
      String cols = String.join(",", columnNames);
      Dataset<Row> columnFamilyDf = spark.sql("select " + cols + " from " + viewName);
      Utils.debugDataFrame(columnFamilyDf, "col_family_" + cf.getName());
      columnFamilyDf.write().format(cf.getFormat()).mode(saveMode).save(cf.getPath());
      logger.info(
          "Written column family: {} to path: {}, mode: {}, format: {}",
          cf.getName(),
          cf.getPath(),
          saveMode,
          cf.getFormat());
    }
    dataToWrite.unpersist();
    logger.info("Un-persisted input data from cache. Writing complete!");
  }

  private void prepareColumnFamiliesForWriting(String parentDfPath) {
    // set paths
    if (columnFamiliesToWrite.isEmpty()) {
      ColumnFamily zionCf = new ColumnFamily(Constants.ZION_CF, new ArrayList<>(srcDfColumns));
      columnFamiliesToWrite.add(zionCf);
      logger.warn(
          "No column families specified to write. Writing all columns to zion column family.");
    } else {
      if (writeZionCf) {
        boolean zionCfExists = false;
        for (ColumnFamily cf : columnFamiliesToWrite) {
          if (cf.getName().equals(Constants.ZION_CF)) {
            zionCfExists = true;
            break;
          }
        }
        if (!zionCfExists) {
          ColumnFamily zionCf = new ColumnFamily(Constants.ZION_CF, new ArrayList<>(srcDfColumns));
          columnFamiliesToWrite.add(zionCf);
          logger.info("As instructed, Writing all columns to zion column family.");
        }
      }
    }

    for (ColumnFamily cf : columnFamiliesToWrite) {
      if (StringUtils.isBlank(cf.getParentPath())) {
        cf.setParentPath(parentDfPath);
      }
      if (StringUtils.isBlank(cf.getPath())) {
        cf.setPath(getPathForCf(parentDfPath, cf.getName()));
      }

      if (StringUtils.isBlank(cf.getFormat())) {
        cf.setFormat(format);
      }
    }
  }

  private void writeMetaData(final String parentDfPath, List<ColumnFamily> columnFamiliesToWrite) {
    List<Row> rows = new ArrayList<>();
    for (ColumnFamily cf : columnFamiliesToWrite) {
      rows.add(
          RowFactory.create(
              cf.getPath(),
              cf.getFormat(),
              cf.getName(),
              cf.getColumns().size(),
              cf.getColumns().stream().map(Kolumn::getName).collect(Collectors.toList()),
              cf.getParentPath(),
              Utils.getPathId(parentDfPath),
              cf.getColumns().stream().map(Kolumn::getTypeJson).collect(Collectors.toList())));
    }

    Dataset<Row> metaData = spark.createDataFrame(rows, Bamboo.getBambooMetaSchema());
    metaData
        .coalesce(1)
        .write()
        .mode(SaveMode.Overwrite)
        .format(METADATA_FORMAT)
        .save(parentDfPath);
    logger.info("Written metadata to path: {}", parentDfPath);
    Utils.debugDataFrame(metaData, "metaData_written");
  }

  static Dataset<Row> addRowId(Dataset<Row> data) {
    return data.withColumn("sid", org.apache.spark.sql.functions.monotonically_increasing_id())
        .withColumn("system_time", functions.current_timestamp())
        .withColumn(
            "concatenated_column",
            functions.concat(
                functions.col("sid").cast("string"), functions.col("system_time").cast("string")))
        .withColumn(ROW_ID, hashOfColumn(functions.col("concatenated_column")))
        .drop("sid", "system_time", "concatenated_column");
  }

  private static void logInputDataFrameWithRowIds(Dataset<Row> data) {
    if (System.getProperty("DEBUG_DATAFRAMES", "no").equals("yes")) {
      if (StorageLevel.NONE().equals(Bamboo.getWritingCacheStrategy())) {
        data.persist(StorageLevel.DISK_ONLY());
      }
      // why do we cache before showing it.
      // this show action is followed up by other actions which triggers the computation again.
      // if not cached, the computation will be triggered again.
      // the rowId column will be different in the second computation.
      Utils.debugDataFrame(data, "inputdata_with_rowId");
    }
  }

  private static String getPathForCf(String parentDfPath, String cfName) {
    return Bamboo.getStorePath()
        + File.separator
        + Utils.getPathId(parentDfPath)
        + File.separator
        + cfName;
  }
}
