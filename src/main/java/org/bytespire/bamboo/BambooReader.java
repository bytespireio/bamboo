package org.bytespire.bamboo;

import static org.apache.logging.log4j.LogManager.getLogger;
import static org.bytespire.bamboo.Bamboo.METADATA_FORMAT;
import static org.bytespire.bamboo.Bamboo.statsReporter;

import java.util.*;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;
import org.bytespire.bamboo.stats.ReporterException;

public class BambooReader {

  private static final Logger logger = getLogger(BambooReader.class.getName());
  private final SparkSession spark;
  private final Set<String> columnsToRead = new HashSet<>();

  BambooReader(SparkSession spark) {
    this.spark = spark;
  }

  public BambooReader format(String format) {
    // NO-OP, Bamboo can self detect the format
    return this;
  }

  public BambooReader columns(String... columnNames) {
    for (String column : columnNames) {
      if (StringUtils.isBlank(column)) {
        throw new IllegalArgumentException("column name cannot be null or empty");
      }
      columnsToRead.add(column.toLowerCase());
    }
    return this;
  }

  public Dataset<Row> load(String path) {
    if (StringUtils.isBlank(path)) {
      throw new IllegalArgumentException("input path cannot be null or empty");
    }
    Dataset<Row> metaDataDf = spark.read().format(METADATA_FORMAT).load(path);
    Utils.debugDataFrame(metaDataDf, "metaData");

    Map<String, List<ColumnFamily>> columnFamiliesPerPath =
        getColumnFamiliesFromMetaData(metaDataDf);

    if (columnFamiliesPerPath.size() > 1) {
      logger.info("Glob path provided for read.");
      checkForConsistencyInColumnFamiliesAcrossPaths(columnFamiliesPerPath);
    }

    Set<String> uniqColsInData = new HashSet<>();
    if (columnsToRead.isEmpty()) {
      logger.warn("No columns specified to be read  => select * from ..,");
      // choose one parentDfPath and get all the columnFams for that path.
      List<ColumnFamily> columnFamilyList = new ArrayList<>(columnFamiliesPerPath.values()).get(0);
      for (ColumnFamily cf : columnFamilyList) {
        List<String> columnNames =
            cf.getColumns().stream().map(Kolumn::getName).collect(Collectors.toList());
        uniqColsInData.addAll(columnNames);
      }
      uniqColsInData.remove(Constants.ROW_ID);
      columnsToRead.addAll(uniqColsInData);
    }

    List<ColumnFamily> columnFamiliesToLoad =
        getColumnFamiliesToLoad(columnFamiliesPerPath, columnsToRead);
    List<String> familiyNamesToLoad =
        columnFamiliesToLoad.stream().map(ColumnFamily::getName).collect(Collectors.toList());
    Dataset<Row> toReturn = null;

    for (String pathOfParentDf : columnFamiliesPerPath.keySet()) {
      List<ColumnFamily> columnFamilies = columnFamiliesPerPath.get(pathOfParentDf);
      logger.info("Loading column families for dataset with parent path: {}", pathOfParentDf);
      Dataset<Row> dataset = loadFromCfs(spark, columnFamilies, familiyNamesToLoad);
      if (toReturn == null) {
        toReturn = dataset;
      } else {
        toReturn = toReturn.unionByName(dataset);
        logger.info(
            "Unioned dataset with column families of dataset with parent path: {}", pathOfParentDf);
      }
    }
    Utils.debugDataFrame(toReturn, "reconstructed_for_read");

    try {
      StructType schema = toReturn.schema();
      List<Kolumn> colsRequested = new ArrayList<>();
      for (String colName : columnsToRead) {
        DataType colType = schema.apply(colName).dataType();
        colsRequested.add(new Kolumn(colName, colType));
      }
      statsReporter.report(path, colsRequested, columnFamiliesToLoad);
    } catch (ReporterException e) {
      logger.warn("Failed to collect stats for read.", e);
    }
    return toReturn;
  }

  static List<ColumnFamily> getColumnFamiliesToLoad(
      Map<String, List<ColumnFamily>> columnFamiliesPerPath, Set<String> columnsToRead)
      throws IllegalArgumentException {
    // select one path and take its cfs
    List<ColumnFamily> baseColumnFamilyList =
        new ArrayList<>(columnFamiliesPerPath.values()).get(0);

    // could there be a non-zion column family that satisfies all the columns to be read? if so take
    // it
    ColumnFamily optimalNonZionCf = null;
    for (ColumnFamily cf : baseColumnFamilyList) {
      if (Utils.isZionColumnFamily(cf)) {
        continue;
      }

      List<String> columnNamesInCf =
          cf.getColumns().stream().map(Kolumn::getName).collect(Collectors.toList());

      if (new HashSet<>(columnNamesInCf).containsAll(columnsToRead)) {
        if (optimalNonZionCf == null) {
          optimalNonZionCf = cf;
        } else {
          if (optimalNonZionCf.getColumns().size() > cf.getColumns().size()) {
            optimalNonZionCf = cf;
          }
        }
      }
    }
    if (optimalNonZionCf != null) {
      return Collections.singletonList(optimalNonZionCf);
    }

    Map<String, ColumnFamily> bestCandidateCfForColumn =
        getBestCandidateCfForColumn(baseColumnFamilyList, columnsToRead);

    if (columnsToRead.size() != bestCandidateCfForColumn.size()) {
      logger.warn(
          "Some columns to be read are not present in the column families. ColumnsToRead: {}, ColumnFamsThatCanBeLoaded: {}.",
          columnsToRead,
          bestCandidateCfForColumn.keySet());
      throw new IllegalArgumentException("column families cant satisfy the columns to be read.");
    }
    ColumnFamily zionCf =
        baseColumnFamilyList.stream()
            .filter(cf -> cf.getName().equals(Constants.ZION_CF))
            .findFirst()
            .orElse(null);
    return getMinNumberOfColumnFamiliesToLoadFromCandidates(
        bestCandidateCfForColumn, zionCf, columnsToRead);
  }

  private Map<String, List<ColumnFamily>> getColumnFamiliesFromMetaData(Dataset<Row> metaData) {
    // return map: key is parentDfPath vs value is list of column families for that parentDf
    Map<String, List<ColumnFamily>> columnFamiliesPerPath = new HashMap<>();
    List<Row> rows = metaData.collectAsList();
    for (Row row : rows) {
      String columnFamilyPath = row.getAs(Constants.PATH);
      String columnFamilyName = row.getAs(Constants.COLUMN_FAMILY_NAME);
      String format = row.getAs(Constants.FORMAT);
      String parentPath = row.getAs(Constants.PARENT_PATH);
      List<String> columns =
          scala.collection.JavaConverters.seqAsJavaList(row.getAs(Constants.COLUMNS));
      List<String> columnTypes =
          scala.collection.JavaConverters.seqAsJavaList(row.getAs(Constants.COLUMN_TYPES));
      List<Kolumn> kolumns = new ArrayList<>();
      for (int i = 0; i < columns.size(); i++) {
        kolumns.add(new Kolumn(columns.get(i), DataType.fromJson(columnTypes.get(i))));
      }
      ColumnFamily columnFamily = new ColumnFamily(columnFamilyName, kolumns, format);
      columnFamily.setFormat(format);
      columnFamily.setParentPath(parentPath);
      columnFamily.setPath(columnFamilyPath);

      List<ColumnFamily> cfForPath =
          columnFamiliesPerPath.computeIfAbsent(parentPath, k -> new ArrayList<>());
      cfForPath.add(columnFamily);
    }
    return columnFamiliesPerPath;
  }

  private static Map<String, ColumnFamily> getBestCandidateCfForColumn(
      List<ColumnFamily> columnFamiliesInMeta, Set<String> columnsToRead) {
    Map<String, ColumnFamily> bestCandidateCfForColumn =
        new HashMap<>(); // key is colName, value is cf
    for (String columnToRead : columnsToRead) {
      // handle rid column
      for (ColumnFamily candidateCf : columnFamiliesInMeta) {
        List<String> columnNamesInCf =
            candidateCf.getColumns().stream().map(Kolumn::getName).collect(Collectors.toList());
        if (columnNamesInCf.contains(columnToRead)) {
          ColumnFamily prevCandidate = bestCandidateCfForColumn.getOrDefault(columnToRead, null);
          if (prevCandidate == null) {
            bestCandidateCfForColumn.put(columnToRead, candidateCf);
          } else {
            if (prevCandidate.getColumns().size() > candidateCf.getColumns().size()) {
              bestCandidateCfForColumn.put(columnToRead, candidateCf);
            }
            // todo in case of tie of number of columns choose smaller datatypes ?
          }
        }
      }
    }

    return bestCandidateCfForColumn;
  }

  /**
   * If a column is only available in zion cf then zion cf is the only cf to load.
   *
   * @param candidateCfForColumn key is colName, value is cf
   * @return list of column families to load
   */
  static List<ColumnFamily> getMinNumberOfColumnFamiliesToLoadFromCandidates(
      Map<String, ColumnFamily> candidateCfForColumn,
      ColumnFamily zionCf,
      Set<String> columnsToRead) {

    // if a column is only available in zion cf then zion cf is the only cf to load.
    for (ColumnFamily bestCandidateCf : candidateCfForColumn.values()) {
      if (bestCandidateCf.getName().equals(Constants.ZION_CF)) {
        return Collections.singletonList(bestCandidateCf);
      }
    }

    // if a candidate cf for a column contains all the columnsToRead, then we should load that cf
    // only.
    for (ColumnFamily bestCandidateCf : candidateCfForColumn.values()) {
      List<String> columnNamesInCf =
          bestCandidateCf.getColumns().stream().map(Kolumn::getName).collect(Collectors.toList());
      HashSet<String> colsInCandidateCf = new HashSet<>(columnNamesInCf);
      if (colsInCandidateCf.containsAll(columnsToRead)) {
        return Collections.singletonList(bestCandidateCf);
      }
    }

    if (zionCf != null) {
      return Collections.singletonList(zionCf);
    }

    // else we need to load all the candidate cfs and eventually join them.
    return new ArrayList<>(new HashSet<>(candidateCfForColumn.values()));
  }

  private void checkForConsistencyInColumnFamiliesAcrossPaths(
      Map<String, List<ColumnFamily>> columnFamiliesPerPath) throws IllegalStateException {
    Integer numFams = null;
    Set<String> cfNames = new HashSet<>();
    // check for number of cfs,namse of those cfs across paths
    for (List<ColumnFamily> columnFamilies : columnFamiliesPerPath.values()) {
      if (numFams == null) {
        numFams = columnFamilies.size();
        cfNames.addAll(
            columnFamilies.stream().map(ColumnFamily::getName).collect(Collectors.toSet()));
      } else {
        if (numFams != columnFamilies.size()) {
          throw new IllegalStateException(
              "Column families across paths are not consistent. They differ in number of column families");
        }
        if (!cfNames.equals(
            columnFamilies.stream().map(ColumnFamily::getName).collect(Collectors.toSet()))) {
          throw new IllegalStateException(
              "Column families across paths are not consistent. They differ in names of column families");
        }
      }
    }

    // number of cfs and names of cfs are consistent across paths. Now check for column names in
    // each cf across paths
    for (String cfName : cfNames) {
      Set<String> columnNamesInCf = null;
      for (List<ColumnFamily> columnFamilies : columnFamiliesPerPath.values()) {
        Optional<ColumnFamily> cf =
            columnFamilies.stream().filter(c -> c.getName().equals(cfName)).findFirst();
        List<String> colNamesInCf =
            cf.get().getColumns().stream().map(Kolumn::getName).collect(Collectors.toList());

        if (columnNamesInCf == null) {
          columnNamesInCf = new HashSet<>(colNamesInCf);
        } else {
          if (!columnNamesInCf.equals(new HashSet<>(colNamesInCf))) {
            throw new IllegalStateException(
                "Column families across paths are not consistent. They differ in column names of column families");
          }
        }
      }
    }
    // todo: check for column types also

  }

  static Dataset<Row> loadFromCfs(
      SparkSession spark, List<ColumnFamily> inputCfs, List<String> toLoad) {

    Dataset<Row> toReturn = null;
    for (ColumnFamily inputCf : inputCfs) {
      if (!toLoad.contains(inputCf.getName())) {
        continue;
      }
      Dataset<Row> columnFamilyDf =
          spark.read().format(inputCf.getFormat()).load(inputCf.getPath());
      logger.info("Loaded column family: {} from path: {}", inputCf.getName(), inputCf.getPath());

      if (toReturn == null) {
        toReturn = columnFamilyDf;
      } else {
        logger.warn(
            "Joining column family: {} from path: {}", inputCf.getName(), inputCf.getPath());
        toReturn = toReturn.join(columnFamilyDf, Constants.ROW_ID);
      }
    }
    Utils.debugDataFrame(toReturn, "joined_for_read");
    return toReturn.drop(Constants.ROW_ID);
  }
}
