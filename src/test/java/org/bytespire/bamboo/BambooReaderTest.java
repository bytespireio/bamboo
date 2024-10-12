package org.bytespire.bamboo;

import static org.bytespire.bamboo.BambooWriterTest.*;

import java.io.File;
import java.io.IOException;
import java.util.*;
import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import org.bytespire.bamboo.stats.ReporterException;
import org.bytespire.bamboo.stats.StatsReporter;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class BambooReaderTest {

  static TestReporter myCollector = new TestReporter();

  @Before
  public void setUp() throws IOException {
    FileUtils.deleteDirectory(new File("/tmp/bamboo"));
    FileUtils.deleteDirectory(new File("/tmp/bamboo_farm"));
    FileUtils.forceMkdir(new File("/tmp/bamboo_farm/multi"));
    FileUtils.forceMkdir(new File("/tmp/bamboo_farm"));
    FileUtils.forceMkdir(new File("/tmp/bamboo"));
    System.setProperty("DEBUG_DATAFRAMES", "yes");
    Bamboo.setStatsReporter(myCollector);
  }

  @After
  public void tearDown() throws IOException {
    System.setProperty("DEBUG_DATAFRAMES", "no");
    myCollector.clear();
  }

  @Test
  public void testBlankPath() {
    Bamboo.setStorePath("/tmp/bamboo_farm");
    Bamboo.setWritingCacheStrategy(StorageLevel.MEMORY_ONLY());
    try (SparkSession spark = SparkSession.builder().master("local").getOrCreate()) {
      for (String path : Arrays.asList(null, "", "  ")) {
        try {
          Bamboo.read(spark).format("does_not_matter").columns("col1", "col2").load(path);
          Assert.fail("Should have thrown IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
        }
      }
    }
  }

  @Test()
  public void testReadColumnsFromEmptyLocation() {
    Bamboo.setStorePath("/tmp/bamboo_farm");
    Bamboo.setWritingCacheStrategy(StorageLevel.MEMORY_ONLY());
    try (SparkSession spark = SparkSession.builder().master("local").getOrCreate()) {
      try {
        Bamboo.read(spark)
            .format("does_not_matter")
            .columns("x", "colA")
            .load("/tmp/bamboo_farm/foobarxyz");
        Assert.fail("Should have thrown PATH_NOT_FOUND exception");
      } catch (Exception e) {
        if (!Utils.IsPathNotFound(e)) {
          throw e;
        }
      }
    }
  }

  @Test
  public void testReadColumnsWhichAreNotAvailable() {
    String outputPath = "/tmp/bamboo_farm/abc/";
    Bamboo.setStorePath("/tmp/bamboo_farm");
    Bamboo.setWritingCacheStrategy(StorageLevel.MEMORY_ONLY());
    try (SparkSession spark = SparkSession.builder().master("local").getOrCreate()) {
      Bamboo.write(spark, sampleDataset(spark))
          .mode(SaveMode.Overwrite)
          .format("parquet")
          .writeZionCf(false)
          .columnFamily(new ColumnFamily("cf1", Arrays.asList(colA, colB)))
          .columnFamily(new ColumnFamily("cf2", Arrays.asList(colC)))
          .save(outputPath);
      try {
        Bamboo.read(spark).format("does_not_matter").columns("x", "colA").load(outputPath);
      } catch (IllegalArgumentException expected) {
      }
    }
  }

  @Test
  public void testSelectStarWithZionPresent() {
    // ;
    String outputPath = "/tmp/bamboo_farm/star/";
    Bamboo.setStorePath("/tmp/bamboo_farm");
    Bamboo.setWritingCacheStrategy(StorageLevel.MEMORY_ONLY());
    try (SparkSession spark = SparkSession.builder().master("local").getOrCreate()) {
      Bamboo.write(spark, sampleDataset(spark))
          .mode(SaveMode.Overwrite)
          .format("parquet")
          .writeZionCf(true)
          .columnFamily(new ColumnFamily("cf1", Arrays.asList(colA, colB)))
          .columnFamily(new ColumnFamily("cf2", Arrays.asList(colC)))
          .save(outputPath);

      Dataset<Row> result = Bamboo.read(spark).format("does_not_matter").load(outputPath);

      Assert.assertEquals(2, result.count());
      Assert.assertEquals(3, result.columns().length);
      Assert.assertFalse(new HashSet<>(Arrays.asList(result.columns())).contains("__bamboo__rid"));
      Assert.assertEquals(1, result.where("cola = 'a0' and colb = 'b0' and colc = 'c0'").count());
      Assert.assertEquals(1, result.where("cola = 'a1' and colb = 'b1' and colc = 'c1'").count());
      Assert.assertEquals(1, myCollector.familiesUsed.size());
      Assert.assertEquals("__bamboo__zion_cf", myCollector.familiesUsed.get(0).getName());
    }
  }

  @Test
  public void testSelectStarWithZionAbsent() {
    // ;
    String outputPath = "/tmp/bamboo_farm/star/";
    Bamboo.setStorePath("/tmp/bamboo_farm");
    Bamboo.setWritingCacheStrategy(StorageLevel.MEMORY_ONLY());
    try (SparkSession spark = SparkSession.builder().master("local").getOrCreate()) {
      Bamboo.write(spark, sampleDataset(spark))
          .mode(SaveMode.Overwrite)
          .format("parquet")
          .writeZionCf(false)
          .columnFamily(new ColumnFamily("cf1", Arrays.asList(colA, colB)))
          .columnFamily(new ColumnFamily("cf2", Arrays.asList(colC)))
          .save(outputPath);

      Dataset<Row> result = Bamboo.read(spark).format("does_not_matter").load(outputPath);

      Assert.assertEquals(2, result.count());
      Assert.assertEquals(3, result.columns().length);
      Assert.assertFalse(new HashSet<>(Arrays.asList(result.columns())).contains("__bamboo__rid"));
      Assert.assertEquals(1, result.where("cola = 'a0' and colb = 'b0' and colc = 'c0'").count());
      Assert.assertEquals(1, result.where("cola = 'a1' and colb = 'b1' and colc = 'c1'").count());

      myCollector.familiesUsed.sort(Comparator.comparing(ColumnFamily::getName));
      myCollector.columnsRequested.sort(Comparator.comparing(Kolumn::getName));
      Assert.assertEquals(2, myCollector.familiesUsed.size());
      Assert.assertEquals("cf1", myCollector.familiesUsed.get(0).getName());
      Assert.assertEquals("cf2", myCollector.familiesUsed.get(1).getName());
      Assert.assertEquals(3, myCollector.columnsRequested.size());
      Assert.assertEquals("cola", myCollector.columnsRequested.get(0).getName());
      Assert.assertEquals("colb", myCollector.columnsRequested.get(1).getName());
      Assert.assertEquals("colc", myCollector.columnsRequested.get(2).getName());
    }
  }

  @Test
  public void testSelectSpecificWithZionPresent() {
    // ;
    String outputPath = "/tmp/bamboo_farm/star/";
    Bamboo.setStorePath("/tmp/bamboo_farm");
    Bamboo.setWritingCacheStrategy(StorageLevel.MEMORY_ONLY());
    try (SparkSession spark = SparkSession.builder().master("local").getOrCreate()) {
      Bamboo.write(spark, sampleDataset(spark))
          .mode(SaveMode.Overwrite)
          .format("parquet")
          .writeZionCf(true)
          .columnFamily(new ColumnFamily("cf1", Arrays.asList(colA, colB)))
          .columnFamily(new ColumnFamily("cf2", Arrays.asList(colC)))
          .save(outputPath);

      Dataset<Row> result =
          Bamboo.read(spark).format("does_not_matter").columns("colA").load(outputPath);

      Assert.assertEquals(2, result.count());
      Assert.assertEquals(2, result.columns().length);
      Assert.assertFalse(new HashSet<>(Arrays.asList(result.columns())).contains("__bamboo__rid"));
      Assert.assertEquals(1, result.where("cola = 'a0' and colb = 'b0' ").count());
      Assert.assertEquals(1, result.where("cola = 'a1' and colb = 'b1'").count());
      Assert.assertEquals(1, myCollector.familiesUsed.size());
      Assert.assertEquals("cf1", myCollector.familiesUsed.get(0).getName());
    }
  }

  @Test
  public void testForMinimumReadSelectSpecificWithZionPresent1() {

    List<Row> rows = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      rows.add(RowFactory.create("a" + i, "b" + i, "c" + i, "d" + i));
    }

    // ;
    String outputPath = "/tmp/bamboo_farm/star/";
    Bamboo.setStorePath("/tmp/bamboo_farm");
    Bamboo.setWritingCacheStrategy(StorageLevel.MEMORY_ONLY());
    try (SparkSession spark = SparkSession.builder().master("local").getOrCreate()) {
      Dataset<Row> sample =
          spark.createDataFrame(
              rows,
              DataTypes.createStructType(
                  new StructField[] {
                    DataTypes.createStructField("colA", DataTypes.StringType, false),
                    DataTypes.createStructField("colB", DataTypes.StringType, false),
                    DataTypes.createStructField("colC", DataTypes.StringType, false),
                    DataTypes.createStructField("colD", DataTypes.StringType, false)
                  }));

      Bamboo.write(spark, sample)
          .mode(SaveMode.Overwrite)
          .format("parquet")
          .writeZionCf(true)
          .columnFamily(new ColumnFamily("cab", Arrays.asList(colA, colB)))
          .columnFamily(new ColumnFamily("cabc", Arrays.asList(colA, colB, colC)))
          .columnFamily(new ColumnFamily("cd", Arrays.asList(colD)))
          .save(outputPath);

      Dataset<Row> result =
          Bamboo.read(spark).format("does_not_matter").columns("colB", "colA").load(outputPath);

      Assert.assertEquals(2, result.count());
      Assert.assertEquals(2, result.columns().length);
      Assert.assertFalse(new HashSet<>(Arrays.asList(result.columns())).contains("__bamboo__rid"));
      Assert.assertEquals(1, result.where("cola = 'a0' and colb = 'b0' ").count());
      Assert.assertEquals(1, result.where("cola = 'a1' and colb = 'b1'").count());
      Assert.assertEquals(1, myCollector.familiesUsed.size());
      Assert.assertEquals("cab", myCollector.familiesUsed.get(0).getName());
    }
  }

  @Test
  public void testForMinimumReadSelectSpecificWithZionPresent2() {

    List<Row> rows = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      rows.add(RowFactory.create("a" + i, "b" + i, "c" + i, "d" + i, i));
    }

    // ;
    String outputPath = "/tmp/bamboo_farm/star/";
    Bamboo.setStorePath("/tmp/bamboo_farm");
    Bamboo.setWritingCacheStrategy(StorageLevel.MEMORY_ONLY());
    try (SparkSession spark = SparkSession.builder().master("local").getOrCreate()) {
      Dataset<Row> sample =
          spark.createDataFrame(
              rows,
              DataTypes.createStructType(
                  new StructField[] {
                    DataTypes.createStructField("colA", DataTypes.StringType, false),
                    DataTypes.createStructField("colB", DataTypes.StringType, false),
                    DataTypes.createStructField("colC", DataTypes.StringType, false),
                    DataTypes.createStructField("colD", DataTypes.StringType, false),
                    DataTypes.createStructField("colE", DataTypes.IntegerType, false)
                  }));

      Bamboo.write(spark, sample)
          .mode(SaveMode.Overwrite)
          .format("parquet")
          .writeZionCf(true)
          //          .columnFamily(new ColumnFamily("cab", Arrays.asList("colA", "colB")))
          //          .columnFamily(new ColumnFamily("cabd", Arrays.asList("colA", "colB", "colD")))
          //          .columnFamily(new ColumnFamily("cabde", Arrays.asList("colA", "colB", "colD",
          // "colE")))
          //          .columnFamily(new ColumnFamily("cd", Arrays.asList("colD")))
          .columnFamily(new ColumnFamily("cab", Arrays.asList(colA, colB)))
          .columnFamily(new ColumnFamily("cabd", Arrays.asList(colA, colB, colD)))
          .columnFamily(new ColumnFamily("cabde", Arrays.asList(colA, colB, colD, colE)))
          .columnFamily(new ColumnFamily("cd", Arrays.asList(colD)))
          .save(outputPath);

      Dataset<Row> result =
          Bamboo.read(spark)
              .format("does_not_matter")
              .columns("colB", "colA", "colD")
              .load(outputPath);

      Assert.assertEquals(2, result.count());
      Assert.assertEquals(3, result.columns().length);
      Assert.assertFalse(new HashSet<>(Arrays.asList(result.columns())).contains("__bamboo__rid"));
      Assert.assertEquals(1, result.where("cola = 'a0' and colb = 'b0' and cold = 'd0' ").count());
      Assert.assertEquals(1, result.where("cola = 'a1' and colb = 'b1' and cold = 'd1' ").count());
      Assert.assertEquals(1, myCollector.familiesUsed.size());
      Assert.assertEquals("cabd", myCollector.familiesUsed.get(0).getName());
    }
  }

  @Test
  public void readNonExistentColumn() {
    String outputPath = "/tmp/bamboo_farm/abc/";
    Bamboo.setStorePath("/tmp/bamboo_farm");
    Bamboo.setWritingCacheStrategy(StorageLevel.MEMORY_ONLY());
    try (SparkSession spark = SparkSession.builder().master("local").getOrCreate()) {
      Bamboo.write(spark, sampleDataset(spark))
          .mode(SaveMode.Overwrite)
          .format("parquet")
          .writeZionCf(false)
          .columnFamily(new ColumnFamily("cf1", Arrays.asList(colA, colB)))
          .columnFamily(new ColumnFamily("cf2", Arrays.asList(colC)))
          .save(outputPath);

      try {
        Bamboo.read(spark).format("does_not_matter").columns("       ").load(outputPath);
        Assert.fail("Should have thrown IllegalArgumentException for empty column name");
      } catch (IllegalArgumentException expected) {
        expected.printStackTrace();
      }

      try {
        Bamboo.read(spark).format("does_not_matter").columns("colA", "colX").load(outputPath);
        Assert.fail("Should have thrown IllegalArgumentException");
      } catch (IllegalArgumentException expected) {
        expected.printStackTrace();
      }
    }
  }

  @Test
  public void testReadWithNoZion() {
    String outputPath = "/tmp/bamboo_farm/abc/";
    Bamboo.setStorePath("/tmp/bamboo_farm");
    Bamboo.setWritingCacheStrategy(StorageLevel.MEMORY_ONLY());
    try (SparkSession spark = SparkSession.builder().master("local").getOrCreate()) {
      Bamboo.write(spark, sampleDataset(spark))
          .mode(SaveMode.Overwrite)
          .format("parquet")
          .writeZionCf(false)
          .columnFamily(new ColumnFamily("cf1", Arrays.asList(colA, colB)))
          .save(outputPath);

      try {
        Bamboo.read(spark).format("does_not_matter").columns("colC").load(outputPath);
        Assert.fail("Should have thrown IllegalArgumentException as colC was not stored");
      } catch (IllegalArgumentException expected) {
        expected.printStackTrace();
      }
    }
  }

  @Test
  public void testReadMultiplePartitionsWithoutZion() {
    // ;
    String outputPath = "/tmp/bamboo_farm/multi";
    Bamboo.setStorePath("/tmp/bamboo_farm");
    Bamboo.setWritingCacheStrategy(StorageLevel.MEMORY_ONLY());

    List<Row> rows1 = new ArrayList<>();
    rows1.add(RowFactory.create("a0", "b0", "c0"));

    List<Row> rows2 = new ArrayList<>();
    rows2.add(RowFactory.create("a1", "b1", "c1"));

    try (SparkSession spark = SparkSession.builder().master("local").getOrCreate()) {
      Bamboo.write(spark, spark.createDataFrame(rows1, sampleSchema))
          .mode(SaveMode.Overwrite)
          .format("parquet")
          .columnFamily(new ColumnFamily("cf1", Arrays.asList(colA, colB)))
          .columnFamily(new ColumnFamily("cf2", Arrays.asList(colC)))
          .save(outputPath + "/part1");

      Bamboo.write(spark, spark.createDataFrame(rows2, sampleSchema))
          .mode(SaveMode.Overwrite)
          .format("parquet")
          .columnFamily(new ColumnFamily("cf1", Arrays.asList(colA, colB)))
          .columnFamily(new ColumnFamily("cf2", Arrays.asList(colC)))
          .save(outputPath + "/part2");

      Dataset<Row> result =
          Bamboo.read(spark)
              .format("does_not_matter")
              .columns("colA", "colB", "colC")
              .load(outputPath + "/*/");
      result.show(false);

      Assert.assertEquals(2, result.count());
      Assert.assertEquals(3, result.columns().length);
      Assert.assertFalse(new HashSet<>(Arrays.asList(result.columns())).contains("__bamboo__rid"));
      Assert.assertEquals(1, result.where("cola = 'a0' and colb = 'b0' and colc = 'c0' ").count());
      Assert.assertEquals(1, result.where("cola = 'a1' and colb = 'b1' and colc = 'c1' ").count());

      myCollector.familiesUsed.sort(Comparator.comparing(ColumnFamily::getName));
      Assert.assertEquals(2, myCollector.familiesUsed.size());
      Assert.assertEquals("cf1", myCollector.familiesUsed.get(0).getName());
      Assert.assertEquals("cf2", myCollector.familiesUsed.get(1).getName());
    }
  }

  @Test
  public void testReadMultiplePartitionsDifferentSchemas() {
    // ;
    String outputPath = "/tmp/bamboo_farm/multi";
    Bamboo.setStorePath("/tmp/bamboo_farm");
    Bamboo.setWritingCacheStrategy(StorageLevel.MEMORY_ONLY());

    List<Row> rows1 = new ArrayList<>();
    rows1.add(RowFactory.create("a0"));

    List<Row> rows2 = new ArrayList<>();
    rows2.add(RowFactory.create("b0"));

    try (SparkSession spark = SparkSession.builder().master("local").getOrCreate()) {
      Bamboo.write(
              spark,
              spark.createDataFrame(
                  rows1, new StructType().add("colA", DataTypes.StringType, false)))
          .mode(SaveMode.Overwrite)
          .format("parquet")
          .columnFamily(new ColumnFamily("cf1", Arrays.asList(colA)))
          .save(outputPath + "/part1");

      Bamboo.write(
              spark,
              spark.createDataFrame(
                  rows2, new StructType().add("colB", DataTypes.StringType, false)))
          .mode(SaveMode.Overwrite)
          .format("parquet")
          .columnFamily(new ColumnFamily("cf1", Arrays.asList(colB)))
          .save(outputPath + "/part2");

      try {
        Bamboo.read(spark).format("does_not_matter").load(outputPath + "/*/");
        Assert.fail("Should have thrown IllegalStateException");
      } catch (IllegalStateException e) {
        Assert.assertTrue(
            e.getMessage().contains("They differ in column names of column families"));
      }
    }
  }

  @Test
  public void testReadMultiplePartitionsDifferentNumberColumnFams() {
    // ;
    String outputPath = "/tmp/bamboo_farm/multi";
    Bamboo.setStorePath("/tmp/bamboo_farm");
    Bamboo.setWritingCacheStrategy(StorageLevel.MEMORY_ONLY());

    List<Row> rows2 = new ArrayList<>();
    rows2.add(RowFactory.create("b0"));

    try (SparkSession spark = SparkSession.builder().master("local").getOrCreate()) {
      Bamboo.write(spark, sampleDataset(spark))
          .mode(SaveMode.Overwrite)
          .format("parquet")
          .columnFamily(new ColumnFamily("cf1", Arrays.asList(colA)))
          .columnFamily(new ColumnFamily("cf2", Arrays.asList(colB)))
          .save(outputPath + "/part1");

      Bamboo.write(
              spark,
              spark.createDataFrame(
                  rows2, new StructType().add("colB", DataTypes.StringType, false)))
          .mode(SaveMode.Overwrite)
          .format("parquet")
          .columnFamily(new ColumnFamily("cf1", Arrays.asList(colB)))
          .save(outputPath + "/part2");

      try {
        Bamboo.read(spark).format("does_not_matter").load(outputPath + "/*/");
        Assert.fail("Should have thrown IllegalStateException");
      } catch (IllegalStateException e) {
        Assert.assertTrue(e.getMessage().contains("They differ in number of column families"));
      }
    }
  }

  @Test
  public void testReadMultiplePartitionsDifferentNamesColumnFams() {
    String outputPath = "/tmp/bamboo_farm/multi";
    Bamboo.setStorePath("/tmp/bamboo_farm");
    Bamboo.setWritingCacheStrategy(StorageLevel.MEMORY_ONLY());

    List<Row> rows1 = new ArrayList<>();
    rows1.add(RowFactory.create("a0"));

    List<Row> rows2 = new ArrayList<>();
    rows2.add(RowFactory.create("b0"));

    try (SparkSession spark = SparkSession.builder().master("local").getOrCreate()) {
      Bamboo.write(
              spark,
              spark.createDataFrame(
                  rows1, new StructType().add("colA", DataTypes.StringType, false)))
          .mode(SaveMode.Overwrite)
          .format("parquet")
          .columnFamily(new ColumnFamily("cf1", Arrays.asList(colA)))
          .save(outputPath + "/part1");

      Bamboo.write(
              spark,
              spark.createDataFrame(
                  rows2, new StructType().add("colB", DataTypes.StringType, false)))
          .mode(SaveMode.Overwrite)
          .format("parquet")
          .columnFamily(new ColumnFamily("cf2", Arrays.asList(colB)))
          .save(outputPath + "/part2");

      try {
        Bamboo.read(spark).format("does_not_matter").load(outputPath + "/*/");
        Assert.fail("Should have thrown IllegalStateException");
      } catch (IllegalStateException e) {
        Assert.assertTrue(e.getMessage().contains("They differ in names of column families"));
      }
    }
  }

  private static class TestReporter implements StatsReporter {

    private List<ColumnFamily> familiesUsed = new ArrayList<>();
    private List<Kolumn> columnsRequested = new ArrayList<>();

    private void clear() {
      familiesUsed.clear();
      columnsRequested.clear();
    }

    @Override
    public void report(
        String datasetPath, List<Kolumn> columnsRequested, List<ColumnFamily> columnFamiliesRead)
        throws ReporterException {
      familiesUsed.addAll(columnFamiliesRead);
      this.columnsRequested.addAll(columnsRequested);
    }
  }
}
