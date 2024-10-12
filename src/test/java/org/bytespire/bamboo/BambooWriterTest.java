package org.bytespire.bamboo;

import static org.bytespire.bamboo.Bamboo.METADATA_FORMAT;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class BambooWriterTest {
  @Before
  public void setUp() throws IOException {
    FileUtils.deleteDirectory(new File("/tmp/bamboo"));
    FileUtils.deleteDirectory(new File("/tmp/bamboo_farm"));
    FileUtils.forceMkdir(new File("/tmp/bamboo_farm"));
    FileUtils.forceMkdir(new File("/tmp/bamboo"));
    System.setProperty("DEBUG_DATAFRAMES", "yes");
  }

  @After
  public void tearDown() throws IOException {
    System.setProperty("DEBUG_DATAFRAMES", "no");
  }

  @Test
  public void testOverwriteModeWithNoZion() {
    String outputPath = "/tmp/bamboo/input1";
    Bamboo.setStorePath("/tmp/bamboo_farm");
    Bamboo.setWritingCacheStrategy(StorageLevel.MEMORY_ONLY());
    try (SparkSession spark = SparkSession.builder().master("local").getOrCreate()) {
      Dataset<Row> inputDataset =
          sampleDataset(SparkSession.builder().master("local").getOrCreate());
      inputDataset.show(false);

      Bamboo.write(spark, inputDataset)
          .mode(SaveMode.Overwrite)
          .format("parquet")
          .writeZionCf(false)
          .columnFamily(new ColumnFamily("cf1", Arrays.asList("colA", "colB")))
          .columnFamily(new ColumnFamily("cf2", Arrays.asList("colC")))
          .save(outputPath);

      // verify metadata written
      Dataset<Row> metadata = spark.read().format(METADATA_FORMAT).load(outputPath);
      metadata.show(false);
      Assert.assertEquals(2, metadata.count());

      String parentId = Utils.getPathId(outputPath);
      Assert.assertEquals(1l, metadata.select("parent_id").distinct().count());
      Assert.assertEquals(parentId, metadata.select("parent_id").distinct().first().get(0));

      Assert.assertEquals(
          1,
          metadata
              .where(
                  "column_family_name = 'cf1' and format = 'parquet' and parent_path ='/tmp/bamboo/input1' and path = '/tmp/bamboo_farm/"
                      + parentId
                      + "/cf1'")
              .count());
      Assert.assertEquals(
          1,
          metadata
              .where(
                  "column_family_name = 'cf2' and format = 'parquet' and parent_path ='/tmp/bamboo/input1' and path = '/tmp/bamboo_farm/"
                      + parentId
                      + "/cf2'")
              .count());
      Assert.assertEquals(0, metadata.where("column_family_name = '__bamboo__zion_cf'").count());
      Assert.assertEquals(
          "[cola, colb, __bamboo__rid]",
          scala.collection.JavaConverters.seqAsJavaList(
                  metadata.select("columns").where("column_family_name = 'cf1'").first().getSeq(0))
              .toString());
      Assert.assertEquals(
          "[colc, __bamboo__rid]",
          scala.collection.JavaConverters.seqAsJavaList(
                  metadata.select("columns").where("column_family_name = 'cf2'").first().getSeq(0))
              .toString());

      // verify data written
      Dataset<Row> cf1 = spark.read().parquet("/tmp/bamboo_farm/" + parentId + "/cf1");
      cf1.show(false);
      Assert.assertEquals(2, cf1.count());
      Assert.assertEquals(3, cf1.columns().length);
      Assert.assertTrue(new HashSet<>(Arrays.asList(cf1.columns())).contains("__bamboo__rid"));
      Assert.assertFalse(new HashSet<>(Arrays.asList(cf1.columns())).contains("colc"));
      Assert.assertEquals(1, cf1.where("cola = 'a0' and colb = 'b0'").count());
      Assert.assertEquals(1, cf1.where("cola = 'a1' and colb = 'b1'").count());

      Dataset<Row> cf2 = spark.read().parquet("/tmp/bamboo_farm/" + parentId + "/cf2");
      cf2.show(false);
      Assert.assertEquals(2, cf2.count());
      Assert.assertEquals(2, cf2.columns().length);
      Assert.assertTrue(new HashSet<>(Arrays.asList(cf2.columns())).contains("__bamboo__rid"));
      Assert.assertFalse(new HashSet<>(Arrays.asList(cf2.columns())).contains("cola"));
      Assert.assertFalse(new HashSet<>(Arrays.asList(cf2.columns())).contains("colb"));
      Assert.assertEquals(1, cf2.where("colc = 'c0'").count());
      Assert.assertEquals(1, cf2.where("colc = 'c1'").count());

      // test functionality if RowID column
      Dataset<Row> joined =
          cf1.join(cf2, cf1.col("__bamboo__rid").equalTo(cf2.col("__bamboo__rid")), "inner")
              .drop("__bamboo__rid");
      joined.show(false);
      Assert.assertEquals(2, joined.count());
      Assert.assertEquals(3, joined.columns().length);
      Assert.assertFalse(new HashSet<>(Arrays.asList(joined.columns())).contains("__bamboo__rid"));
      Assert.assertEquals(1, joined.where("cola = 'a0' and colb = 'b0' and colc = 'c0'").count());
      Assert.assertEquals(1, joined.where("cola = 'a1' and colb = 'b1' and colc = 'c1'").count());
    }
  }

  @Test
  public void testOverwriteModeWithZion() {
    String outputPath = "/tmp/bamboo/input1";
    Bamboo.setStorePath("/tmp/bamboo_farm");
    Bamboo.setWritingCacheStrategy(StorageLevel.MEMORY_ONLY());
    try (SparkSession spark = SparkSession.builder().master("local").getOrCreate()) {
      Dataset<Row> inputDataset =
          sampleDataset(SparkSession.builder().master("local").getOrCreate());
      inputDataset.show(false);

      Bamboo.write(spark, inputDataset)
          .mode(SaveMode.Overwrite)
          .format("parquet")
          .writeZionCf(true)
          .columnFamily(new ColumnFamily("cf1", Arrays.asList("colA", "colB")))
          .columnFamily(new ColumnFamily("cf2", Arrays.asList("colC")))
          .save(outputPath);

      // verify metadata written
      Dataset<Row> metadata = spark.read().format(METADATA_FORMAT).load(outputPath);
      metadata.show(false);
      Assert.assertEquals(3, metadata.count());

      String parentId = Utils.getPathId(outputPath);
      Assert.assertEquals(1l, metadata.select("parent_id").distinct().count());
      Assert.assertEquals(parentId, metadata.select("parent_id").distinct().first().get(0));

      Assert.assertEquals(
          1,
          metadata
              .where(
                  "column_family_name = 'cf1' and format = 'parquet' and parent_path ='/tmp/bamboo/input1' and path = '/tmp/bamboo_farm/"
                      + parentId
                      + "/cf1'")
              .count());
      Assert.assertEquals(
          1,
          metadata
              .where(
                  "column_family_name = 'cf2' and format = 'parquet' and parent_path ='/tmp/bamboo/input1' and path = '/tmp/bamboo_farm/"
                      + parentId
                      + "/cf2'")
              .count());
      Assert.assertEquals(1, metadata.where("column_family_name = '__bamboo__zion_cf'").count());
      Assert.assertEquals(
          "[cola, colb, colc]",
          scala.collection.JavaConverters.seqAsJavaList(
                  metadata
                      .select("columns")
                      .where("column_family_name = '__bamboo__zion_cf'")
                      .first()
                      .getSeq(0))
              .toString());
      Assert.assertEquals(
          "[cola, colb, __bamboo__rid]",
          scala.collection.JavaConverters.seqAsJavaList(
                  metadata.select("columns").where("column_family_name = 'cf1'").first().getSeq(0))
              .toString());
      Assert.assertEquals(
          "[colc, __bamboo__rid]",
          scala.collection.JavaConverters.seqAsJavaList(
                  metadata.select("columns").where("column_family_name = 'cf2'").first().getSeq(0))
              .toString());

      // verify data written
      Dataset<Row> cf1 = spark.read().parquet("/tmp/bamboo_farm/" + parentId + "/cf1");
      cf1.show(false);
      Assert.assertEquals(2, cf1.count());
      Assert.assertEquals(3, cf1.columns().length);
      Assert.assertTrue(new HashSet<>(Arrays.asList(cf1.columns())).contains("__bamboo__rid"));
      Assert.assertFalse(new HashSet<>(Arrays.asList(cf1.columns())).contains("colc"));
      Assert.assertEquals(1, cf1.where("cola = 'a0' and colb = 'b0'").count());
      Assert.assertEquals(1, cf1.where("cola = 'a1' and colb = 'b1'").count());

      Dataset<Row> cf2 = spark.read().parquet("/tmp/bamboo_farm/" + parentId + "/cf2");
      cf2.show(false);
      Assert.assertEquals(2, cf2.count());
      Assert.assertEquals(2, cf2.columns().length);
      Assert.assertTrue(new HashSet<>(Arrays.asList(cf2.columns())).contains("__bamboo__rid"));
      Assert.assertFalse(new HashSet<>(Arrays.asList(cf2.columns())).contains("cola"));
      Assert.assertFalse(new HashSet<>(Arrays.asList(cf2.columns())).contains("colb"));
      Assert.assertEquals(1, cf2.where("colc = 'c0'").count());
      Assert.assertEquals(1, cf2.where("colc = 'c1'").count());

      // verify zion
      Dataset<Row> zion =
          spark.read().parquet("/tmp/bamboo_farm/" + parentId + "/__bamboo__zion_cf");
      zion.show(false);
      Assert.assertEquals(2, zion.count());
      Assert.assertEquals(3, zion.columns().length);
      Assert.assertFalse(new HashSet<>(Arrays.asList(zion.columns())).contains("__bamboo__rid"));
      Assert.assertEquals(1, zion.where("cola = 'a0' and colb = 'b0' and colc = 'c0'").count());
      Assert.assertEquals(1, zion.where("cola = 'a1' and colb = 'b1' and colc = 'c1'").count());
    }
  }

  @Test
  public void testOverwriteModeNoCfSpecified() {
    String outputPath = "/tmp/bamboo/input1";
    Bamboo.setStorePath("/tmp/bamboo_farm");
    Bamboo.setWritingCacheStrategy(StorageLevel.MEMORY_ONLY());
    try (SparkSession spark = SparkSession.builder().master("local").getOrCreate()) {
      Dataset<Row> inputDataset =
          sampleDataset(SparkSession.builder().master("local").getOrCreate());
      inputDataset.show(false);

      for (int i = 0; i < 2; i++) {
        boolean writeZion = i == 0;
        Bamboo.write(spark, inputDataset)
            .mode(SaveMode.Overwrite)
            .format("parquet")
            .writeZionCf(writeZion)
            .save(outputPath);

        // verify metadata written
        Dataset<Row> metadata = spark.read().format(METADATA_FORMAT).load(outputPath);
        metadata.show(false);
        Assert.assertEquals(1, metadata.count());

        String parentId = Utils.getPathId(outputPath);
        Assert.assertEquals(1l, metadata.select("parent_id").distinct().count());
        Assert.assertEquals(parentId, metadata.select("parent_id").distinct().first().get(0));

        Assert.assertEquals(1, metadata.where("column_family_name = '__bamboo__zion_cf'").count());
        Assert.assertEquals(
            "[cola, colb, colc]",
            scala.collection.JavaConverters.seqAsJavaList(
                    metadata
                        .select("columns")
                        .where("column_family_name = '__bamboo__zion_cf'")
                        .first()
                        .getSeq(0))
                .toString());

        // verify zion
        Dataset<Row> zion =
            spark.read().parquet("/tmp/bamboo_farm/" + parentId + "/__bamboo__zion_cf");
        zion.show(false);
        Assert.assertEquals(2, zion.count());
        Assert.assertEquals(3, zion.columns().length);
        Assert.assertFalse(new HashSet<>(Arrays.asList(zion.columns())).contains("__bamboo__rid"));
        Assert.assertEquals(1, zion.where("cola = 'a0' and colb = 'b0' and colc = 'c0'").count());
        Assert.assertEquals(1, zion.where("cola = 'a1' and colb = 'b1' and colc = 'c1'").count());
      }
    }
  }

  @Test
  public void testAppendModeWithZion() {
    String outputPath = "/tmp/bamboo/input1";
    Bamboo.setStorePath("/tmp/bamboo_farm");
    Bamboo.setWritingCacheStrategy(StorageLevel.MEMORY_ONLY());
    try (SparkSession spark = SparkSession.builder().master("local").getOrCreate()) {
      Dataset<Row> inputDataset =
          sampleDataset(SparkSession.builder().master("local").getOrCreate());
      inputDataset.show(false);

      // first time write
      Bamboo.write(spark, inputDataset)
          .mode(SaveMode.Overwrite)
          .format("parquet")
          .columnFamily(new ColumnFamily("cf1", Arrays.asList("colA", "colB")))
          .save(outputPath);

      // try append with column families
      for (int i = 0; i < 2; i++) {
        final boolean writeZion = i == 0;
        try {
          Bamboo.write(spark, inputDataset)
              .mode(SaveMode.Append)
              .format("parquet")
              .writeZionCf(writeZion)
              .columnFamily(new ColumnFamily("cf1", Arrays.asList("colA", "colB")))
              .save(outputPath);
          Assert.fail(
              "append to existing dataset with column families Should have thrown IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
        }
      }

      for (int i = 1; i < 3; i++) {
        final boolean writeZion = i == 1;
        Bamboo.write(spark, inputDataset)
            .mode(SaveMode.Append)
            .format("parquet")
            .writeZionCf(writeZion)
            .save(outputPath);

        // verify metadata written
        Dataset<Row> metadata = spark.read().format(METADATA_FORMAT).load(outputPath);
        metadata.show(false);
        Assert.assertEquals(1, metadata.count());

        String parentId = Utils.getPathId(outputPath);
        Assert.assertEquals(1l, metadata.select("parent_id").distinct().count());
        Assert.assertEquals(parentId, metadata.select("parent_id").distinct().first().get(0));

        Assert.assertEquals(
            1,
            metadata
                .where(
                    "column_family_name = 'cf1' and format = 'parquet' and parent_path ='/tmp/bamboo/input1' and path = '/tmp/bamboo_farm/"
                        + parentId
                        + "/cf1'")
                .count());
        Assert.assertEquals(
            0,
            metadata
                .where(
                    "column_family_name = 'cf2' and format = 'parquet' and parent_path ='/tmp/bamboo/input1' and path = '/tmp/bamboo_farm/"
                        + parentId
                        + "/cf2'")
                .count());
        Assert.assertEquals(0, metadata.where("column_family_name = '__bamboo__zion_cf'").count());
        Assert.assertEquals(
            "[cola, colb, __bamboo__rid]",
            scala.collection.JavaConverters.seqAsJavaList(
                    metadata
                        .select("columns")
                        .where("column_family_name = 'cf1'")
                        .first()
                        .getSeq(0))
                .toString());

        // verify data written
        Dataset<Row> cf1 = spark.read().parquet("/tmp/bamboo_farm/" + parentId + "/cf1");
        cf1.show(false);
        Assert.assertEquals(2 + (2 * i), cf1.count());
        Assert.assertEquals(3, cf1.columns().length);
        Assert.assertTrue(new HashSet<>(Arrays.asList(cf1.columns())).contains("__bamboo__rid"));
        Assert.assertFalse(new HashSet<>(Arrays.asList(cf1.columns())).contains("colc"));
        Assert.assertEquals(1 + (i), cf1.where("cola = 'a0' and colb = 'b0'").count());
        Assert.assertEquals(1 + (i), cf1.where("cola = 'a1' and colb = 'b1'").count());
      }
    }
  }

  @Test
  public void testAppendModeWithExistingZion() {
    String outputPath = "/tmp/bamboo/input1";
    Bamboo.setStorePath("/tmp/bamboo_farm");
    Bamboo.setWritingCacheStrategy(StorageLevel.MEMORY_ONLY());
    try (SparkSession spark = SparkSession.builder().master("local").getOrCreate()) {
      Dataset<Row> inputDataset =
          sampleDataset(SparkSession.builder().master("local").getOrCreate());
      inputDataset.show(false);

      // first time write
      Bamboo.write(spark, inputDataset)
          .mode(SaveMode.Overwrite)
          .writeZionCf(true)
          .format("parquet")
          .columnFamily(new ColumnFamily("cf1", Arrays.asList("colA", "colB")))
          .save(outputPath);

      // try append with column families
      for (int i = 0; i < 2; i++) {
        final boolean writeZion = i == 0;
        try {
          Bamboo.write(spark, inputDataset)
              .mode(SaveMode.Append)
              .format("parquet")
              .writeZionCf(writeZion)
              .columnFamily(new ColumnFamily("cf1", Arrays.asList("colA", "colB")))
              .save(outputPath);
          Assert.fail(
              "append to existing dataset with column families Should have thrown IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
        }
      }

      for (int i = 1; i < 3; i++) {
        final boolean writeZion = i == 1;
        Bamboo.write(spark, inputDataset)
            .mode(SaveMode.Append)
            .format("parquet")
            .writeZionCf(writeZion)
            .save(outputPath);

        // verify metadata written
        Dataset<Row> metadata = spark.read().format(METADATA_FORMAT).load(outputPath);
        metadata.show(false);
        Assert.assertEquals(2, metadata.count());

        String parentId = Utils.getPathId(outputPath);
        Assert.assertEquals(1l, metadata.select("parent_id").distinct().count());
        Assert.assertEquals(parentId, metadata.select("parent_id").distinct().first().get(0));

        Assert.assertEquals(
            1,
            metadata
                .where(
                    "column_family_name = 'cf1' and format = 'parquet' and parent_path ='/tmp/bamboo/input1' and path = '/tmp/bamboo_farm/"
                        + parentId
                        + "/cf1'")
                .count());
        Assert.assertEquals(
            0,
            metadata
                .where(
                    "column_family_name = 'cf2' and format = 'parquet' and parent_path ='/tmp/bamboo/input1' and path = '/tmp/bamboo_farm/"
                        + parentId
                        + "/cf2'")
                .count());
        Assert.assertEquals(1, metadata.where("column_family_name = '__bamboo__zion_cf'").count());
        Assert.assertEquals(
            "[cola, colb, __bamboo__rid]",
            scala.collection.JavaConverters.seqAsJavaList(
                    metadata
                        .select("columns")
                        .where("column_family_name = 'cf1'")
                        .first()
                        .getSeq(0))
                .toString());
        Assert.assertEquals(
            "[cola, colb, colc]",
            scala.collection.JavaConverters.seqAsJavaList(
                    metadata
                        .select("columns")
                        .where("column_family_name = '__bamboo__zion_cf'")
                        .first()
                        .getSeq(0))
                .toString());

        // verify data written
        Dataset<Row> cf1 = spark.read().parquet("/tmp/bamboo_farm/" + parentId + "/cf1");
        cf1.show(false);
        Assert.assertEquals(2 + (2 * i), cf1.count());
        Assert.assertEquals(3, cf1.columns().length);
        Assert.assertTrue(new HashSet<>(Arrays.asList(cf1.columns())).contains("__bamboo__rid"));
        Assert.assertFalse(new HashSet<>(Arrays.asList(cf1.columns())).contains("colc"));
        Assert.assertEquals(1 + (i), cf1.where("cola = 'a0' and colb = 'b0'").count());
        Assert.assertEquals(1 + (i), cf1.where("cola = 'a1' and colb = 'b1'").count());

        // verify zion
        Dataset<Row> zion =
            spark.read().parquet("/tmp/bamboo_farm/" + parentId + "/__bamboo__zion_cf");
        zion.show(false);
        Assert.assertEquals(2 + (2 * i), zion.count());
        Assert.assertEquals(3, zion.columns().length);
        Assert.assertFalse(new HashSet<>(Arrays.asList(zion.columns())).contains("__bamboo__rid"));
        Assert.assertEquals(
            1 + i, zion.where("cola = 'a0' and colb = 'b0' and colc = 'c0'").count());
        Assert.assertEquals(
            1 + i, zion.where("cola = 'a1' and colb = 'b1' and colc = 'c1'").count());
      }
    }
  }

  @Test
  public void testErrorIfExistsAndIgnore() {
    String outputPath = "/tmp/bamboo/input1";
    Bamboo.setStorePath("/tmp/bamboo_farm");
    Bamboo.setWritingCacheStrategy(StorageLevel.MEMORY_ONLY());
    try (SparkSession spark = SparkSession.builder().master("local").getOrCreate()) {
      Dataset<Row> inputDataset =
          sampleDataset(SparkSession.builder().master("local").getOrCreate());
      inputDataset.show(false);

      // first time write
      Bamboo.write(spark, inputDataset)
          .mode(SaveMode.Overwrite)
          .writeZionCf(true)
          .format("parquet")
          .save(outputPath);

      File file1 =
          new File(
              "/tmp/bamboo_farm/" + Utils.getPathId(outputPath) + "/__bamboo__zion_cf/_SUCCESS");
      final long lastModified = file1.lastModified();

      File metadataSuccess1 = new File("/tmp/bamboo/input1/_SUCCESS");
      final long lastModifiedMd = metadataSuccess1.lastModified();

      Bamboo.write(spark, inputDataset)
          .mode(SaveMode.Ignore)
          .format("parquet")
          .writeZionCf(true)
          .columnFamily(new ColumnFamily("cf1", Arrays.asList("colA", "colB")))
          .save(outputPath);

      File file2 =
          new File(
              "/tmp/bamboo_farm/" + Utils.getPathId(outputPath) + "/__bamboo__zion_cf/_SUCCESS");
      Assert.assertEquals(lastModified, file2.lastModified());

      File metadataSuccess2 = new File("/tmp/bamboo/input1/_SUCCESS");
      Assert.assertEquals(lastModifiedMd, metadataSuccess2.lastModified());

      try {
        Bamboo.write(spark, inputDataset)
            .mode(SaveMode.ErrorIfExists)
            .format("parquet")
            .writeZionCf(true)
            .columnFamily(new ColumnFamily("cf1", Arrays.asList("colA", "colB")))
            .save(outputPath);
        Assert.fail("Should have thrown IllegalStateException");
      } catch (IllegalStateException expected) {
        expected.printStackTrace();
      }
    }
  }

  @Test
  public void testBlankPath() {
    Bamboo.setStorePath("/tmp/bamboo_farm");
    Bamboo.setWritingCacheStrategy(StorageLevel.MEMORY_ONLY());
    try (SparkSession spark = SparkSession.builder().master("local").getOrCreate()) {
      Dataset<Row> inputDataset =
          sampleDataset(SparkSession.builder().master("local").getOrCreate());
      inputDataset.show(false);

      for (String path : Arrays.asList(null, "", "  ")) {
        try {
          Bamboo.write(spark, inputDataset)
              .mode(SaveMode.Overwrite)
              .format("parquet")
              .writeZionCf(true)
              .columnFamily(new ColumnFamily("cf1", Arrays.asList("colA", "colB")))
              .save(path);
          Assert.fail("Should have thrown IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
          expected.printStackTrace();
        }
      }
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testWriteEmptyCf() {
    String outputPath = "/tmp/bamboo/input1";
    Bamboo.setStorePath("/tmp/bamboo_farm");
    Bamboo.setWritingCacheStrategy(StorageLevel.MEMORY_ONLY());
    try (SparkSession spark = SparkSession.builder().master("local").getOrCreate()) {
      Dataset<Row> inputDataset =
          sampleDataset(SparkSession.builder().master("local").getOrCreate());
      inputDataset.show(false);

      Bamboo.write(spark, inputDataset)
          .mode(SaveMode.Overwrite)
          .format("parquet")
          .columnFamily(new ColumnFamily("cf2", new ArrayList<>()))
          .save(outputPath);
    }
  }

  static Dataset<Row> sampleDataset(SparkSession spark) {
    List<Row> rows = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      rows.add(RowFactory.create("a" + i, "b" + i, "c" + i));
    }

    return spark.createDataFrame(rows, sampleSchema);
  }

  static StructType sampleSchema =
      new StructType()
          .add("colA", DataTypes.StringType, false)
          .add("colB", DataTypes.StringType, false)
          .add("colC", DataTypes.StringType, false);
}
