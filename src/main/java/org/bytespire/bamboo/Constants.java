package org.bytespire.bamboo;

public class Constants {

  public static final String BAMBOO_PREFIX = "__bamboo__";
  public static final String COLUMN_FAMILY_NAME = "column_family_name";
  public static final String COLUMNS = "columns";
  public static final String TOTAL_COLUMNS = "total_columns";
  public static final String PATH = "path";
  public static final String PARENT_PATH = "parent_path";
  public static final String PARENT_HASH = "parent_id";
  public static final String FORMAT = "format";
  public static final String ROW_ID = BAMBOO_PREFIX + "rid";

  // this column family has all the columns of the source dataset.
  public static final String ZION_CF = BAMBOO_PREFIX + "zion_cf";
}
