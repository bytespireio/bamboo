package org.bytespire.bamboo;

import java.util.List;
import org.apache.commons.lang3.StringUtils;

/** ColumnFamily represents a collection of columns in a table. */
public class ColumnFamily {
  private final String name;
  private final List<String> columnNames;
  private String path;

  private String format = "parquet";
  private String parentPath;

  public ColumnFamily(String name, List<String> columns) {
    this(name, columns, "parquet");
  }

  public ColumnFamily(String name, List<String> columns, String format) {
    if (StringUtils.isBlank(name)) {
      throw new IllegalArgumentException("Column family name cannot be null or empty");
    }
    if (columns == null || columns.isEmpty()) {
      throw new IllegalArgumentException("list of Columns in family cannot be null or empty");
    }
    this.name = name;
    this.columnNames = (columns);
    this.format = format;
  }

  public String getName() {
    return name;
  }

  public List<String> getColumnNames() {
    return columnNames;
  }

  public String getPath() {
    return path;
  }

  public String getFormat() {
    return format;
  }

  public void setPath(String path) {
    this.path = path;
  }

  public void setFormat(String format) {
    this.format = format;
  }

  public String getParentPath() {
    return parentPath;
  }

  public void setParentPath(String parentPath) {
    this.parentPath = parentPath;
  }

  @Override
  public String toString() {
    return "ColumnFamily{"
        + "name='"
        + name
        + '\''
        + ", columnNames="
        + columnNames
        + ", path='"
        + path
        + '\''
        + ", format='"
        + format
        + '\''
        + ", parentPath='"
        + parentPath
        + '\''
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    ColumnFamily that = (ColumnFamily) o;

    if (!name.equals(that.name)) return false;
    return parentPath.equals(that.parentPath);
  }

  @Override
  public int hashCode() {
    int result = name.hashCode();
    result = 31 * result + parentPath.hashCode();
    return result;
  }
}
