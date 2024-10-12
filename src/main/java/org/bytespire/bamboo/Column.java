package org.bytespire.bamboo;

import java.util.Objects;
import org.apache.spark.sql.types.DataTypes;

public class Column {

  public final String name;
  public final DataTypes type;

  public Column(String name, DataTypes type) {
    this.name = name;
    this.type = type;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    Column column = (Column) o;

    if (!Objects.equals(name, column.name)) return false;
    return Objects.equals(type, column.type);
  }

  @Override
  public int hashCode() {
    int result = name != null ? name.hashCode() : 0;
    result = 31 * result + (type != null ? type.hashCode() : 0);
    return result;
  }
}
