package org.bytespire.bamboo;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

public class Kolumn {

  public final String name;
  public final DataType type;

  public Kolumn(String name, DataType type) {
    this.name = name.toLowerCase();
    this.type = type;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    Kolumn kolumn = (Kolumn) o;

    if (!name.equals(kolumn.name)) return false;
    return type.equals(kolumn.type);
  }

  @Override
  public int hashCode() {
    int result = name.hashCode();
    result = 31 * result + type.hashCode();
    return result;
  }

  public String getName() {
    return name;
  }

  public DataType getType() {
    return type;
  }

  public String getTypeJson() {
    return type.prettyJson();
  }

  @Override
  public String toString() {
    return "Kolumn{" + "name='" + name + '\'' + ", type='" + type + '\'' + '}';
  }

  public static final Kolumn ROW_ID_COLUMN = new Kolumn(Constants.ROW_ID, DataTypes.NullType);

  public static void main(String[] args) {
    System.out.println(DataTypes.NullType.prettyJson());
    System.out.println(DataTypes.StringType.prettyJson());
    System.out.println(DataType.fromJson(DataTypes.TimestampType.prettyJson()));
    System.out.println(DataType.fromJson(DataTypes.NullType.prettyJson()));
  }
}
