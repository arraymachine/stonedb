/**
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package sdb.engine;

import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.ParseException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import au.com.bytecode.opencsv.CSVReader;

public class CsvTable {

  private static Logger logger = Logger.getLogger(CsvTable.class.getName());
  private static DateFormat _df = new SimpleDateFormat("MM/dd/yy");
  private static Calendar _d = Calendar.getInstance();

  private static Object convertToTimestamp(String value) {
    if (value.indexOf('.') < 0) {
      try {
        return Long.parseLong(value);
      }
      catch (NumberFormatException e) {
      }
    }
    else {
      try {
        return ((long)Double.parseDouble(value) * 1000);
      }
      catch (NumberFormatException e) {
      }
    }

    try {
      _d.setTime(_df.parse(value));
      return _d.getTimeInMillis();
    }
    catch (ParseException e) {
    }

    return value;
  }

  private static Object convertToLong(String value) {
    try {
      return Long.parseLong(value);
    }
    catch (NumberFormatException e) {
    }

    try {
      return (long)Double.parseDouble(value);
    }
    catch (NumberFormatException e) {
    }

    return value;
  }

  private static Object convertToInt(String value) {
    try {
      return Integer.parseInt(value);
    }
    catch (NumberFormatException e) {
    }

    try {
      return (int)Long.parseLong(value);
    }
    catch (NumberFormatException e) {
    }

    try {
      return (int)Double.parseDouble(value);
    }
    catch (NumberFormatException e) {
    }

    return value;
  }

  private static Object convertToTableType(String value, Table.Column_Type type) {
    switch (type) {
      case VARCHAR8:
      case VARCHAR16:
      case VARCHAR256: 
        return value.toCharArray();
      case TIMESTAMP:
        return convertToTimestamp(value);
      case DOUBLE:
        return Double.parseDouble(value);
      case INT:
        return convertToInt(value);
      case LONG:
        return convertToLong(value);
      case BOOLEAN:
        return Boolean.parseBoolean(value);
      default:
        return value;
    }
  }

  public static Table load(CSVReader csvReader, Table table, boolean hasHeader)
    throws FileNotFoundException, IOException {
    Table.Column_Type[] schema = table.getSchemaTypes();
    Object[] tuple = new Object[schema.length];
    if (hasHeader) {
      csvReader.readNext();
    }
    String[] values;
    while ((values = csvReader.readNext()) != null) {
      if (values.length != schema.length) {
        logger.warn("line does not match schema: expected column count = " + schema.length + ", actual = " + values.length + ". Skipping");
        continue;
      }
      for (int i = 0; i < schema.length; i++) {
        tuple[i] = convertToTableType(values[i], schema[i]);
      }
      table.append(tuple);
    }
    return table;
  }

  public static Table load(String fileName, Table table, boolean hasHeader)
    throws FileNotFoundException, IOException {
    CSVReader csvReader = new CSVReader(new BufferedReader(new FileReader(fileName)));
    Table ret = load(csvReader, table, hasHeader);
    csvReader.close();
    return ret;
  }
}
