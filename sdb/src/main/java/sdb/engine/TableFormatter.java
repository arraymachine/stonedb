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
import org.joda.time.DateTime;

public final class TableFormatter {
    private static Logger logger = Logger.getLogger(TableFormatter.class.getName());

    public static String csv(Table table) {
        return encode(table, ',');
    }

    public static String tsv(Table table) {
        return encode(table, '\t');
    }

    public static String encode(Table table, char separator){
        StringBuffer buf = new StringBuffer();

        buf.append("#");
        String[] col_name = table.getSchemaNames();
        for(int i = 0; i < col_name.length; i++) {
           buf.append(col_name[i]);
           if (i < col_name.length -1)
             buf.append(separator);
        }
        buf.append("\n");

        for(int i = 0; i < table.count(); i++) {
            Object[] row = table.get(i);
            for (int c = 0; c < row.length; c++) {
                if (row[c] instanceof char[])
                    buf.append(new String((char[])row[c]));
                else
                    buf.append(row[c]);
                if (c < row.length - 1)
                    buf.append(separator);
            }
            buf.append("\n");
        }
        buf.append("\n");
        return buf.toString();
    }

    public static String json(Table table) {
        StringBuffer buf = new StringBuffer();

        buf.append("{\"name\":[");
        String[] col_name = table.getSchemaNames();
        for(int i = 0; i < col_name.length; i++) {
            buf.append("\"" + col_name[i] + "\"");
            if (i < col_name.length -1)
                buf.append(",");
        }
        buf.append("],");

        buf.append("\"type\":[");
        Table.Column_Type[] col_type= table.getSchemaTypes();
        for(int i = 0; i < col_type.length; i++) {
            buf.append("\"" + col_type[i].name() + "\"");
            if (i < col_type.length -1)
                buf.append(",");
        }
        buf.append("],");

        buf.append("\"data\":[");
        int rowCount = table.count();
        for(int i = 0; i < rowCount; i++) {
            Object[] row = table.get(i);
            buf.append("[");
            for (int c = 0; c < row.length; c++) {
                if (row[c] instanceof char[])
                    buf.append("\"" + new String((char[])row[c]) + "\"");
                else if (row[c] instanceof DateTime)
                    buf.append("\"" + row[c] + "\"");
                else
                    buf.append(row[c]);
                if (c < row.length - 1)
                    buf.append(",");
            }
            buf.append((i < rowCount - 1) ? "]," : "]");
        }
        buf.append("]}");

        return buf.toString();
    }

    public static Object[] getArray(Table.Vector vector){
        Object[] array = new Object[vector.length()];
        for(int i=0; i< vector.length(); i++) {
            switch (vector.type){
                case VECTOR:
                    array[i] = getArray((Table.Vector)(vector.get(i)));
                    break;
                case SYMBOL:
                    array[i] = new String((char[])vector.get(i));
                    break;
                case TIMESTAMP:
                    array[i] = ((DateTime) vector.get(i)).getMillis();
                    break;
                default:
                    array[i] = vector.get(i);
                    break;
            }
        }
        return array;
    }
}
