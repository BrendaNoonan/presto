/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.facebook.presto.tests.hive;

import com.teradata.test.fulfillment.hive.DataSource;
import com.teradata.test.fulfillment.hive.HiveTableDefinition;
import com.teradata.test.fulfillment.table.TableDefinitionsRepository;

import static com.teradata.test.fulfillment.hive.InlineDataSource.createResourceDataSource;

public final class AllSimpleTypesTableDefinitions
{
    private AllSimpleTypesTableDefinitions()
    {
    }

    @TableDefinitionsRepository.RepositoryTableDefinition
    public static final HiveTableDefinition ALL_PRESTO_TYPES_TEXTFILE = allSimpleTypesTableDefinition("TEXTFILE");

    @TableDefinitionsRepository.RepositoryTableDefinition
    public static final HiveTableDefinition ALL_PRESTO_TYPES_PARQUET = allSimpleTypesParquetTableDefinition();

    @TableDefinitionsRepository.RepositoryTableDefinition
    public static final HiveTableDefinition ALL_PRESTO_TYPES_ORC = allSimpleTypesTableDefinition("ORC");

    @TableDefinitionsRepository.RepositoryTableDefinition
    public static final HiveTableDefinition ALL_PRESTO_TYPES_RCFILE = allSimpleTypesTableDefinition("RCFILE");

    private static HiveTableDefinition allSimpleTypesTableDefinition(String fileFormat)
    {
        String tableName = fileFormat.toLowerCase() + "_all_types";
        DataSource dataSource = createResourceDataSource(tableName, "" + System.currentTimeMillis(), "com/facebook/presto/tests/hive/" + tableName + ".data");
        return HiveTableDefinition.builder()
                .setName(tableName)
                .setCreateTableDDLTemplate("" +
                        "CREATE EXTERNAL TABLE %NAME%(" +
                        "   c_tinyint            TINYINT," +
                        "   c_smallint           SMALLINT," +
                        "   c_int                INT," +
                        "   c_bigint             BIGINT," +
                        "   c_float              FLOAT," +
                        "   c_double             DOUBLE," +
                        "   c_decimal            DECIMAL," +
                        "   c_decimal_w_params   DECIMAL(10,5)," +
                        "   c_timestamp          TIMESTAMP," +
                        "   c_date               DATE," +
                        "   c_string             STRING," +
                        "   c_varchar            VARCHAR(10)," +
                        "   c_char               CHAR(10)," +
                        "   c_boolean            BOOLEAN," +
                        "   c_binary             BINARY" +
                        ") " +
                        "ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' " +
                        "STORED AS " + fileFormat + " " +
                        "LOCATION '%LOCATION%'")
                .setDataSource(dataSource)
                .build();
    }

    private static HiveTableDefinition allSimpleTypesParquetTableDefinition()
    {
        String tableName = "parquet_all_types";
        DataSource dataSource = createResourceDataSource(tableName, "" + System.currentTimeMillis(), "com/facebook/presto/tests/hive/" + tableName + ".data");
        return HiveTableDefinition.builder()
                .setName(tableName)
                .setCreateTableDDLTemplate("" +
                        "CREATE EXTERNAL TABLE %NAME%(" +
                        "   c_tinyint            TINYINT," +
                        "   c_smallint           SMALLINT," +
                        "   c_int                INT," +
                        "   c_bigint             BIGINT," +
                        "   c_float              FLOAT," +
                        "   c_double             DOUBLE," +
                        "   c_string             STRING," +
                        "   c_char               CHAR(10)," +
                        "   c_boolean            BOOLEAN" +
                        ") " +
                        "ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' " +
                        "STORED AS PARQUET " +
                        "LOCATION '%LOCATION%'")
                .setDataSource(dataSource)
                .build();
    }
}
