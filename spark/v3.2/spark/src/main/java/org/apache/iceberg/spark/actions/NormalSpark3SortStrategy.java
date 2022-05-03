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

package org.apache.iceberg.spark.actions;

import java.util.List;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.util.SortOrderUtil;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.iceberg.distributions.Distribution;
import org.apache.spark.sql.connector.iceberg.distributions.Distributions;
import org.apache.spark.sql.connector.iceberg.expressions.SortOrder;

public class NormalSpark3SortStrategy extends BaseSpark3SortStrategy {
    public NormalSpark3SortStrategy(Table table, SparkSession spark) {
        super(table, spark);
    }

    @Override
    protected SortOrder[] localSortOrder(List<FileScanTask> filesToRewrite, Table table) {
        SortOrder[] ordering;
        boolean requiresRepartition = !filesToRewrite.get(0).spec().equals(table.spec());
        if (requiresRepartition) {
            // Build in the requirement for Partition Sorting into our sort order
            ordering = Spark3Util.convert(SortOrderUtil.buildSortOrder(table.schema(), table.spec(), sortOrder()));
        } else {
            ordering = Spark3Util.convert(sortOrder());
        }
        return ordering;
    }

    @Override
    protected Distribution distribution(SortOrder[] localSorting) {
        return Distributions.ordered(localSorting);
    }
}
