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
import org.apache.iceberg.ZOrderCurve;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.utils.ZOrderUtils$;
import org.apache.spark.sql.connector.iceberg.distributions.Distribution;
import org.apache.spark.sql.connector.iceberg.distributions.Distributions;
import org.apache.spark.sql.connector.iceberg.expressions.SortOrder;

public class Spark3ZOrderSortStrategy extends BaseSpark3SortStrategy {

    Spark3ZOrderSortStrategy(Table table, SparkSession spark, ZOrderCurve spaceCurve) {
        super(table, spark);
        super.sortSpaceCurve(spaceCurve);
    }

    @Override
    protected Dataset<Row> preSort(Dataset<Row> scanDF) {
        return ZOrderUtils$.MODULE$.genZValue(scanDF, spaceFillingCurve());
    }

    @Override
    protected Dataset<Row> postSort(Dataset<Row> sortedDF) {
        return sortedDF.drop(ZOrderCurve.Z_VALUE);
    }

    @Override
    protected SortOrder[] localSortOrder(List<FileScanTask> filesToRewrite, Table table) {
        return Spark3Util.convert(spaceFillingCurve().localSortOrder());
    }

    @Override
    protected Distribution distribution(SortOrder[] localSorting) {
        return Distributions.ordered(Spark3Util.convert(spaceFillingCurve().distributionsSortOrder()));
    }
}
