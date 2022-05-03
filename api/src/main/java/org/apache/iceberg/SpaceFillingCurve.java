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

package org.apache.iceberg;

import java.io.Serializable;
import java.util.List;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;

public abstract class SpaceFillingCurve implements Serializable {
    protected final SortOrder sortOrder;
    private transient volatile List<String> fieldNamesList;

    protected SpaceFillingCurve(Schema schema, List<String> fieldNames) {
        SortOrder.Builder builder = SortOrder.builderFor(schema);
        for (String fieldName : fieldNames) {
            builder.desc(fieldName);
        }
        this.sortOrder = builder.build();
    }

    public Schema schema() {
        return sortOrder.schema();
    }

    public SortOrder sortOrder() {
        return sortOrder;
    }

    public List<String> fieldsNames() {
        return lazyFieldList();
    }

    private List<String> lazyFieldList() {
        if (fieldNamesList == null) {
            synchronized (this) {
                if (fieldNamesList == null) {
                    Schema schema = sortOrder.schema();
                    this.fieldNamesList = sortOrder.fields()
                        .stream()
                        .map(f -> schema.findField(f.sourceId()).name())
                        .collect(ImmutableList.toImmutableList());
                }
            }
        }
        return fieldNamesList;
    }

    public abstract String name();

    public abstract SortOrder localSortOrder();

    public abstract SortOrder distributionsSortOrder();

}
