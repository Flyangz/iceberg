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

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import static org.apache.iceberg.types.Type.TypeID.BINARY;
import static org.apache.iceberg.types.Type.TypeID.DATE;
import static org.apache.iceberg.types.Type.TypeID.DECIMAL;
import static org.apache.iceberg.types.Type.TypeID.DOUBLE;
import static org.apache.iceberg.types.Type.TypeID.FLOAT;
import static org.apache.iceberg.types.Type.TypeID.INTEGER;
import static org.apache.iceberg.types.Type.TypeID.LONG;
import static org.apache.iceberg.types.Type.TypeID.STRING;
import static org.apache.iceberg.types.Type.TypeID.TIME;
import static org.apache.iceberg.types.Type.TypeID.TIMESTAMP;

public class ZOrderCurve extends SpaceFillingCurve {
    public final static String RANK_SIZE = "rank_size";
    public final static String Z_VALUE = "z_value";

    private final Strategy strategy;
    private final Map<String, String> opts;
    private final Schema zValueSchema = new Schema(Types.NestedField.required(1, Z_VALUE, Types.IntegerType.get()));

    private ZOrderCurve(Schema schema, List<String> fieldNames, Strategy strategy, Map<String, String> opts) {
        super(schema, fieldNames);
        this.strategy = strategy;
        this.opts = ImmutableMap.copyOf(opts);
    }

    @Override
    public String name() {
        return "ZOrderCurve";
    }

    @Override
    public SortOrder localSortOrder() {
        String localSortFieldName = fieldsNames().get(0);
        return SortOrder.builderFor(sortOrder().schema()).asc(localSortFieldName).build();
    }

    @Override
    public SortOrder distributionsSortOrder() {
        return SortOrder.builderFor(zValueSchema).asc(Z_VALUE).build();
    }

    public Strategy getStrategy() {
        return strategy;
    }

    public String getPropertyOrDefault(String key, String defaultValue) {
        return opts.getOrDefault(key, defaultValue);
    }

    public Map<String, String> properties() {
        return opts;
    }

    public static Builder builderFor(Schema schema) {
        return new Builder(schema);
    }

    public static class Builder {
        private final Schema schema;
        private final List<String> fieldNames = Lists.newArrayList();
        private final Map<String, String> opts = Maps.newHashMap();
        private Strategy strategy = Strategy.SAMPLE;
        private Set<Type.TypeID> supportedType = Sets.newHashSet(
            INTEGER, LONG, FLOAT, DOUBLE, DATE, TIME, TIMESTAMP, STRING, BINARY, DECIMAL);

        public Builder(Schema schema) {
            this.schema = schema;
        }

        public Builder addField(String fieldName) {
            fieldNames.add(fieldName);
            return this;
        }

        public Builder addFields(List<String> fieldNames) {
            this.fieldNames.addAll(fieldNames);
            return this;
        }

        public Builder setStrategy(Strategy strategy) {
            this.strategy = strategy;
            return this;
        }

        public Builder addOption(String key, String value) {
            opts.put(key, value);
            return this;
        }

        public SpaceFillingCurve build() {
            ValidationException.check(!fieldNames.isEmpty(), "z-order fields should not be empty.");
            for (String fieldName : fieldNames) {
                Types.NestedField field = schema.findField(fieldName);
                ValidationException.check(
                    field != null,
                    "Cannot find source column for sort field: %s", fieldName);
                ValidationException.check(
                    supportedType.contains(field.type().typeId()),
                    "Unsupported type for z-order field: %s", field.type()
                );
            }

            return new ZOrderCurve(schema, fieldNames, strategy, opts);
        }
    }

    public enum Strategy {
        SAMPLE("sample"), VALUE("value");

        private final String strategyName;

        Strategy(String strategyName) {
            this.strategyName = strategyName;
        }

        public String strategyName() {
            return strategyName;
        }

        public static Strategy fromName(String name) {
            Preconditions.checkNotNull(name, "Name of z-order curve strategy should not be null");
            return Strategy.valueOf(name.toUpperCase(Locale.ENGLISH));
        }
    }
}
