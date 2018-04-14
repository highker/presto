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
package com.facebook.presto.hive.metastore;

import com.facebook.presto.hive.HdfsEnvironment.HdfsContext;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.concurrent.Immutable;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

@Immutable
public class Action<T>
{
    private final ActionType type;
    private final T data;
    private final HdfsContext context;

    public Action(ActionType type, T data, HdfsContext context)
    {
        this.type = requireNonNull(type, "type is null");
        if (type == ActionType.DROP) {
            checkArgument(data == null, "data is not null");
        }
        else {
            requireNonNull(data, "data is null");
        }
        this.data = data;
        this.context = requireNonNull(context, "context is null");
    }

    @JsonCreator
    public static <T> Action<T> jsonCreator(
            @JsonProperty("type") ActionType type,
            @JsonProperty("context") HdfsContext context,
            @JsonProperty("data") T data)
    {
        return new Action<>(type, data, context);
    }

    @JsonProperty
    public ActionType getType()
    {
        return type;
    }

    public T getData()
    {
        checkState(type != ActionType.DROP);
        return data;
    }

    @JsonProperty("data")
    public T getUncheckedData()
    {
        return data;
    }

    @JsonProperty
    public HdfsContext getContext()
    {
        return context;
    }

    @Override
    public boolean equals(Object other)
    {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        Action otherAction = (Action) other;
        return Objects.equals(type, otherAction.type) &&
                Objects.equals(data, otherAction.data) &&
                Objects.equals(context, otherAction.context);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(type, data, context);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("type", type)
                .add("data", data)
                .toString();
    }
}
