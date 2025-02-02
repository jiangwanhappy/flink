/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.plugable;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

/**
 * The serialization delegate exposes an arbitrary element as a {@link IOReadableWritable} for
 * serialization, with the help of a type serializer.
 *
 * @param <T> The type to be represented as an IOReadableWritable.
 */
public class SerializationDelegate<T> implements IOReadableWritable {

    private T instance; // 要序列化的数据

    private final TypeSerializer<T> serializer; // 序列化器

    public SerializationDelegate(TypeSerializer<T> serializer) {
        this.serializer = serializer;
    }

    public void setInstance(T instance) {
        this.instance = instance;
    }

    public T getInstance() {
        return this.instance;
    }
    // 将数据instance根据序列化器serializer序列化保存到out中
    @Override
    public void write(DataOutputView out) throws IOException {
        this.serializer.serialize(this.instance, out);
    }

    @Override
    public void read(DataInputView in) throws IOException {
        throw new IllegalStateException("Deserialization method called on SerializationDelegate.");
    }
}
