/**
 * Copyright 2016 Bimal Tandel

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

package org.apache.hadoop.hive.kududb.KuduHandler;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.kudu.Type;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Created by bimal on 4/12/16.
 */
public class HiveKuduWritable implements Writable {


    private Object[] columnValues;
    private Type[] columnTypes;

    public HiveKuduWritable() {

    }

    public HiveKuduWritable(Type[] types) {
        this.columnValues = new Object[types.length];
        this.columnTypes = types;
    }

    public void clear() {
        Arrays.fill(columnValues, null);
    }

    public void set(int i, Object javaObject) {
        columnValues[i] = javaObject;
    }

    public Object get(int i) {
        return columnValues[i];
    }

    public Type getType(int i) { return columnTypes[i]; }

    public int getColCount() {
        return this.columnTypes.length;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int size = in.readInt();
        if (size == -1) {
            return;
        }
        if (columnValues == null) {
            this.columnValues = new Object[size];
            this.columnTypes = new Type[size];
        } else {
            clear();
        }
        
        int nullCounts = in.readInt();
        Set<Integer> nullIndices = new HashSet<Integer>();
        for(int i=0; i<nullCounts; ++i)
            nullIndices.add(in.readInt());
        
        for (int i = 0; i < size; i++) {
            Type kuduType = WritableUtils.readEnum(in, Type.class);
            columnTypes[i] = kuduType;
            Object v = (nullIndices.contains(i)) ? null: 
            	HiveKuduBridgeUtils.readObject(in, kuduType);
            columnValues[i] = v;
        }
    }
    @Override
    public void write(DataOutput out) throws IOException {
        if (columnValues == null) {
            out.writeInt(-1);
            return;
        }
        if (columnTypes == null) {
            out.writeInt(-1);
            return;
        }

        final Object[] values = this.columnValues;
        final Type[] types = this.columnTypes;
        
        out.writeInt(values.length);
        
        Set<Integer> nullIndices = IntStream.range(0, values.length)
            	.filter(x -> values[x] == null)
            	.boxed()
            	.collect(Collectors.toSet());
            	
        out.writeInt(nullIndices.size());
        for(int idx:nullIndices)
        	out.writeInt(idx);
        
        for (int i = 0; i < values.length; i++) {
        	WritableUtils.writeEnum(out, types[i]);
        	if(!nullIndices.contains(i))
        		HiveKuduBridgeUtils.writeObject(values[i], types[i], out);
        }
    }
}
