package lupos.cloud.pig.udfs;

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
 * 
 * Angepasste Version. z.B. gepatcht:
 * 
 * https://issues.apache.org/jira/browse/PIG-2348
 * 
 */


import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;

import org.apache.pig.FilterFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;

/**
 * Use a Bloom filter build previously by BuildBloom.  You would first
 * build a bloom filter in a group all job.  For example:
 * in a group all job.  For example:
 * define bb BuildBloom('jenkins', '10', '0.1');
 * small = load 'S' as (x, y, z);
 * grpd = group small all;
 * fltrd = foreach grpd generate bb(small.x) as a0;
 * The bloom filter can be on multiple keys by passing more than one field
 * (or the entire bag) to BuildBloom.
 * The resulting file can then be used in a Bloom filter as:
 * define bloom Bloom(mybloom);
 * large = load 'L' as (a, b, c);
 * flarge = filter large by Bloom(fltrd.a0, a);
 * joined = join small by x, flarge by a;
 * store joined into 'results';
 * It uses {@link org.apache.hadoop.util.bloom.BloomFilter}.
 */
public class CloudBloom extends FilterFunc {

    public BloomFilter filter = null;

    public CloudBloom() {
    }

    @Override
    public Boolean exec(Tuple input) throws IOException {
        byte[] b;
        if (filter==null) {
            filter = new BloomFilter();
            filter.readFields(new DataInputStream(new ByteArrayInputStream(((DataByteArray)input.get(0)).get())));
        }
        if (input.size() == 1) b = DataType.toBytes(input.get(0));
        else {
            DataByteArray dba = new DataByteArray();
            for (int i=1;i<input.size();i++)
                dba.append(new DataByteArray(DataType.toBytes(input.get(i))));
            b = dba.get();
        }

        Key k = new Key(b);
        return filter.membershipTest(k);
    }


    /**
     * For testing only, do not use directly.
     */
    public void setFilter(DataByteArray dba) throws IOException {
        DataInputStream dis = new DataInputStream(new
            ByteArrayInputStream(dba.get()));
        filter = new BloomFilter();
        filter.readFields(dis);
    }

}
