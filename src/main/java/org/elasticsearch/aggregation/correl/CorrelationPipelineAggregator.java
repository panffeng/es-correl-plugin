/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.aggregation.correl;


import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregation.ReduceContext;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.pipeline.BucketHelpers.GapPolicy;
import org.elasticsearch.search.aggregations.pipeline.InternalSimpleValue;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.io.IOException;
import java.util.*;

import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.elasticsearch.search.aggregations.pipeline.BucketHelpers.resolveBucketValue;

public class CorrelationPipelineAggregator extends PipelineAggregator {
    private final DocValueFormat formatter;
    private final GapPolicy gapPolicy;
    private final int lag;
    private final Map<String, String> bucketsPathsMap;

    private final Logger logger = Loggers.getLogger(getClass());


    public CorrelationPipelineAggregator(String name, Map<String, String> bucketsPathsMap, int lag, DocValueFormat formatter,
                                         GapPolicy gapPolicy, Map<String, Object> metadata) {
        super(name, bucketsPathsMap.values().toArray(new String[bucketsPathsMap.size()]), metadata);
        this.bucketsPathsMap = bucketsPathsMap;
        this.lag = lag;
        this.formatter = formatter;
        this.gapPolicy = gapPolicy;
    }

    /**
     * Read from a stream.
     */
    @SuppressWarnings("unchecked")
    public CorrelationPipelineAggregator(StreamInput in) throws IOException {
        super(in);
        lag = in.readVInt();
        formatter = in.readNamedWriteable(DocValueFormat.class);
        gapPolicy = GapPolicy.readFrom(in);
        bucketsPathsMap = (Map<String, String>) in.readGenericValue();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeVInt(lag);
        out.writeNamedWriteable(formatter);
        gapPolicy.writeTo(out);
        out.writeGenericValue(bucketsPathsMap);
    }

    @Override
    public String getWriteableName() {
        return CorrelationPipelineAggregationBuilder.NAME;
    }

    @Override
    public InternalAggregation reduce(InternalAggregation aggregation, ReduceContext reduceContext) {
        InternalMultiBucketAggregation<InternalMultiBucketAggregation, InternalMultiBucketAggregation.InternalBucket> originalAgg =
                (InternalMultiBucketAggregation<InternalMultiBucketAggregation, InternalMultiBucketAggregation.InternalBucket>) aggregation;
        List<? extends InternalMultiBucketAggregation.InternalBucket> buckets = originalAgg.getBuckets();


        List<InternalMultiBucketAggregation.InternalBucket> newBuckets = new ArrayList<>();

        List<InternalAggregation> relatedAggs = null;
        InternalMultiBucketAggregation.InternalBucket relatedBucket = null;

        Map<String,List<Double>> map = new HashMap<>();

        logger.info("buckets"+buckets);

        for (InternalMultiBucketAggregation.InternalBucket bucket : buckets) {


            boolean skipBucket = false;

            for (Map.Entry<String, String> entry : bucketsPathsMap.entrySet()) {
                String varName = entry.getKey();
                String bucketsPath = entry.getValue();
                Double value = resolveBucketValue(originalAgg, bucket, bucketsPath, gapPolicy);

                logger.info("resolveBucketValue:"+varName+"\t"+bucketsPath+"\t"+value);
//                if (GapPolicy.SKIP == gapPolicy && (value == null || Double.isNaN(value))) {
//                    skipBucket = true;
//                    break;
//                }
                if(Double.isNaN(value)||value==null){
                    value=0d;
                }
                putIntoLists(map,varName + bucketsPath, value);

            }
//            if (skipBucket) {
//                newBuckets.add(bucket);
//            } else {


            final List<InternalAggregation> aggs = StreamSupport.stream(bucket.getAggregations().spliterator(), false).map(
                    (p) -> (InternalAggregation) p).collect(Collectors.toList());

            if (relatedAggs == null) {
                relatedAggs = aggs;
                relatedBucket = bucket;
            }
//            }
        }


        Set<String> keys = map.keySet();
        Double returned = -1d;

        logger.info("the map for correlation"+map);

        if (keys.size() == 2) {

            List<Collection<Double>> theValueList = keys.stream().map((x) -> (map.get(x))).collect(Collectors.toList());
            Collection<Double> x = theValueList.get(0);
            Collection<Double> y = theValueList.get(1);


            logger.info("x::the vars for correlation"+x);
            logger.info("y::the vars for correlation"+y);

            double[] xArray = new double[x.size()];
            int idx = 0;
            for (double i : x) {
                xArray[idx++] = i;
            }
            double[] yArray = new double[y.size()];
            idx = 0;
            for (double i : y) {
                yArray[idx++] = i;
            }


            returned = new PearsonsCorrelation().correlation(xArray, yArray);

            if (!(returned instanceof Number)) {
                throw new AggregationExecutionException("PearsonsCorrelation for reducer [" + name()
                        + "] must return a Number");
            }
        }
        relatedAggs.add(new InternalSimpleValue(name(), ((Number) returned).doubleValue(), formatter,
                new ArrayList<>(), metaData()));
        InternalMultiBucketAggregation.InternalBucket newBucket = originalAgg.createBucket(new InternalAggregations(relatedAggs),
                relatedBucket);
        newBuckets.add(newBucket);

        return originalAgg.create(newBuckets);
    }

    private void putIntoLists(Map<String, List<Double>> map, String s, Double value) {
        List<Double> list = map.get(s);
        if(list==null){
            list=new LinkedList<>();
        }
        list.add(value);
        map.put(s,list);
    }
}
