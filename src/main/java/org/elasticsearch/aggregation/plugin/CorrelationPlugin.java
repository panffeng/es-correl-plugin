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

package org.elasticsearch.aggregation.plugin;


import org.apache.logging.log4j.Logger;
import org.elasticsearch.aggregation.correl.CorrelationPipelineAggregationBuilder;
import org.elasticsearch.aggregation.correl.CorrelationPipelineAggregator;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.aggregations.PipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;


public class CorrelationPlugin extends Plugin implements SearchPlugin {
    private final Logger logger = Loggers.getLogger(getClass());


    @Override
    public List<PipelineAggregationSpec> getPipelineAggregations() {



        Writeable.Reader<? extends PipelineAggregationBuilder> builderReader = new Writeable.Reader<CorrelationPipelineAggregationBuilder>() {
            @Override
            public CorrelationPipelineAggregationBuilder read(StreamInput in) throws IOException {
                return new CorrelationPipelineAggregationBuilder(in);
            }
        };

        Writeable.Reader<? extends CorrelationPipelineAggregator> aggregatorReader = new Writeable.Reader<CorrelationPipelineAggregator>() {
            @Override
            public CorrelationPipelineAggregator read(StreamInput in) throws IOException {
                return new CorrelationPipelineAggregator(in);
            }
        };

        // not null value
        PipelineAggregator.Parser parser=new PipelineAggregator.Parser() {
            @Override
            public PipelineAggregationBuilder parse(String s, QueryParseContext queryParseContext) throws IOException {
                return CorrelationPipelineAggregationBuilder.parse(s, queryParseContext);
            }
        };
        PipelineAggregationSpec pipelineAggregationSpec = new PipelineAggregationSpec( CorrelationPipelineAggregationBuilder.NAME,
                 builderReader,
                aggregatorReader,parser
               ) ;
        logger.info("correlation plugin added to lifecycle");
        return Arrays.asList(pipelineAggregationSpec);
    }
}
