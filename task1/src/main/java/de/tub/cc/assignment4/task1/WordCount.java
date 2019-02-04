package de.tub.cc.assignment4.task1;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.util.Collector;

import java.lang.invoke.MethodHandles;


public class WordCount {
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        // get input data
        DataSet<String> text;
        if (params.has("input")) {
            // read the text file from given input path
            text = env.readTextFile(params.get("input"));
        } else {
            // get default test text data
            log.error("Use --input to specify file input.");
            return;
        }

        DataSet<Tuple2<String, Integer>> counts =
                // split up the lines in pairs (2-tuples) containing: (word,1)
                text.flatMap(new Tokenizer())
                        // group by the tuple field "0" and sum up tuple field "1"
                        .groupBy(0)
                        .sum(1);

        // emit result
        if (params.has("output")) {
            if (params.has("parallelism")) {
                int parallelism = Integer.parseInt(params.get("parallelism"));
                log.info("Running flink using parallelism = {}", parallelism);
                log.info("To use AUTO parallelism use value -1");
                counts.sortPartition(0, Order.ASCENDING)
                        .writeAsCsv(params.get("output"), "\n", ",")
                        .setParallelism(parallelism); // Save output to the one file - parallelism default = -1 AUTO
            } else {
                log.info("Running Flink using paralelism = 1.");
                counts.sortPartition(0, Order.ASCENDING)
                        .writeAsCsv(params.get("output"), "\n", ",")
                        .setParallelism(1);
            }
            // execute flink
            env.execute();
        } else {
            log.info("Printing result to stdout. Use --output to specify output path.");
            counts.print();
        }

    }


    public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {

            // "\W+" splits the txt file on all non word characters!
            // "\\P{Alpha}+" - matches any non-alphabetic character
            String[] tokens = value.toLowerCase().split("\\P{Alpha}+");

            // emit the pairs
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new Tuple2<>(token, 1));
                }
            }
        }
    }

}
