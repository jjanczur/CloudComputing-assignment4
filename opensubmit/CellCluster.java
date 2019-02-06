package de.tub.cc.assignment4.task2;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;

@SuppressWarnings("serial")
public class CellCluster
{
    private static int parallelism = 1;

    public static void main(String[] args) throws Exception
    {
        final ParameterTool params = ParameterTool.fromArgs(args);

        // Set parallelism grade

        if(params.has("parallelism"))
        {
            parallelism = params.getInt("parallelism");
        }


        // Set up execution environment

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params); // make parameters available in the web interface


        // Read tower information from the provided path or die

        DataSet<Tower> towers = getTowers(params, env);


        // Read relevant MNCs

        Collection<Integer> mnc = new ArrayList<>();
        if(params.has("mnc"))
        {
            for(String str : params.get("mnc").split(";"))
            {
                mnc.add(Integer.valueOf(str));
            }
        }


        // Split towers in Points (GSM and UMTS) and Centroids (LTE)

        DataSet<Point> points = towers
                .filter(tower -> mnc.isEmpty()|| mnc.contains(tower.net))
                .filter(tower -> !tower.radio.equals("LTE"))
                .map(tower -> new Point(tower.lat, tower.lon));

        DataSet<Centroid> centroids = towers
                .filter(tower -> tower.radio.equals("LTE"))
                .map(tower -> new Centroid(tower.cell, tower.lat, tower.lon));


        // Use first k centroids, if k provided

        if(params.has("k"))
        {
            centroids = centroids.first(params.getInt("k"));
        }


        // LOOP
        IterativeDataSet<Centroid> loop = centroids.iterate(params.getInt("iterations", 10));

        DataSet<Centroid> newCentroids = points

                // Compute closest centroid for each point
                .map(new SelectNearestCenter())
                .withBroadcastSet(loop, "centroids")

                // Count and sum point coordinates for each centroid
                .map(new CountAppender())
                .groupBy(0)
                .reduce(new CentroidAccumulator())

                // Compute new centroids from point counts and coordinate sums
                .map(new CentroidAverager());

        DataSet<Centroid> finalCentroids = loop.closeWith(newCentroids);
        // END

        // Assign points to clusters

        DataSet<Tuple2<Integer, Point>> clusteredPoints = points
                .map(new SelectNearestCenter())
                .withBroadcastSet(finalCentroids, "centroids");


        // Emit result

        if (params.has("output"))
        {
            // Write content to a tmp file

            clusteredPoints
                    .writeAsCsv(params.get("output"), "\n", ",", FileSystem.WriteMode.OVERWRITE)
                    .setParallelism(parallelism);

            env.execute("de.tub.cc.assignment4.task2.CellCluster Example");



        } else
            {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            clusteredPoints.print();
        }
    }

    // *************************************************************************
    //     UTILITY FUNCTIONS
    // *************************************************************************

    private static DataSet<Tower> getTowers(ParameterTool params, ExecutionEnvironment env)
    {
        DataSet<Tower> towers;

        if(!params.has("input"))
        {
            System.out.println("No input file specified. Use --input <file> to do so.");
            System.exit(10);
        }

        towers = env.readCsvFile(params.get("input"))
                .ignoreFirstLine()
                .ignoreInvalidLines()
                .includeFields("10101011000000")
                .fieldDelimiter(",")
                .pojoType(Tower.class, "radio", "net", "cell", "lon", "lat");

        return towers;
    }

    // *************************************************************************
    //     DATA TYPES
    // *************************************************************************

    /**
     * Cell tower.
     */
    public static class Tower implements Serializable
    {
        public int cell;
        public int net;
        public String radio;
        public double lon, lat;

        public Tower() {}

        public Tower(int cell, int net, String radio, double lon, double lat)
        {
            this.cell = cell;
            this.net = net;
            this.radio = radio;
            this.lon = lon;
            this.lat = lat;
        }
    }

    /**
     * A simple two-dimensional point.
     */
    public static class Point implements Serializable {

        public double x, y;

        public Point() {}

        public Point(double x, double y) {
            this.x = x;
            this.y = y;
        }

        public Point add(Point other) {
            x += other.x;
            y += other.y;
            return this;
        }

        public Point div(long val) {
            x /= val;
            y /= val;
            return this;
        }

        public double euclideanDistance(Point other) {
            return Math.sqrt((x - other.x) * (x - other.x) + (y - other.y) * (y - other.y));
        }

        public void clear() {
            x = y = 0.0;
        }

        @Override
        public String toString() {
            return x + "," + y;
        }
    }

    /**
     * A simple two-dimensional centroid, basically a point with an ID.
     */
    public static class Centroid extends Point {

        public int id;

        public Centroid() {}

        public Centroid(int id, double x, double y) {
            super(x, y);
            this.id = id;
        }

        public Centroid(int id, Point p) {
            super(p.x, p.y);
            this.id = id;
        }

        @Override
        public String toString() {
            return id + "," + super.toString();
        }
    }

    // *************************************************************************
    //     USER FUNCTIONS
    // *************************************************************************

    /** Determines the closest cluster center for a data point. */
    @ForwardedFields("*->1")
    public static final class SelectNearestCenter extends RichMapFunction<Point, Tuple2<Integer, Point>> {
        private Collection<Centroid> centroids;

        /** Reads the centroid values from a broadcast variable into a collection. */
        @Override
        public void open(Configuration parameters) throws Exception {
            this.centroids = getRuntimeContext().getBroadcastVariable("centroids");
        }

        @Override
        public Tuple2<Integer, Point> map(Point p) throws Exception {

            double minDistance = Double.MAX_VALUE;
            int closestCentroidId = -1;

            // check all cluster centers
            for (Centroid centroid : centroids) {
                // compute distance
                double distance = p.euclideanDistance(centroid);

                // update nearest cluster if necessary
                if (distance < minDistance) {
                    minDistance = distance;
                    closestCentroidId = centroid.id;
                }
            }

            // emit a new record with the center id and the data point.
            return new Tuple2<>(closestCentroidId, p);
        }
    }

    /** Appends a count variable to the tuple. */
    @ForwardedFields("f0;f1")
    public static final class CountAppender implements MapFunction<Tuple2<Integer, Point>, Tuple3<Integer, Point, Long>> {

        @Override
        public Tuple3<Integer, Point, Long> map(Tuple2<Integer, Point> t) {
            return new Tuple3<>(t.f0, t.f1, 1L);
        }
    }

    /** Sums and counts point coordinates. */
    @ForwardedFields("0")
    public static final class CentroidAccumulator implements ReduceFunction<Tuple3<Integer, Point, Long>> {

        @Override
        public Tuple3<Integer, Point, Long> reduce(Tuple3<Integer, Point, Long> val1, Tuple3<Integer, Point, Long> val2) {
            return new Tuple3<>(val1.f0, val1.f1.add(val2.f1), val1.f2 + val2.f2);
        }
    }

    /** Computes new centroid from coordinate sum and count of points. */
    @ForwardedFields("0->id")
    public static final class CentroidAverager implements MapFunction<Tuple3<Integer, Point, Long>, Centroid> {

        @Override
        public Centroid map(Tuple3<Integer, Point, Long> value) {
            return new Centroid(value.f0, value.f1.div(value.f2));
        }
    }
}
