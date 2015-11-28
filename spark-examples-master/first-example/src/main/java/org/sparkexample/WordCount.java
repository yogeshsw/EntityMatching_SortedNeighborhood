package org.sparkexample;

import com.google.common.base.Optional;
import org.apache.spark.*;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;

import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import au.com.bytecode.opencsv.*;

import java.io.*;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.*;

public class WordCount {
    public static class ParseLine implements PairFunction<String, String, String[]>
    {

        public Tuple2<String, String[]> call(String line) throws Exception {
            CSVReader reader = new CSVReader(new StringReader(line));
            String[] str = reader.readNext();
            String[] elements = new String[7];

            if(str != null && str[0].matches("-?\\d+(\\.\\d+)?")) {
                elements[0] = str[1];
                elements[1] = str[4];
                elements[2] = str[5];
                elements[3] = str[6];
                elements[4] = str[0];
                elements[5] = str[7];
                elements[6] = str[8];
                return new Tuple2((str[2].length() % 2) + "." + str[3], elements);
            }
            String key = "blank";
            if(str == null)
            {
                return new Tuple2(key, str);
            }
            System.out.println("1st value" + str[0]);
            if(!str[0].matches("-?\\d+(\\.\\d+)?"))
            {
                return new Tuple2(key, str);
            }
            if (str != null)
            {
                if (str.length >= 7) {
                    key = str[2] + "." + str[3];
                }
            }
            return new Tuple2(key, elements);
        }
    }

    Double key_maps = 0.0;
    static int iterator = 0;

    public static long temp_count = 0;
    public static int reducer_count = 1;

    static int part1 = 0, part2 = 0;

    private static final Function<Tuple2<String, String[]>, Boolean> NULL_FILTER = new Function<Tuple2<String, String[]> ,Boolean>() {
        @Override
        public Boolean call(Tuple2<String, String[]> i) {
            return !i._1().equalsIgnoreCase("blank");
        }
    };

    private static final PairFunction<Tuple2<String, String[]>, String, String[]> EXTRA_MR =
            new PairFunction<Tuple2<String, String[]> ,String, String[]>() {
                @Override
                public Tuple2<String, String[]> call(Tuple2<String, String[]> i) {
                    String str = "1." + i._1();
                    return new Tuple2(str, i._2());
                }
            };

    private static final PairFunction<Tuple2<String, String[]>, String, String[]> EXTRA_MR_PART =
            new PairFunction<Tuple2<String, String[]> ,String, String[]>() {
                @Override
                public Tuple2<String, String[]> call(Tuple2<String, String[]> i) {

                    return new Tuple2(i._1(), i._2());
                }
            };

    public static int i = 0;

    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            System.err.println("Please provide the input file full path as argument");
            System.exit(0);
        }

        SparkConf conf = new SparkConf().setAppName("org.sparkexample.WordCount").setMaster("local[4]").setSparkHome("C:\\spark-1.5.1-bin-hadoop2.3\\bin").set("spark.driver.memory", "3g");

        JavaSparkContext context = new JavaSparkContext(conf);

        Partitioner partitioner = new Partitioner() {
            public int numPartitions() {
                return 2;
            }
            public int getPartition(Object key) {

                String[] str = ((String)key).split("\\.");

                return Integer.parseInt(str[0]);
            }
        };

        final int part_nbr = 1;
        JavaRDD<String> file1 = context.textFile(args[0]);
        JavaPairRDD<String, String[]> keyedRDD1 = file1.mapToPair(new ParseLine())
                .filter(NULL_FILTER)
                .cache();

        final JavaPairRDD<String, String[]> repartitioned = keyedRDD1
                .repartitionAndSortWithinPartitions(partitioner);

        TaskContext context_task = TaskContext$.MODULE$.empty();
        Partition p;
        //window size 3, partitions 2
        boolean flag_repart = false;
        JavaPairRDD<String, String[]> repartitioned_new = repartitioned;
        final List<Tuple2<String, String[]>>[] parts = repartitioned.collectPartitions(new int[] {0, 1});
        int partition_nbr = 0, partition_size = 0, count_records = 0;
        if((parts[1].size() - parts[0].size()) < 0.3 * (parts[1].size() + parts[0].size()) )
        {
            partition_nbr = 1;
            partition_size = parts[1].size();
            flag_repart = true;
        }
        else if ((parts[0].size() - parts[1].size()) < 0.3 * (parts[1].size() + parts[0].size()) )
        {
            partition_nbr = 0;
            partition_size = parts[0].size();
            flag_repart = true;
        }
        else
        {
            //no change in partitions
        }

        if(flag_repart)
        {
            final int partition_nbr_new = partition_nbr, partition_size_new = partition_size, count_records_new = partition_size;
            final List<Integer> tup_arr_int = new ArrayList<Integer>();;
            int count_tup = 0;
            Partitioner partitioner_new = new Partitioner() {
                public int numPartitions() {
                    return 3;
                }
                public int getPartition(Object key) {

                    String[] str = ((String)key).split("\\.");
                    if(partition_nbr_new == Integer.parseInt(str[0]))
                    {
                        tup_arr_int.add(1);
                        if(tup_arr_int.size() < (partition_size_new/2))
                        {
                            return Integer.parseInt(str[0]);
                        }
                        else
                        {
                            return 2;
                        }
                    }
                    return Integer.parseInt(str[0]);
                }
            };
            repartitioned_new = keyedRDD1.repartitionAndSortWithinPartitions(partitioner_new);
        }


        final List<Tuple2<String, String[]>> tup_arr = new ArrayList<Tuple2<String, String[]>>();;
        if(!flag_repart)
        {
            tup_arr.add(parts[0].get(parts[0].size() - 1));
            tup_arr.add(parts[0].get(parts[0].size() - 2));
            tup_arr.add(parts[1].get(0));
            tup_arr.add(parts[1].get(1));
        }
        else
        {
            System.out.println("size : " + repartitioned_new.partitions().size());
            System.out.println("count : " + repartitioned_new.count());
            List<Tuple2<String, String[]>>[] parts_new = repartitioned_new.collectPartitions(new int[] {0, 1, 2});
            tup_arr.add(new Tuple2<String, String[]>("0." + parts_new[0].get(parts_new[0].size() - 1)._1(), parts_new[0].get(parts_new[0].size() - 1)._2()));
            tup_arr.add(new Tuple2<String, String[]>("0." + parts_new[0].get(parts_new[0].size() - 2)._1(), parts_new[0].get(parts_new[0].size() - 2)._2()));
            tup_arr.add(new Tuple2<String, String[]>("0." + parts_new[1].get(0)._1(), parts_new[1].get(0)._2()));
            tup_arr.add(new Tuple2<String, String[]>("0." + parts_new[1].get(1)._1(), parts_new[1].get(1)._2()));

            tup_arr.add(new Tuple2<String, String[]>("1." + parts_new[1].get(parts_new[1].size() - 1)._1(), parts_new[1].get(parts_new[1].size() - 1)._2()));
            tup_arr.add(new Tuple2<String, String[]>("1." + parts_new[1].get(parts_new[1].size() - 2)._1(), parts_new[1].get(parts_new[1].size() - 2)._2()));
            tup_arr.add(new Tuple2<String, String[]>("1." + parts_new[2].get(0)._1(), parts_new[2].get(0)._2()));
            tup_arr.add(new Tuple2<String, String[]>("1." + parts_new[2].get(1)._1(), parts_new[2].get(1)._2()));

        }

        if(!flag_repart) {
            repartitioned.foreachPartition(new VoidFunction<Iterator<Tuple2<String, String[]>>>() {
                @Override
                public void call(Iterator<Tuple2<String, String[]>> tuple2Iterator) throws Exception {
                    String t = "temp";
                    String t2[] = new String[10];
                    Tuple2<String, String[]> temp = new Tuple2<String, String[]>(t, t2);
                    Tuple2<String, String[]> temp2 = new Tuple2<String, String[]>(t, t2);
                    Tuple2<String, String[]> temp3 = new Tuple2<String, String[]>(t, t2);


                    while (tuple2Iterator.hasNext()) {
                        if (temp._1().equals(t)) {
                            temp = tuple2Iterator.next();
                        } else if (temp2._1().equals(t)) {
                            temp2 = tuple2Iterator.next();
                        } else {
                            temp3 = tuple2Iterator.next();
                            System.out.println("window:");
                            System.out.println(temp._1() + " , " + temp._1());
                            System.out.println(temp._1() + " , " + temp2._1());
                            System.out.println(temp._1() + " , " + temp3._1());

                            if (temp._2()[5].equals(temp._2()[5])) {
                                System.out.println(temp._2()[5] + "  " + temp._2()[5]);
                            }
                            if (temp._2()[5].equals(temp2._2()[5])) {
                                System.out.println(temp._2()[5] + "  " + temp2._2()[5]);
                            }
                            if (temp._2()[5].equals(temp3._2()[5])) {
                                System.out.println(temp._2()[5] + "  " + temp3._2()[5]);
                            }
                            temp = temp2;
                            temp2 = temp3;
                        }
                    }
                }
            });
        }
        else
        {
            repartitioned_new.foreachPartition(new VoidFunction<Iterator<Tuple2<String, String[]>>>() {
                @Override
                public void call(Iterator<Tuple2<String, String[]>> tuple2Iterator) throws Exception {
                    String t = "temp";
                    String t2[] = new String[10];
                    Tuple2<String, String[]> temp = new Tuple2<String, String[]>(t, t2);
                    Tuple2<String, String[]> temp2 = new Tuple2<String, String[]>(t, t2);
                    Tuple2<String, String[]> temp3 = new Tuple2<String, String[]>(t, t2);


                    while (tuple2Iterator.hasNext()) {
                        if (temp._1().equals(t)) {
                            temp = tuple2Iterator.next();
                        } else if (temp2._1().equals(t)) {
                            temp2 = tuple2Iterator.next();
                        } else {
                            temp3 = tuple2Iterator.next();
                            System.out.println("window:");
                            System.out.println(temp._1() + " , " + temp._1());
                            System.out.println(temp._1() + " , " + temp2._1());
                            System.out.println(temp._1() + " , " + temp3._1());

                            if (temp._2()[5].equals(temp._2()[5])) {
                                System.out.println(temp._2()[5] + "  " + temp._2()[5]);
                            }
                            if (temp._2()[5].equals(temp2._2()[5])) {
                                System.out.println(temp._2()[5] + "  " + temp2._2()[5]);
                            }
                            if (temp._2()[5].equals(temp3._2()[5])) {
                                System.out.println(temp._2()[5] + "  " + temp3._2()[5]);
                            }
                            temp = temp2;
                            temp2 = temp3;
                        }
                    }
                }
            });
        }



        JavaRDD<Tuple2<String, String[]>> bingo = context.parallelize(tup_arr);

        JavaPairRDD<String, String[]> grad;
        if(!flag_repart) {
            grad = bingo.mapToPair(EXTRA_MR).partitionBy(partitioner).persist(StorageLevel.MEMORY_ONLY());
        }
        else
        {
            grad = bingo.mapToPair(EXTRA_MR_PART).partitionBy(partitioner).persist(StorageLevel.MEMORY_ONLY());
        }
        System.out.println("MAPREDUCE2");
        System.out.println("count : " + grad.partitions().size());
        grad.foreachPartition(new VoidFunction<Iterator<Tuple2<String, String[]>>>() {
            @Override
            public void call(Iterator<Tuple2<String, String[]>> tuple2Iterator) throws Exception {
                String t = "temp";
                String t2[] = new String[10];
                Tuple2<String, String[]> temp = new Tuple2<String, String[]>(t, t2);
                Tuple2<String, String[]> temp2 = new Tuple2<String, String[]>(t, t2);
                Tuple2<String, String[]> temp3 = new Tuple2<String, String[]>(t, t2);


                while (tuple2Iterator.hasNext()) {
                    if (temp._1().equals(t)) {
                        temp = tuple2Iterator.next();
                    } else if (temp2._1().equals(t)) {
                        temp2 = tuple2Iterator.next();
                    } else {
                        temp3 = tuple2Iterator.next();
                        System.out.println("window:");
                        System.out.println(temp._1() + " , " + temp._1());
                        System.out.println(temp._1() + " , " + temp2._1());
                        System.out.println(temp._1() + " , " + temp3._1());

                        if (temp._2()[5].equals(temp._2()[5])) {
                            System.out.println(temp._2()[5] + "  " + temp._2()[5]);
                        }
                        if (temp._2()[5].equals(temp2._2()[5])) {
                            System.out.println(temp._2()[5] + "  " + temp2._2()[5]);
                        }
                        if (temp._2()[5].equals(temp3._2()[5])) {
                            System.out.println(temp._2()[5] + "  " + temp3._2()[5]);
                        }
                        temp = temp2;
                        temp2 = temp3;
                    }
                }
            }
        });



        context.stop();
    }
}

