package site.bigdataresource.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkJobInfo;
import org.apache.spark.SparkStageInfo;
import org.apache.spark.api.java.JavaFutureAction;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;
import scala.Int;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * 功能：实现追踪Spark的状态
 * Created by deqiangqin@gmail.com on 9/21/17.
 */
public class JavaSparkStatusTracker {

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        if (args.length < 1) {
            System.err.println("Usage:JavaSparkStatusTracker ");
            System.exit(1);
        }

        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("site.bigdataresource.spark.JavaSparkStatusTracker");
        if (args[0].toLowerCase().equals("n")) {
            sparkConf.setMaster("local[2]");
        }

        SparkSession spark = SparkSession.builder().config(sparkConf).getOrCreate();
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

        //通过运行一个简单的JOB实现进度报告

        //总的Task数是由原始数据的分片数(num slices)决定的
        JavaRDD<Integer> list = jsc.parallelize(Arrays.asList(1, 2, 3, 4, 5), 3);
        JavaRDD<Integer> rdd = list.map(new Function<Integer, Integer>() {
            public Integer call(Integer x) throws Exception {
                //每2秒返回当前的数值
                Thread.sleep(2000);
                return x;
            }
        });


        //状态获取,只有当执行Action操作的时候才可以
        //执行action操作的时候，使用async异步的方式
        //注意：所有的异步方法的结果都是返回到Driver的，所以，最好该Job的执行结果足够小
        JavaFutureAction<List<Integer>> jobFuture = rdd.collectAsync();
        while (!jobFuture.isDone()) {
            Thread.sleep(1000);
            List<Integer> jobIds = jobFuture.jobIds();
            if (jobIds.isEmpty()) {
                //有可能任务还没有开始
                System.out.println();
                continue;
            }
            //TODO: JobInfo与StageInfo的关系

            //获取JobId
            Integer curJobId = jobIds.get(jobIds.size() - 1);
            for (Integer curId : jobIds)
                System.out.println("curId=" + curId + "   jobIdsSize=" + jobIds.size());
            //通过JobId获取Job的信息
            SparkJobInfo jobInfo = jsc.statusTracker().getJobInfo(curJobId);
            SparkStageInfo stageInfo = jsc.statusTracker().getStageInfo(jobInfo.stageIds()[0]);
            System.out.println(stageInfo.numTasks() + "task total: " + stageInfo.numActiveTasks()
                    + " active, " + stageInfo.numCompletedTasks() + " complete");


        }

        //获取Job的执行结果
        System.out.println("Job resutls are :" + jobFuture.get());
        spark.stop();


    }
}
