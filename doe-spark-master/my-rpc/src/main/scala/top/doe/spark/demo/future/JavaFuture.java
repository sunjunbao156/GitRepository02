package top.doe.spark.demo.future;

import java.util.concurrent.*;

public class JavaFuture {

    public static void main(String[] args) throws Exception {

        //创建一个线程池，只有一个线程对象
        ExecutorService pool = Executors.newSingleThreadExecutor();

        //向线程池提交一个Callable，开启一个子线程
        Future<Integer> future = pool.submit(new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                System.out.println("子线程：开始执行计算逻辑，需要运行一定时间...");
                Thread.sleep(2000); //模拟某个耗时操作 比如网络请求
                System.out.println("子线程：执行结束，将执行结果返回给Future");
                return 10;
            }
        });

        //立即判断future是否完成，并理解从future中取出结果
        boolean d1 = future.isDone();
        Integer r1 = null;
        try {
            r1 = future.get(0, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            //记录异常信息
        }
        System.out.println("主线程：future的状态："+ d1 +" future中的数据：" + r1);
        System.out.println("主线程：睡眠3秒...");
        Thread.sleep(3000);
        //睡眠后再次取出结果
        boolean d2 = future.isDone();
        Integer r2 = future.get();
        System.out.println("主线程：future的状态："+ d2 +" future中的数据：" + r2);
        //关闭主线程
        pool.shutdown();

    }
}
