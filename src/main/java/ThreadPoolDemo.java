import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ThreadPoolDemo {

    public static void main(String[] args) {
        //单线程池
        //ExecutorService pool = Executors.newSingleThreadExecutor();
        //多线程池
       // ExecutorService pool = Executors.newFixedThreadPool(5);

        ExecutorService pool = Executors.newCachedThreadPool();
        for(int i=0;i<20;i++){
            pool.execute(new Runnable() {
                @Override
                public void run() {
                    System.out.println(Thread.currentThread().getName());
                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    System.out.println(Thread.currentThread().getName()+"is over");
                }
            });
        }

        System.out.println("all task is submitted");
        pool.shutdown();
    }
}
