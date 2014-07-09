import com.hazelcast.config.Config;
import com.hazelcast.config.ExecutorConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.IMap;

import java.io.Serializable;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 */
public class Test5 {

    public static final Logger logger = Logger.getLogger(Test5.class.getName());

    public static final String EXECUTOR_NAME = "exe";
    public static final String MAP1_NAME = "map1";
    public static final String MAP2_NAME = "map2";

    public static final int MAP_SIZE = 1000;
    public static final int TASK_QUANTITY = 100000;

    public static final AtomicInteger doneCounter = new AtomicInteger(0);
    public static final AtomicInteger hz1DoneCounter = new AtomicInteger(0);
    public static final AtomicInteger hz2DoneCounter = new AtomicInteger(0);

    public static class RunnableTask implements Runnable, Serializable, HazelcastInstanceAware {

        private HazelcastInstance hazelcastInstance;

        private int key = -1;

        public RunnableTask(int key) {
            this.key = key;
        }

        @Override
        public void run() {

            IMap<Integer, Integer> map1 = hazelcastInstance.getMap(MAP1_NAME);
            IMap<Integer, Integer> map2 = hazelcastInstance.getMap(MAP1_NAME);

            map1.lock(key);

            try {

                int value1 = map1.get(key);
                int value2 = map2.get(key);

                int result = value1 + value2;

                map1.put(key, result);
                map2.put(key, result);

            } finally {
                map1.unlock(key);
            }

            doneCounter.incrementAndGet();

            if (hazelcastInstance.getConfig().getInstanceName().equals("1")) {
                hz1DoneCounter.incrementAndGet();
            } else {
                hz2DoneCounter.incrementAndGet();
            }


            try {
                TimeUnit.MILLISECONDS.sleep(10L);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            this.hazelcastInstance = hazelcastInstance;
        }
    }

    public static void main(String[] args) throws InterruptedException {

        try {
            run();
        } finally {
            Hazelcast.shutdownAll();
        }
    }

    private static Config createConfig(String name) {
        Config config = new Config(name);

        ExecutorConfig executorConfig = config.getExecutorConfig(EXECUTOR_NAME);
        executorConfig.setPoolSize(10);

        // map without backup

        MapConfig mapConfig1 = config.getMapConfig(MAP1_NAME);
        mapConfig1.setBackupCount(0);

        MapConfig mapConfig2 = config.getMapConfig(MAP2_NAME);
        mapConfig2.setBackupCount(0);

        return config;
    }

    private static void run() throws InterruptedException {

        HazelcastInstance hzInstance1 = Hazelcast.newHazelcastInstance(createConfig("1"));
        HazelcastInstance hzInstance2 = Hazelcast.newHazelcastInstance(createConfig("2"));

        Random random = new Random();

        // map filling
        IMap<Integer, Integer> map1 = hzInstance1.getMap(MAP1_NAME);
        for (int i = 0; i < MAP_SIZE; i++) {
            map1.put(i, random.nextInt(MAP_SIZE));
        }
        IMap<Integer, Integer> map2 = hzInstance1.getMap(MAP1_NAME);
        for (int i = 0; i < MAP_SIZE; i++) {
            map2.put(i, random.nextInt(MAP_SIZE));
        }

        // run tasks
        IExecutorService executorService = hzInstance1.getExecutorService(EXECUTOR_NAME);

        long startTime = System.currentTimeMillis();

        int i = 0;
        while (i++ < TASK_QUANTITY) {

            if (i % 100 == 0) {
                TimeUnit.MILLISECONDS.sleep(11L);
            }

            if (i % 1000 == 0) {
                logger.info(Integer.toString(i) + "\t" + doneCounter.intValue() + "\t  sec: " + String.format("%8.3f",
                        (double) (System.currentTimeMillis() -
                                startTime) / 1000));
            }

            int key = key = random.nextInt(MAP_SIZE);
            RunnableTask task = new RunnableTask(key);

            executorService.executeOnKeyOwner(new RunnableTask(key), key);
        }

        // wait until finish the freeze
        int oldDoneCounterValue = 0;
        int diff = -1;
        while (doneCounter.intValue() != TASK_QUANTITY) {

            diff = oldDoneCounterValue - doneCounter.intValue();
            if (diff == 0) {
                break;
            }

            oldDoneCounterValue = doneCounter.intValue();

            TimeUnit.MILLISECONDS.sleep(10L);
        }

        logger.info("Done tasks: hz1: " + hz1DoneCounter.intValue() + " hz2: " + hz2DoneCounter.intValue() +
                " sec: " + String.format("%8.3f", (double) (System.currentTimeMillis() - startTime) / 1000));
    }

}


