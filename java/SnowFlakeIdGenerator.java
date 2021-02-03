
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.ThreadLocalRandom;

/**
 * 保留(1bit) + 时间(41bit) + 数据中心(5bit) + 工作节点(5bit) + 序列号(12bit)
 *
 * @author xuxinsheng
 * @since 2021-02-03
 */
public final class SnowFlakeIdGenerator {
    /**
     * 初始时间截 (2021-01-01)
     */
    private static final long INITIAL_TIME_STAMP = 1609430400000L;

    /**
     * 保留位数
     */
    private static final long UNUSED_BITS = 1L;
    /**
     * 时间所占的位数41
     */
    private static final long TIME_STAMP_BITS = 41L;
    /**
     * 数据标识id所占的位数
     */
    private static final long DATACENTER_ID_BITS = 5L;
    /**
     * 机器id所占的位数
     */
    private static final long WORKER_ID_BITS = 5L;
    /**
     * 序列在id中占的位数
     */
    private static final long SEQUENCE_BITS = 12L;
    /**
     * -1
     */
    private static final long NEGATIVE_ONE = -1L;
    /**
     * 支持的最大数据标识id，结果是31
     */
    private static final long MAX_DATACENTER_ID = ~(NEGATIVE_ONE << DATACENTER_ID_BITS);
    /**
     * 支持的最大机器id，结果是31 (这个移位算法可以很快的计算出几位二进制数所能表示的最大十进制数)
     */
    private static final long MAX_WORKER_ID = ~(NEGATIVE_ONE << WORKER_ID_BITS);
    /**
     * 机器ID的偏移量(12)
     */
    private static final long WORKERID_OFFSET = SEQUENCE_BITS;
    /**
     * 数据中心ID的偏移量(12+5)
     */
    private static final long DATACENTERID_OFFSET = SEQUENCE_BITS + WORKER_ID_BITS;
    /**
     * 时间截的偏移量(5+5+12)
     */
    private static final long TIMESTAMP_OFFSET = SEQUENCE_BITS + WORKER_ID_BITS + DATACENTER_ID_BITS;
    /**
     * 生成序列的掩码，这里为4095 (0b111111111111=0xfff=4095)
     */
    private static final long SEQUENCE_MASK = ~(NEGATIVE_ONE << SEQUENCE_BITS);
    /**
     * 工作节点ID(0~31)
     */
    private long workerId;
    /**
     * 数据中心ID(0~31)
     */
    private long datacenterId;
    /**
     * 毫秒内序列(0~4095)
     */
    private long sequence = 0L;
    /**
     * 上次生成ID的时间截
     */
    private long lastTimestamp = NEGATIVE_ONE;

    /**
     * 构造函数
     */
    private SnowFlakeIdGenerator() {
        this.datacenterId = getDatacenterId(MAX_DATACENTER_ID);
        this.workerId = getWorkerId(datacenterId, MAX_WORKER_ID);
    }

    /**
     * 构造函数
     *
     * @param datacenterId 数据中心ID (0~31)
     * @param workerId     工作ID (0~31)
     */
    private SnowFlakeIdGenerator(long datacenterId, long workerId) {
        if (datacenterId > MAX_DATACENTER_ID || datacenterId < 0) {
            throw new IllegalArgumentException(String.format("DataCenterID 不能大于 %d 或小于 0", MAX_DATACENTER_ID));
        }
        if (workerId > MAX_WORKER_ID || workerId < 0) {
            throw new IllegalArgumentException(String.format("WorkerID 不能大于 %d 或小于 0", MAX_WORKER_ID));
        }
        this.datacenterId = datacenterId;
        this.workerId = workerId;
    }

    /**
     * 一台机子只需要一个实例，以保证产生有序的、不重复的ID, 数据中心ID和工作ID可以考虑从配置文件或者数据库获取
     */
    private static volatile SnowFlakeIdGenerator SNOW_FLAKE_ID_GENERATOR;

    public static SnowFlakeIdGenerator getInstance() {
        if (SNOW_FLAKE_ID_GENERATOR == null) {
            synchronized (SnowFlakeIdGenerator.class) {
                if (SNOW_FLAKE_ID_GENERATOR == null) {
                    SNOW_FLAKE_ID_GENERATOR = new SnowFlakeIdGenerator();
                }
            }
        }
        return SNOW_FLAKE_ID_GENERATOR;
    }

    /**
     * 获取 WorkerId
     * 另一种方式：使用本机的ip之和作为workId的初始值，但是有些ip的和一样
     *
     * @param datacenterId 数据标识id
     * @param maxWorkerId  最大工作id
     * @return 获取工作id
     */
    protected static long getWorkerId(long datacenterId, long maxWorkerId) {
        StringBuilder mpid = new StringBuilder();
        mpid.append(datacenterId);
        String name = ManagementFactory.getRuntimeMXBean().getName();
        if (null != name && name.length() > 0) {
            /*
             * GET jvmPid
             */
            mpid.append(name.split("@")[0]);
        }
        /*
         * MAC + PID 的 hashcode 获取16个低位
         */
        final long mask = 0xffff;
        return (mpid.toString().hashCode() & mask) % (maxWorkerId + 1);
    }

    /**
     * 数据标识id部分
     *
     * @param maxDatacenterId 最大数据标识id
     * @return 数据标识id
     */
    protected static long getDatacenterId(long maxDatacenterId) {
        long id = 0L;
        try {
            InetAddress ip = InetAddress.getLocalHost();
            NetworkInterface network = NetworkInterface.getByInetAddress(ip);
            if (network == null) {
                id = 1L;
            } else {
                byte[] mac = network.getHardwareAddress();
                if (null != mac) {
                    final long lowMask = 0x000000FF;
                    final long highMask = 0x0000FF00;
                    id = ((lowMask & (long) mac[mac.length - 1])
                            | (highMask & (((long) mac[mac.length - 2]) << 8)))
                            >> 6;
                    id = id % (maxDatacenterId + 1);
                }
            }
        } catch (Exception e) {
            System.out.println("getDatacenterId: " + e.getMessage());
        }
        return id;
    }

    /**
     * 获得下一个ID (用同步锁保证线程安全)
     *
     * @return SnowflakeId
     */
    public synchronized long nextId() {
        long timestamp = timeGen();
        //如果当前时间小于上一次ID生成的时间戳，说明系统时钟回退过这个时候应当抛出异常
        if (timestamp < lastTimestamp) {
            long offset = lastTimestamp - timestamp;
            // 闰秒
            if (offset <= 5) {
                try {
                    wait(offset << 1); //时间偏差大小小于5ms，则等待两倍时间
                    timestamp = timeGen();
                    if (timestamp < lastTimestamp) {
                        throw new RuntimeException(String.format("Clock moved backwards.  Refusing to generate id for %d milliseconds", offset));
//                        this.workerId = (this.workerId + 1) % (MAX_WORKER_ID + 1);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            } else {
                throw new RuntimeException(String.format("Clock moved backwards.  Refusing to generate id for %d milliseconds", offset));
//                this.workerId = (this.workerId + 1) % (MAX_WORKER_ID + 1);
            }
            throw new RuntimeException("当前时间小于上一次记录的时间戳！");
        }
        //如果是同一时间生成的，则进行毫秒内序列
        if (lastTimestamp == timestamp) {
            sequence = (sequence + 1) & SEQUENCE_MASK;
            //sequence等于0说明毫秒内序列已经增长到最大值
            if (sequence == 0) {
                //阻塞到下一个毫秒,获得新的时间戳
                timestamp = tilNextMillis(lastTimestamp);
            }
        } else {
            // 时间戳改变，毫秒内序列重置
            // sequence = 0L 会产生大量偶数，如果直接应用该ID来做分库分表，则极有可能出现数据不均匀的情况
            // 解决办法：1、自增ID满值后再重置
            // 2、重置时使用范围随机(重置随机为0~9)
            sequence = ThreadLocalRandom.current().nextLong(0, 9);
            // 3、生成ID的时候把序列号部分尾数用时间戳对应的位置覆盖
            // sequence = 127 & timestamp
        }
        //上次生成ID的时间截
        lastTimestamp = timestamp;
        //移位并通过或运算拼到一起组成64位的ID
        // 时间戳部分 | 数据中心部分 | 机器标识部分 | 序列号部分
        return ((timestamp - INITIAL_TIME_STAMP) << TIMESTAMP_OFFSET)
                | (datacenterId << DATACENTERID_OFFSET)
                | (workerId << WORKERID_OFFSET)
                | sequence;
    }

    /**
     * 获得下一个ID字符串
     *
     * @return ID字符串
     */
    public String nextIdStr() {
        return String.valueOf(nextId());
    }

    /**
     * 阻塞到下一个毫秒，直到获得新的时间戳
     *
     * @param lastTimestamp 上次生成ID的时间截
     * @return 当前时间戳
     */
    protected long tilNextMillis(long lastTimestamp) {
        long timestamp = timeGen();
        while (timestamp <= lastTimestamp) {
            timestamp = timeGen();
        }
        return timestamp;
    }

    protected long timeGen() {
        // 高并发场景下System.currentTimeMillis()的性能问题的优化
        return SystemClock.now();
//        return System.currentTimeMillis();
    }

    public long[] parseId(long id) {
        long[] arr = new long[5];
        arr[4] = ((id & diode(UNUSED_BITS, TIME_STAMP_BITS)) >> TIMESTAMP_OFFSET);
        arr[0] = arr[4] + INITIAL_TIME_STAMP;
        arr[1] = (id & diode(UNUSED_BITS + TIME_STAMP_BITS, DATACENTER_ID_BITS)) >> DATACENTERID_OFFSET;
        arr[2] = (id & diode(UNUSED_BITS + TIME_STAMP_BITS + DATACENTER_ID_BITS, WORKER_ID_BITS)) >> WORKERID_OFFSET;
        arr[3] = (id & diode(UNUSED_BITS + TIME_STAMP_BITS + DATACENTER_ID_BITS + WORKER_ID_BITS, SEQUENCE_BITS));
        return arr;
    }

    public String formatId(long id) {
        long[] arr = parseId(id);
        Date date = new Date(arr[0]);
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        String tmf = formatter.format(date);
        return String.format("%s, @(%d,%d), #%d", tmf, arr[1], arr[2], arr[3]);
    }

    /**
     * a diode is a long value whose left and right margin are ZERO, while
     * middle bits are ONE in binary string layout. it looks like a diode in
     * shape.
     *
     * @param offset left margin position
     * @param length offset+length is right margin position
     * @return a long value
     */
    private long diode(long offset, long length) {
        final int max = 64;
        final long n = -1L;
        int lb = (int) (max - offset);
        int rb = (int) (max - (offset + length));
        return (n << lb) ^ (n << rb);
    }

    public long customId(long currentTimeMillis, int datacenterId, int workerId, int sequence) {
        // 时间戳部分 | 数据中心部分 | 机器标识部分 | 序列号部分
        return ((currentTimeMillis - INITIAL_TIME_STAMP) << TIMESTAMP_OFFSET)
                | (datacenterId << DATACENTERID_OFFSET)
                | (workerId << WORKERID_OFFSET)
                | sequence;
    }

    @Override
    public String toString() {
        return "Snowflake Settings [timestampBits=" + TIME_STAMP_BITS + ", datacenterIdBits=" + DATACENTER_ID_BITS
                + ", workerIdBits=" + WORKER_ID_BITS + ", sequenceBits=" + SEQUENCE_BITS + ", initialTimeStamp=" + INITIAL_TIME_STAMP
                + ", datacenterId=" + datacenterId + ", workerId=" + workerId + "]";
    }

    public static void main(String[] args) {
        System.out.println(SnowFlakeIdGenerator.getInstance().nextId());
    }
}
