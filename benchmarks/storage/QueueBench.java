import org.rocksdb.*;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * SQS 형태의 큐 부하를 RocksDB에 걸어 본다.
 * - 메시지 본문: 'm' + queue(4) + seq(8) -> 256B
 * - 받을 수 있는 시각 색인: 'v' + queue(4) + visibleAt(8) + seq(8) -> 빈 값
 * - 받기: 색인 앞에서 10건 찾기 -> 색인을 lease 만료 시각으로 옮김(삭제+쓰기)
 * - 처리 완료: 본문 삭제 + 색인 삭제
 * 처리가 밀린 상태(미리 쌓아 둔 메시지)를 비우는 속도를 잰다.
 */
public class QueueBench {
    static final int BATCH = 10;
    static final long LEASE_MS = 30_000;
    static final byte[] BODY = new byte[256];
    static final byte[] EMPTY = new byte[0];

    static byte[] mKey(int q, long seq) {
        return ByteBuffer.allocate(13).put((byte) 'm').putInt(q).putLong(seq).array();
    }

    static byte[] vKey(int q, long vis, long seq) {
        return ByteBuffer.allocate(21).put((byte) 'v').putInt(q).putLong(vis).putLong(seq).array();
    }

    static byte[] vBound(int q, long vis) {
        return ByteBuffer.allocate(13).put((byte) 'v').putInt(q).putLong(vis).array();
    }

    public static void main(String[] args) throws Exception {
        RocksDB.loadLibrary();
        new Random(1).nextBytes(BODY);
        String base = args[0];
        run(base + "/a1", "A 큐 1000개 x 1000건 | 매번 처음부터 찾기 | 1스레드", 1000, 1000, false, 1, 60);
        run(base + "/a2", "A 큐 1000개 x 1000건 | 시작 위치 기록 | 1스레드", 1000, 1000, true, 1, 60);
        run(base + "/a3", "A 큐 1000개 x 1000건 | 시작 위치 기록 | 8스레드", 1000, 1000, true, 8, 60);
        run(base + "/b1", "B 큐 1개 x 20만건 | 매번 처음부터 찾기 | 1스레드", 1, 200_000, false, 1, 15);
        run(base + "/b2", "B 큐 1개 x 20만건 | 시작 위치 기록 | 1스레드", 1, 200_000, true, 1, 15);
    }

    static void run(String path, String name, int queues, int perQueue, boolean useMark,
                    int threads, int capSec) throws Exception {
        deleteDir(Paths.get(path));
        Files.createDirectories(Paths.get(path).toAbsolutePath().getParent());
        try (Options o = new Options().setCreateIfMissing(true).setIncreaseParallelism(8);
             RocksDB db = RocksDB.open(o, path);
             WriteOptions w = new WriteOptions()) {

            long t0 = System.currentTimeMillis();
            long seq = 0;
            long ps = System.nanoTime();
            try (WriteBatch b = new WriteBatch()) {
                for (int q = 0; q < queues; q++) {
                    for (int i = 0; i < perQueue; i++) {
                        b.put(mKey(q, seq), BODY);
                        b.put(vKey(q, t0, seq), EMPTY);
                        seq++;
                        if (b.count() >= 2000) {
                            db.write(w, b);
                            b.clear();
                        }
                    }
                }
                if (b.count() > 0) db.write(w, b);
            }
            double preloadSec = (System.nanoTime() - ps) / 1e9;
            long total = (long) queues * perQueue;

            long[] mark = new long[queues];
            AtomicLong drained = new AtomicLong();
            long deadline = System.nanoTime() + capSec * 1_000_000_000L;
            ExecutorService ex = Executors.newFixedThreadPool(threads);
            List<Future<long[]>> fs = new ArrayList<>();
            long ds = System.nanoTime();
            for (int t = 0; t < threads; t++) {
                final int tid = t;
                fs.add(ex.submit(() -> drain(db, w, tid, threads, queues, useMark, mark, drained, deadline)));
            }
            List<long[]> parts = new ArrayList<>();
            for (Future<long[]> f : fs) parts.add(f.get());
            double drainSec = (System.nanoTime() - ds) / 1e9;
            ex.shutdown();

            int n = parts.stream().mapToInt(p -> p.length).sum();
            long[] lat = new long[n];
            int off = 0;
            for (long[] p : parts) {
                System.arraycopy(p, 0, lat, off, p.length);
                off += p.length;
            }
            Arrays.sort(lat);
            double p50 = n == 0 ? 0 : lat[n / 2] / 1000.0;
            double p99 = n == 0 ? 0 : lat[(int) (n * 0.99)] / 1000.0;
            System.out.printf("%s | 쌓기 %.0f건/s | 비운 건수 %d/%d (%.1f초) | 처리 %.0f건/s | 받기 p50 %.1fus p99 %.1fus%n",
                    name, total / preloadSec, drained.get(), total, drainSec, drained.get() / drainSec, p50, p99);
        }
        deleteDir(Paths.get(path));
    }

    static long[] drain(RocksDB db, WriteOptions w, int tid, int threads, int queues, boolean useMark,
                        long[] mark, AtomicLong drained, long deadline) throws Exception {
        List<Integer> mine = new ArrayList<>();
        for (int q = tid; q < queues; q += threads) mine.add(q);
        boolean[] empty = new boolean[queues];
        long[] markSeq = new long[queues];
        int remaining = mine.size();
        long[] lat = new long[1 << 20];
        int n = 0;
        long[] vis = new long[BATCH];
        long[] sq = new long[BATCH];

        outer:
        while (remaining > 0) {
            for (int q : mine) {
                if (empty[q]) continue;
                if (System.nanoTime() > deadline) break outer;
                long now = System.currentTimeMillis();
                long s = System.nanoTime();
                int got = 0;
                byte[] lowKey = useMark ? vKey(q, mark[q], markSeq[q]) : vBound(q, 0);
                try (Slice lo = new Slice(lowKey);
                     Slice hi = new Slice(vBound(q, now + 1));
                     ReadOptions r = new ReadOptions().setIterateLowerBound(lo).setIterateUpperBound(hi);
                     RocksIterator it = db.newIterator(r)) {
                    it.seek(lowKey);
                    while (it.isValid() && got < BATCH) {
                        ByteBuffer k = ByteBuffer.wrap(it.key());
                        k.get();
                        k.getInt();
                        vis[got] = k.getLong();
                        sq[got] = k.getLong();
                        got++;
                        it.next();
                    }
                }
                if (got == 0) {
                    empty[q] = true;
                    remaining--;
                    continue;
                }
                // 새로 들어오는 색인은 항상 지금 이후 시각이므로, 처음 찾은 위치 앞은 모두 지워진 자리다.
                if (useMark) {
                    mark[q] = vis[0];
                    markSeq[q] = sq[0];
                }
                long leaseUntil = now + LEASE_MS;
                try (WriteBatch b = new WriteBatch()) {
                    for (int i = 0; i < got; i++) {
                        b.delete(vKey(q, vis[i], sq[i]));
                        b.put(vKey(q, leaseUntil, sq[i]), EMPTY);
                    }
                    db.write(w, b);
                }
                if (n < lat.length) lat[n++] = System.nanoTime() - s;
                try (WriteBatch b = new WriteBatch()) {
                    for (int i = 0; i < got; i++) {
                        b.delete(mKey(q, sq[i]));
                        b.delete(vKey(q, leaseUntil, sq[i]));
                    }
                    db.write(w, b);
                }
                drained.addAndGet(got);
            }
        }
        return Arrays.copyOf(lat, n);
    }

    static void deleteDir(Path p) throws Exception {
        if (!Files.exists(p)) return;
        try (var walk = Files.walk(p)) {
            walk.sorted(Comparator.reverseOrder()).forEach(x -> x.toFile().delete());
        }
    }
}
