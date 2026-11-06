import org.rocksdb.*;
import java.util.*;

public class Bench {
    public static void main(String[] args) throws Exception {
        RocksDB.loadLibrary();
        String path = args[0];
        int n = 100_000;
        byte[] val = new byte[256];
        new Random(1).nextBytes(val);
        try (Options o = new Options().setCreateIfMissing(true);
             RocksDB db = RocksDB.open(o, path);
             WriteOptions w = new WriteOptions();
             ReadOptions r = new ReadOptions()) {
            // JIT·캐시 예열
            for (int i = 0; i < 20_000; i++) db.put(w, ("w" + i).getBytes(), val);
            for (int i = 0; i < 20_000; i++) db.get(r, ("w" + i).getBytes());

            long[] t = new long[n];
            for (int i = 0; i < n; i++) {
                byte[] k = ("k" + i).getBytes();
                long s = System.nanoTime();
                db.put(w, k, val);
                t[i] = System.nanoTime() - s;
            }
            report("RocksDB put (WAL 기록, fsync 안 함)", t);

            Random rnd = new Random(2);
            for (int i = 0; i < n; i++) {
                byte[] k = ("k" + rnd.nextInt(n)).getBytes();
                long s = System.nanoTime();
                db.get(r, k);
                t[i] = System.nanoTime() - s;
            }
            report("RocksDB get (memtable)", t);

            try (FlushOptions f = new FlushOptions().setWaitForFlush(true)) {
                db.flush(f);
            }
            for (int i = 0; i < n; i++) {
                byte[] k = ("k" + rnd.nextInt(n)).getBytes();
                long s = System.nanoTime();
                db.get(r, k);
                t[i] = System.nanoTime() - s;
            }
            report("RocksDB get (디스크 파일, 캐시 적중)", t);
        }
    }

    static void report(String name, long[] t) {
        long[] c = t.clone();
        Arrays.sort(c);
        double avg = Arrays.stream(c).average().orElse(0) / 1000.0;
        System.out.printf("%s | avg %.2f us | p50 %.2f us | p99 %.2f us%n",
                name, avg, c[c.length / 2] / 1000.0, c[(int) (c.length * 0.99)] / 1000.0);
    }
}
