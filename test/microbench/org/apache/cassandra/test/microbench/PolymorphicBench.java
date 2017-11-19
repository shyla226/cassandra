/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package org.apache.cassandra.test.microbench;

import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Microbenchmark to illustrate the super-cache bug that is present in all current
 * versions of the JVM and will be present at least up to Java 10 and is scheduled
 * for Java 11 (fall 2018): https://bugs.openjdk.java.net/browse/JDK-8180450.
 *
 * <b>Results from a Mac (single socket, 1x4 cores HT), 8 threads</b>
 * <code>
 [java] Benchmark                                               Mode      Cnt         Score    Error  Units
 [java] PolymorphicBench.baseline                             sample  2634095       223.211 ± 57.810  ns/op
 [java] PolymorphicBench.baseline:baseline·p0.00              sample                  1.000           ns/op
 [java] PolymorphicBench.baseline:baseline·p0.50              sample                120.000           ns/op
 [java] PolymorphicBench.baseline:baseline·p0.90              sample                140.000           ns/op
 [java] PolymorphicBench.baseline:baseline·p0.95              sample                149.000           ns/op
 [java] PolymorphicBench.baseline:baseline·p0.99              sample                201.000           ns/op
 [java] PolymorphicBench.baseline:baseline·p0.999             sample                590.000           ns/op
 [java] PolymorphicBench.baseline:baseline·p0.9999            sample              49232.998           ns/op
 [java] PolymorphicBench.baseline:baseline·p1.00              sample           31784960.000           ns/op
 [java] PolymorphicBench.monomorphic                          sample  2232234       310.038 ± 89.589  ns/op
 [java] PolymorphicBench.monomorphic:monomorphic·p0.00        sample                  1.000           ns/op
 [java] PolymorphicBench.monomorphic:monomorphic·p0.50        sample                124.000           ns/op
 [java] PolymorphicBench.monomorphic:monomorphic·p0.90        sample                138.000           ns/op
 [java] PolymorphicBench.monomorphic:monomorphic·p0.95        sample                148.000           ns/op
 [java] PolymorphicBench.monomorphic:monomorphic·p0.99        sample                219.000           ns/op
 [java] PolymorphicBench.monomorphic:monomorphic·p0.999       sample               2375.060           ns/op
 [java] PolymorphicBench.monomorphic:monomorphic·p0.9999      sample             114060.096           ns/op
 [java] PolymorphicBench.monomorphic:monomorphic·p1.00        sample           26869760.000           ns/op
 [java] PolymorphicBench.supercachebug                        sample  2379075       477.641 ± 71.519  ns/op
 [java] PolymorphicBench.supercachebug:supercachebug·p0.00    sample                  2.000           ns/op
 [java] PolymorphicBench.supercachebug:supercachebug·p0.50    sample                314.000           ns/op
 [java] PolymorphicBench.supercachebug:supercachebug·p0.90    sample                429.000           ns/op
 [java] PolymorphicBench.supercachebug:supercachebug·p0.95    sample                467.000           ns/op
 [java] PolymorphicBench.supercachebug:supercachebug·p0.99    sample                551.000           ns/op
 [java] PolymorphicBench.supercachebug:supercachebug·p0.999   sample               3380.000           ns/op
 [java] PolymorphicBench.supercachebug:supercachebug·p0.9999  sample             129443.482           ns/op
 [java] PolymorphicBench.supercachebug:supercachebug·p1.00    sample           23592960.000           ns/op
 * </code>
 * <b>Results from a Mac (single socket, 1x4 cores HT), 4 threads</b>
 * <code>
 [java] Benchmark                                               Mode      Cnt       Score   Error  Units
 [java] PolymorphicBench.baseline                             sample  1399629     102.838 ± 4.945  ns/op
 [java] PolymorphicBench.baseline:baseline·p0.00              sample                1.000          ns/op
 [java] PolymorphicBench.baseline:baseline·p0.50              sample               94.000          ns/op
 [java] PolymorphicBench.baseline:baseline·p0.90              sample              102.000          ns/op
 [java] PolymorphicBench.baseline:baseline·p0.95              sample              108.000          ns/op
 [java] PolymorphicBench.baseline:baseline·p0.99              sample              129.000          ns/op
 [java] PolymorphicBench.baseline:baseline·p0.999             sample              387.000          ns/op
 [java] PolymorphicBench.baseline:baseline·p0.9999            sample             8880.000          ns/op
 [java] PolymorphicBench.baseline:baseline·p1.00              sample           949248.000          ns/op
 [java] PolymorphicBench.monomorphic                          sample  1252881     106.910 ± 0.717  ns/op
 [java] PolymorphicBench.monomorphic:monomorphic·p0.00        sample                1.000          ns/op
 [java] PolymorphicBench.monomorphic:monomorphic·p0.50        sample               99.000          ns/op
 [java] PolymorphicBench.monomorphic:monomorphic·p0.90        sample              108.000          ns/op
 [java] PolymorphicBench.monomorphic:monomorphic·p0.95        sample              114.000          ns/op
 [java] PolymorphicBench.monomorphic:monomorphic·p0.99        sample              139.000          ns/op
 [java] PolymorphicBench.monomorphic:monomorphic·p0.999       sample              462.118          ns/op
 [java] PolymorphicBench.monomorphic:monomorphic·p0.9999      sample             9019.389          ns/op
 [java] PolymorphicBench.monomorphic:monomorphic·p1.00        sample           107904.000          ns/op
 [java] PolymorphicBench.supercachebug                        sample   807191     303.163 ± 6.501  ns/op
 [java] PolymorphicBench.supercachebug:supercachebug·p0.00    sample               11.000          ns/op
 [java] PolymorphicBench.supercachebug:supercachebug·p0.50    sample              280.000          ns/op
 [java] PolymorphicBench.supercachebug:supercachebug·p0.90    sample              384.000          ns/op
 [java] PolymorphicBench.supercachebug:supercachebug·p0.95    sample              418.000          ns/op
 [java] PolymorphicBench.supercachebug:supercachebug·p0.99    sample              493.000          ns/op
 [java] PolymorphicBench.supercachebug:supercachebug·p0.999   sample             5832.000          ns/op
 [java] PolymorphicBench.supercachebug:supercachebug·p0.9999  sample            12788.493          ns/op
 [java] PolymorphicBench.supercachebug:supercachebug·p1.00    sample           932864.000          ns/op
 * </code>
 *
 * <b>Results from PC (single socket, 1x8 cores HT), 16 threads</b>
 * <code>
 [java] Benchmark                                               Mode      Cnt         Score    Error  Units
 [java] PolymorphicBench.baseline                             sample  3998277        76.716 ± 28.061  ns/op
 [java] PolymorphicBench.baseline:baseline·p0.00              sample                 23.000           ns/op
 [java] PolymorphicBench.baseline:baseline·p0.50              sample                 60.000           ns/op
 [java] PolymorphicBench.baseline:baseline·p0.90              sample                 70.000           ns/op
 [java] PolymorphicBench.baseline:baseline·p0.95              sample                 73.000           ns/op
 [java] PolymorphicBench.baseline:baseline·p0.99              sample                 80.000           ns/op
 [java] PolymorphicBench.baseline:baseline·p0.999             sample                 90.000           ns/op
 [java] PolymorphicBench.baseline:baseline·p0.9999            sample               2505.378           ns/op
 [java] PolymorphicBench.baseline:baseline·p1.00              sample           23986176.000           ns/op
 [java] PolymorphicBench.monomorphic                          sample  4197630       111.679 ± 38.183  ns/op
 [java] PolymorphicBench.monomorphic:monomorphic·p0.00        sample                 24.000           ns/op
 [java] PolymorphicBench.monomorphic:monomorphic·p0.50        sample                 74.000           ns/op
 [java] PolymorphicBench.monomorphic:monomorphic·p0.90        sample                 83.000           ns/op
 [java] PolymorphicBench.monomorphic:monomorphic·p0.95        sample                 86.000           ns/op
 [java] PolymorphicBench.monomorphic:monomorphic·p0.99        sample                 92.000           ns/op
 [java] PolymorphicBench.monomorphic:monomorphic·p0.999       sample                120.000           ns/op
 [java] PolymorphicBench.monomorphic:monomorphic·p0.9999      sample               3047.790           ns/op
 [java] PolymorphicBench.monomorphic:monomorphic·p1.00        sample           19988480.000           ns/op
 [java] PolymorphicBench.supercachebug                        sample  4148748       782.701 ± 60.704  ns/op
 [java] PolymorphicBench.supercachebug:supercachebug·p0.00    sample                 74.000           ns/op
 [java] PolymorphicBench.supercachebug:supercachebug·p0.50    sample                629.000           ns/op
 [java] PolymorphicBench.supercachebug:supercachebug·p0.90    sample                976.000           ns/op
 [java] PolymorphicBench.supercachebug:supercachebug·p0.95    sample               1102.000           ns/op
 [java] PolymorphicBench.supercachebug:supercachebug·p0.99    sample               1406.000           ns/op
 [java] PolymorphicBench.supercachebug:supercachebug·p0.999   sample               2220.000           ns/op
 [java] PolymorphicBench.supercachebug:supercachebug·p0.9999  sample              10448.000           ns/op
 [java] PolymorphicBench.supercachebug:supercachebug·p1.00    sample           23986176.000           ns/op
 * </code>
 * <b>Results from PC (single socket, 1x8 cores HT), 8 threads</b>
 * <code>
 [java] Benchmark                                               Mode      Cnt       Score   Error  Units
 [java] PolymorphicBench.baseline                             sample  1955337      43.185 ± 0.240  ns/op
 [java] PolymorphicBench.baseline:baseline·p0.00              sample               23.000          ns/op
 [java] PolymorphicBench.baseline:baseline·p0.50              sample               40.000          ns/op
 [java] PolymorphicBench.baseline:baseline·p0.90              sample               53.000          ns/op
 [java] PolymorphicBench.baseline:baseline·p0.95              sample               63.000          ns/op
 [java] PolymorphicBench.baseline:baseline·p0.99              sample               82.000          ns/op
 [java] PolymorphicBench.baseline:baseline·p0.999             sample              111.000          ns/op
 [java] PolymorphicBench.baseline:baseline·p0.9999            sample             2101.324          ns/op
 [java] PolymorphicBench.baseline:baseline·p1.00              sample           100736.000          ns/op
 [java] PolymorphicBench.monomorphic                          sample  1938645      46.162 ± 0.097  ns/op
 [java] PolymorphicBench.monomorphic:monomorphic·p0.00        sample               23.000          ns/op
 [java] PolymorphicBench.monomorphic:monomorphic·p0.50        sample               48.000          ns/op
 [java] PolymorphicBench.monomorphic:monomorphic·p0.90        sample               54.000          ns/op
 [java] PolymorphicBench.monomorphic:monomorphic·p0.95        sample               57.000          ns/op
 [java] PolymorphicBench.monomorphic:monomorphic·p0.99        sample               70.000          ns/op
 [java] PolymorphicBench.monomorphic:monomorphic·p0.999       sample               93.000          ns/op
 [java] PolymorphicBench.monomorphic:monomorphic·p0.9999      sample             1645.625          ns/op
 [java] PolymorphicBench.monomorphic:monomorphic·p1.00        sample            13664.000          ns/op
 [java] PolymorphicBench.supercachebug                        sample  2431488     513.182 ± 0.793  ns/op
 [java] PolymorphicBench.supercachebug:supercachebug·p0.00    sample               80.000          ns/op
 [java] PolymorphicBench.supercachebug:supercachebug·p0.50    sample              471.000          ns/op
 [java] PolymorphicBench.supercachebug:supercachebug·p0.90    sample              780.000          ns/op
 [java] PolymorphicBench.supercachebug:supercachebug·p0.95    sample              898.000          ns/op
 [java] PolymorphicBench.supercachebug:supercachebug·p0.99    sample             1184.000          ns/op
 [java] PolymorphicBench.supercachebug:supercachebug·p0.999   sample             1862.000          ns/op
 [java] PolymorphicBench.supercachebug:supercachebug·p0.9999  sample             9341.618          ns/op
 [java] PolymorphicBench.supercachebug:supercachebug·p1.00    sample           321536.000          ns/op
 * </code>
 *
 * <b>Results from Ironic c4.8xlarge (2 sockets, 2x12 cores HT), 48 threads</b>
 * <code>
 [java] Benchmark                                               Mode       Cnt         Score    Error  Units
 [java] PolymorphicBench.baseline                             sample  13016711       100.169 ?  9.316  ns/op
 [java] PolymorphicBench.baseline:baseline?p0.00              sample                  40.000           ns/op
 [java] PolymorphicBench.baseline:baseline?p0.50              sample                  86.000           ns/op
 [java] PolymorphicBench.baseline:baseline?p0.90              sample                 104.000           ns/op
 [java] PolymorphicBench.baseline:baseline?p0.95              sample                 107.000           ns/op
 [java] PolymorphicBench.baseline:baseline?p0.99              sample                 114.000           ns/op
 [java] PolymorphicBench.baseline:baseline?p0.999             sample                 160.000           ns/op
 [java] PolymorphicBench.baseline:baseline?p0.9999            sample               16336.000           ns/op
 [java] PolymorphicBench.baseline:baseline?p1.00              sample            12025856.000           ns/op
 [java] PolymorphicBench.monomorphic                          sample  11614114       110.737 ?  7.524  ns/op
 [java] PolymorphicBench.monomorphic:monomorphic?p0.00        sample                  45.000           ns/op
 [java] PolymorphicBench.monomorphic:monomorphic?p0.50        sample                 101.000           ns/op
 [java] PolymorphicBench.monomorphic:monomorphic?p0.90        sample                 123.000           ns/op
 [java] PolymorphicBench.monomorphic:monomorphic?p0.95        sample                 127.000           ns/op
 [java] PolymorphicBench.monomorphic:monomorphic?p0.99        sample                 132.000           ns/op
 [java] PolymorphicBench.monomorphic:monomorphic?p0.999       sample                 144.000           ns/op
 [java] PolymorphicBench.monomorphic:monomorphic?p0.9999      sample               16832.000           ns/op
 [java] PolymorphicBench.monomorphic:monomorphic?p1.00        sample            12009472.000           ns/op
 [java] PolymorphicBench.supercachebug                        sample  10787053      8063.776 ? 39.744  ns/op
 [java] PolymorphicBench.supercachebug:supercachebug?p0.00    sample                 113.000           ns/op
 [java] PolymorphicBench.supercachebug:supercachebug?p0.50    sample                7512.000           ns/op
 [java] PolymorphicBench.supercachebug:supercachebug?p0.90    sample               12848.000           ns/op
 [java] PolymorphicBench.supercachebug:supercachebug?p0.95    sample               14656.000           ns/op
 [java] PolymorphicBench.supercachebug:supercachebug?p0.99    sample               18656.000           ns/op
 [java] PolymorphicBench.supercachebug:supercachebug?p0.999   sample               24992.000           ns/op
 [java] PolymorphicBench.supercachebug:supercachebug?p0.9999  sample               31776.000           ns/op
 [java] PolymorphicBench.supercachebug:supercachebug?p1.00    sample            35979264.000           ns/op
 * </code>
 * <b>Results from Ironic c4.8xlarge (2 sockets, 2x12 cores HT), 24 threads</b>
 * <code>
 [java] Benchmark                                               Mode      Cnt        Score   Error  Units
 [java] PolymorphicBench.baseline                             sample  6690102       70.726 ? 0.204  ns/op
 [java] PolymorphicBench.baseline:baseline?p0.00              sample                25.000          ns/op
 [java] PolymorphicBench.baseline:baseline?p0.50              sample                65.000          ns/op
 [java] PolymorphicBench.baseline:baseline?p0.90              sample                97.000          ns/op
 [java] PolymorphicBench.baseline:baseline?p0.95              sample               107.000          ns/op
 [java] PolymorphicBench.baseline:baseline?p0.99              sample               117.000          ns/op
 [java] PolymorphicBench.baseline:baseline?p0.999             sample               129.000          ns/op
 [java] PolymorphicBench.baseline:baseline?p0.9999            sample              8015.753          ns/op
 [java] PolymorphicBench.baseline:baseline?p1.00              sample             61056.000          ns/op
 [java] PolymorphicBench.monomorphic                          sample  6096732       67.355 ? 0.791  ns/op
 [java] PolymorphicBench.monomorphic:monomorphic?p0.00        sample                40.000          ns/op
 [java] PolymorphicBench.monomorphic:monomorphic?p0.50        sample                60.000          ns/op
 [java] PolymorphicBench.monomorphic:monomorphic?p0.90        sample                92.000          ns/op
 [java] PolymorphicBench.monomorphic:monomorphic?p0.95        sample               102.000          ns/op
 [java] PolymorphicBench.monomorphic:monomorphic?p0.99        sample               112.000          ns/op
 [java] PolymorphicBench.monomorphic:monomorphic?p0.999       sample               126.000          ns/op
 [java] PolymorphicBench.monomorphic:monomorphic?p0.9999      sample              9024.000          ns/op
 [java] PolymorphicBench.monomorphic:monomorphic?p1.00        sample            873472.000          ns/op
 [java] PolymorphicBench.supercachebug                        sample  5056776     6211.991 ? 7.234  ns/op
 [java] PolymorphicBench.supercachebug:supercachebug?p0.00    sample               155.000          ns/op
 [java] PolymorphicBench.supercachebug:supercachebug?p0.50    sample              5800.000          ns/op
 [java] PolymorphicBench.supercachebug:supercachebug?p0.90    sample             10048.000          ns/op
 [java] PolymorphicBench.supercachebug:supercachebug?p0.95    sample             11696.000          ns/op
 [java] PolymorphicBench.supercachebug:supercachebug?p0.99    sample             15872.000          ns/op
 [java] PolymorphicBench.supercachebug:supercachebug?p0.999   sample             21856.000          ns/op
 [java] PolymorphicBench.supercachebug:supercachebug?p0.9999  sample             28320.000          ns/op
 [java] PolymorphicBench.supercachebug:supercachebug?p1.00    sample           4079616.000          ns/op
 * </code>
 * <b>Results from Ironic c4.8xlarge (2 sockets, 2x12 cores HT), 12 threads</b>
 * <code>
 [java] Benchmark                                               Mode      Cnt        Score   Error  Units
 [java] PolymorphicBench.baseline                             sample  3166495       64.249 ? 0.622  ns/op
 [java] PolymorphicBench.baseline:baseline?p0.00              sample                25.000          ns/op
 [java] PolymorphicBench.baseline:baseline?p0.50              sample                65.000          ns/op
 [java] PolymorphicBench.baseline:baseline?p0.90              sample                72.000          ns/op
 [java] PolymorphicBench.baseline:baseline?p0.95              sample                82.000          ns/op
 [java] PolymorphicBench.baseline:baseline?p0.99              sample                97.000          ns/op
 [java] PolymorphicBench.baseline:baseline?p0.999             sample               117.000          ns/op
 [java] PolymorphicBench.baseline:baseline?p0.9999            sample              5048.000          ns/op
 [java] PolymorphicBench.baseline:baseline?p1.00              sample            559104.000          ns/op
 [java] PolymorphicBench.monomorphic                          sample  2812521       75.454 ? 0.249  ns/op
 [java] PolymorphicBench.monomorphic:monomorphic?p0.00        sample                38.000          ns/op
 [java] PolymorphicBench.monomorphic:monomorphic?p0.50        sample                86.000          ns/op
 [java] PolymorphicBench.monomorphic:monomorphic?p0.90        sample                93.000          ns/op
 [java] PolymorphicBench.monomorphic:monomorphic?p0.95        sample                95.000          ns/op
 [java] PolymorphicBench.monomorphic:monomorphic?p0.99        sample               120.000          ns/op
 [java] PolymorphicBench.monomorphic:monomorphic?p0.999       sample               132.000          ns/op
 [java] PolymorphicBench.monomorphic:monomorphic?p0.9999      sample              5904.000          ns/op
 [java] PolymorphicBench.monomorphic:monomorphic?p1.00        sample             37312.000          ns/op
 [java] PolymorphicBench.supercachebug                        sample  3408645     3548.664 ? 4.543  ns/op
 [java] PolymorphicBench.supercachebug:supercachebug?p0.00    sample               133.000          ns/op
 [java] PolymorphicBench.supercachebug:supercachebug?p0.50    sample              3272.000          ns/op
 [java] PolymorphicBench.supercachebug:supercachebug?p0.90    sample              6112.000          ns/op
 [java] PolymorphicBench.supercachebug:supercachebug?p0.95    sample              7136.000          ns/op
 [java] PolymorphicBench.supercachebug:supercachebug?p0.99    sample              9472.000          ns/op
 [java] PolymorphicBench.supercachebug:supercachebug?p0.999   sample             13456.000          ns/op
 [java] PolymorphicBench.supercachebug:supercachebug?p0.9999  sample             18080.000          ns/op
 [java] PolymorphicBench.supercachebug:supercachebug?p1.00    sample           1218560.000          ns/op
 * </code>
 */
@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 2)
@Fork(value = 1, jvmArgsAppend = { "-Xmx256M" })
@Threads(4) // make sure this matches the underlying hardware - run tests with different values, too
@State(Scope.Benchmark)
public class PolymorphicBench
{
    final I3 b = new B();
    final I3 c = new C();

    public static boolean foo(I3 i)
    {
        return i instanceof I1;
    }

    public static boolean goo(I3 i)
    {
        return i instanceof I2;
    }

    @Benchmark
    public void baseline(Blackhole bh)
    {
        bh.consume(true);
        bh.consume(true);
        bh.consume(true);
        bh.consume(true);
    }

    @Benchmark
    public void monomorphic(Blackhole bh)
    {
        bh.consume(foo(b));
        bh.consume(foo(b));
        bh.consume(foo(b));
        bh.consume(foo(b));
    }

    @Benchmark
    public void supercachebug(Blackhole bh)
    {
        bh.consume(foo(b));
        bh.consume(foo(c));
        bh.consume(goo(b));
        bh.consume(goo(c));
    }

    interface I1
    {
    }

    interface I2
    {
    }

    interface I3 extends I1, I2
    {
    }

    class B implements I3
    {
    }

    class C implements I3
    {
    }
}
