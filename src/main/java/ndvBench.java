
import static java.lang.Math.round;

import java.io.FileWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import org.apache.commons.math3.distribution.ZipfDistribution;
import org.apache.datasketches.frequencies.ErrorType;
import org.apache.datasketches.frequencies.LongsSketch;

import com.clearspring.analytics.stream.frequency.CountMinSketch;

import it.unimi.dsi.fastutil.longs.LongArrays;


public class ndvBench {
  
  public static void main(String[] args) {
   
    
    
    class test {
      int actual;
      int expected;
      int falsePostive;
      
      public String toString() {
        return String.valueOf(actual)+", "+String.valueOf(expected) + ", "+String.valueOf(falsePostive);
      }
      
    }
  
    Map<Integer,Map<Integer,Map<Integer,test>>> sizeMap = new HashMap<>();
  
    
    for (int pow = 1;pow <15;pow++) {
      int mapSize = (int) Math.pow(2,pow);
      Map<Integer,Map<Integer,test>> ndvMap = new HashMap<>();
      //    int ndv = 50;
      double tot = 500000;
  
      for (int ndv = 1; ndv < tot / 100; ndv += 25) {
    
        Map<Integer, test> m = new HashMap<>();
        TestCase<Long> testCase = TestCase.getDefaultLongData(ndv, (int) round(tot / ndv));
        double n = testCase.getN();
        Set<Long> A = testCase.getA();
        Map<Long, Long> freq = testCase.getFreq();
        int epoch = 1000;
    
    
        for (int i = 1; i <= epoch; i++) {
          int k = (int) (round((double) n / epoch) * i);
          test test = new test();
          LongsSketch fc = new LongsSketch(mapSize);
          for (Long elem : A) {
            for (long j = 0; j < freq.get(elem); j++) {
              try {
                fc.update(elem);
              } catch (Exception e) {
                System.out.println(e.toString());
              }
            }
          }
      
          LongsSketch.Row[] r = fc.getFrequentItems(round(n / k), ErrorType.NO_FALSE_NEGATIVES);
          Set<Long> hh = (Set<Long>) ((Object) Arrays.stream(r).map(e -> e.getItem()).collect(Collectors.toSet()));
      
          for (Long elem : A) {
            // atleast n/k -
            if (freq.get(elem) >= n / (double) k) {
              test.expected++;
              if (!hh.contains(elem)) {
                //            System.out.println("Gere");
                //              System.out.println(String.format("Not Found %d",elem));
              } else {
                test.actual++;
              }
            }
          }
      
          for (Long elem : hh) {
            // atleast n/(2*k) -> in data
            if (freq.get(elem) == null || freq.get(elem) < n / (2 * (double) k)) {
              test.falsePostive++;
              //            throw new Exception(String.format("Element %d present in Heap has lesser frequency for k %d", elem, k));
            }
          }
          m.put(k, test);
          System.out.println(String.format("%d, %d, %d", mapSize, ndv, i));
        }
        ndvMap.put(ndv, m);
      }
      sizeMap.put(mapSize,ndvMap);
    }
    String eol = System.getProperty("line.separator");
    
    try (Writer writer = new FileWriter("somefile2.csv")) {
      writer.append("MapSize")
        .append(",")
        .append("Ndv")
        .append(',')
        .append("K")
        .append(',')
        .append("Actual")
        .append(',')
        .append("Expected")
        .append(',')
        .append("False Positive")
        .append(eol);
  
      for (Map.Entry<Integer, Map<Integer, Map<Integer,test>>> sizeEntry : sizeMap.entrySet()) {
        for (Map.Entry<Integer, Map<Integer, test>> ndvEntry : sizeEntry.getValue().entrySet()) {
          for (Map.Entry<Integer, test> innerEntry : ndvEntry.getValue().entrySet()) {
            writer.append(sizeEntry.getKey().toString()).append(',')
              .append(ndvEntry.getKey().toString()).append(',').append(innerEntry.getKey().toString()).append(',').append(String.valueOf(innerEntry.getValue().actual)).append(',').append(String.valueOf(innerEntry.getValue().expected)).append(',').append(String.valueOf(innerEntry.getValue().falsePostive)).append(eol);
          }
        }
      }
    } catch (IOException ex) {
      ex.printStackTrace(System.err);
    }
    
  }
  
  
  
  //      System.out.println("Hello World");
  
  
  static class TestCase<T extends Comparable<T>> {
    final Set<T> a;
    final Map<T, Long> freq;
    Long n;
    
    public TestCase(Set<T> a, Map<T, Long> freq, Long n) {
      this.a = a;
      this.freq = freq;
      this.n = n;
    }
    
    public TestCase(Set<T> a, Map<T, Long> freq) {
      this.a = a;
      this.freq = freq;
    }
    
    public Long getN() {
      return n;
    }
    
    public Map<T, Long> getFreq() {
      return freq;
    }
    
    public Set<T> getA() {
      return a;
    }
    
    @Override
    public String toString() {
      return "TestCase{" + "a=" + a.toString() + ", freq=" + freq.toString() + ", n=" + n.toString() + '}';
    }
    
    public static TestCase RandomTestCase() {
      Set<Long> a = new HashSet<Long>(Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 327L));
      Map<Long, Long> m = new HashMap<Long, Long>();
      for (Long elem : a) {
        Long random = (long) (Math.random() * 50 + 1);
        m.put(elem, random);
      }
      Long n = m.values().stream().reduce(0L, Long::sum);
      return new TestCase(a, m, n);
    }
    
    public static TestCase getDefaultLongData(int ndv, int freq) {
      //        Set<Long> a = new HashSet<Long>(Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 327L));
      Set<Long> a = new HashSet<>();
      Map<Long, Long> m = getLongMapData(ndv,freq, a);
      Long n = m.values().stream().reduce(0L, Long::sum);
      return new TestCase(a, m, n);
    }
    
    
    private static Map<Long, Long> getLongMapData(int size,int freq, Set<Long> a) {
      Map<Long, Long> f = new HashMap<Long, Long>();
      
      Random r = new Random();
      for (int i = 0; i < size; i++) {
        long k = r.nextInt(size);
        a.add(k);
        f.put(k, (long) (r.nextInt(freq)));
      }
      return f;
    }
    
  }
  
  //161723 21
}
