package bench;

import static java.lang.Math.abs;
import static java.lang.Math.round;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.lang.instrument.*;

import org.apache.commons.math3.distribution.ZipfDistribution;
import org.apache.datasketches.frequencies.LongsSketch;

import com.clearspring.analytics.stream.frequency.CountMinSketch;




public class accuracyBench {
  
  
 
  
  public static void main(String[] args) {
    // memory ~ 2 * (2/eps)
    // giving same memory to both
    int mapSize = 65536	;
  
    double tot = 500000;
    class test {
      List<Double> errorCms;
      List<Double> errorFc;
      public test(){
        errorCms = new ArrayList<>();
        errorFc = new ArrayList<>();
      }
      
      public double getAvgErrorCms(){
        if(errorCms.size()==0) return 0.0;
        
        return errorCms.stream().mapToDouble(d->d).average().orElse(0.0)/(double) errorCms.size() ;
      }
  
      public double getAvgErrorFc(){
        if(errorFc.size()==0) return 0.0;
  
        return errorFc.stream().mapToDouble(d->d).average().orElse(0.0)/(double) errorFc.size();
      }
      
    }
    Map<Integer,test> ndvMap = new HashMap<>();
    for (int ndv = 1; ndv < tot / 100; ndv += 25) {
      test test  = new test();
      ndvBench.TestCase<Long> testCase = ndvBench.TestCase.getDefaultLongData(ndv, (int) round(tot / ndv));
      Set<Long> a = testCase.getA();
      Map<Long,Long> freq = testCase.getFreq();
      CountMinSketch cms = new CountMinSketch(0.001D,0.99D,1);
      LongsSketch fc = new LongsSketch(mapSize);
      
      
      for(Long elem: a){
        for(int f = 0; f<freq.get(elem); f++){
          cms.add(elem,1);
          fc.update(elem);
        }
      }
  
      for(Long elem: a){
          double cmsE = (abs(cms.estimateCount(elem)-freq.get(elem))/freq.get(elem))*100d;
          double fcE = (abs(fc.getEstimate(elem)-freq.get(elem))/freq.get(elem))*100d;
          test.errorCms.add(cmsE);
          test.errorFc.add(fcE);
      }
      
      ndvMap.put( ndv,test);
      System.out.println(String.format("%d",ndv));
      
    }
    String eol = System.getProperty("line.separator");
  
    try (Writer writer = new FileWriter("accuracyBench.csv")) {
      writer
        .append("Ndv")
        .append(',')
        .append("AvgErrorCms")
        .append(',')
        .append("AvgErrorFc");
    
      for (Map.Entry<Integer, test> ndvEntry : ndvMap.entrySet()) {
            writer.append(ndvEntry.getKey().toString())
              .append(',')
              .append(String.valueOf(ndvEntry.getValue().getAvgErrorCms()))
              .append(',')
              .append(String.valueOf(ndvEntry.getValue().getAvgErrorFc()))
              .append(eol);
      }
      
      
    } catch (IOException ex) {
      ex.printStackTrace(System.err);
    }
    
  }
  
}
