
import static java.lang.Math.round;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.commons.math3.distribution.ZipfDistribution;
import org.apache.datasketches.frequencies.LongsSketch;

import com.clearspring.analytics.stream.frequency.CountMinSketch;
import com.google.common.math.DoubleMath;


public class bench {
  
    public static void main(String[] args) {
      int tot  = 100000000;
      int epoch = Integer.parseInt(args[0]);
      ZipfDistribution zipfDistribution = new ZipfDistribution(tot,Double.parseDouble(args[1]));
      List<Integer> l = new ArrayList<>();
      CountMinSketch cms = new CountMinSketch(0.001D,0.99D,1);
      int mapSize = 1024;
      LongsSketch fc = new LongsSketch(mapSize);
      for(int i=0; i < epoch; i++){
          int x = zipfDistribution.sample();
          long count = round(zipfDistribution.probability(x)*tot);
          l.add(x);
          cms.add(x,count);
          fc.update(x,count);
//          System.out.println(String.format("%d %d",x,count));
      }
      
      List<Double> cmsErrorList = new ArrayList<>();
      List<Double> fcErrorList = new ArrayList<>();
      for(int x : l){
        long count = round(zipfDistribution.probability(x)*tot);
        if(count == 0){
          continue;
        }
        double CmsError = ((double)Math.abs(count - cms.estimateCount(x))*100D)/(double) count;
        double fcError = ((double)Math.abs(count - fc.getEstimate(x))*100D)/(double) count;
        cmsErrorList.add(CmsError);
        fcErrorList.add(fcError);
      }
      
      System.out.println(l);
      System.out.println("........................");
      System.out.println("........................");
      System.out.println(cmsErrorList);
      System.out.println("........................");
      System.out.println("........................");
      System.out.println(fcErrorList);
      
      
      
      
      System.out.println("Hello World");
    }
    
  //161723 21
}
