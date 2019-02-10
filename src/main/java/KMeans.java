import java.io.*;
import java.net.URI;
import java.util.Scanner;
import java.util.Vector;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


class Point implements WritableComparable {
    public double x;
    public double y;
    
    Point(){} 
    
    Point(double X, double Y){
       x = X;
       y = Y;
    }
    
    public void write ( DataOutput out ) throws IOException {
        out.writeDouble(x);
        out.writeDouble(y);
    }

    public void readFields ( DataInput in ) throws IOException {
        x = in.readDouble();
        y = in.readDouble();
    }

	public int compareTo(Object c) {
    	double valueX= ((Point)c).x;
    	double valueY= ((Point)c).y;
    	
    	if ( this.x == valueX ) {
    		if (this.y == valueY)
    			return 0;
    		else if (this.y < valueY)
    			return 1;
    		else 
    			return -1;
    	}
    	else if (this.x < valueX)
			return 1;
    	else 
    		return -1;
	}

	public String toString(){
		return x+","+y;
	}
}


public class KMeans {
    static Vector<Point> centroids = new Vector<Point>(100);

    public static class AvgMapper extends Mapper<Object, Text,Point,Point> {
    	
    	protected void setup(Context context) 
    			throws IOException, InterruptedException {
            URI[] paths = context.getCacheFiles();
        	Configuration conf = context.getConfiguration();
        	FileSystem fs = FileSystem.get(conf);
        	BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(paths[0]))));
        	
        	String line = null;
        	
        	while((line = reader.readLine()) != null) {
	        	Scanner s = new Scanner(line.toString()).useDelimiter(",");
	            double x = s.nextDouble();
	            double y = s.nextDouble();
	            Point p = new Point(x, y);	        	
	        	centroids.add(p);
	        	s.close();
        	}
        	   	
        }
    	
    	public void map(Object key, Text value, Context context)throws IOException, InterruptedException{
    		Scanner s = new Scanner(value.toString()).useDelimiter(",");
    		double x = s.nextDouble();
            double y = s.nextDouble();
            Point p = new Point(x,y);
            
            double distance = 0;
            double min = 50.99999999999999999f;
            int newCentroid = -30;
            Iterator<Point> it = centroids.iterator();
            int i =0;
            while (it.hasNext()) {

				Point centroid = it.next();
            	
	            double a = Math.abs((p.x -centroid.x )*(p.x-centroid.x));
            	double b = Math.abs((p.y - centroid.y)*(p.y-centroid.y));
	            distance = Math.sqrt(a+b);
		  	    if ( distance < min ) {
		  	    	min = distance;
		  	    	newCentroid=i;
				}
				i++;
            }
			//context.write(null , new Point(centroids.get(newCentroid).x, centroids.get(newCentroid).y ));
			context.write(centroids.get(newCentroid),p);
			// System.out.println("newCentroid is: ", centroids.get(newCentroid));
        }
   }
    	

    public static class AvgReducer extends Reducer<Point,Point,Point,Object> {
    	public void reduce ( Point key, Iterable <Point> values,Context context )
    			throws IOException, InterruptedException {
    		int count = 0;
    		float sumx = 0.0f;
    		float sumy = 0.0f;
    		
    		for(Point p: values ) {
    			count++;
    			
    			
    			sumx += p.x;
    			sumy += p.y;   			
    		}
    		key.x= sumx/count;
    		key.y= sumy/count;
    		context.write(key,null);
    }
    }

    public static void main ( String[] args ) throws Exception {
        Job job = Job.getInstance();
        job.setJobName("MykMeansJob");
        job.setJarByClass(KMeans.class);
        job.addCacheFile(new URI(args[1])); 
        
        job.setOutputKeyClass(Point.class);
        job.setOutputValueClass(Point.class);
        
        job.setMapOutputKeyClass(Point.class);
        job.setMapOutputValueClass(Point.class);
        
        job.setMapperClass(AvgMapper.class);
        job.setReducerClass(AvgReducer.class);
        
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);	
        
        FileInputFormat.setInputPaths(job,new Path(args[0]));
//        FileInputFormat.setInputPaths(job,new Path(args[1]));
        FileOutputFormat.setOutputPath(job,new Path(args[2]));
        
        
        job.waitForCompletion(true);
    }
}

