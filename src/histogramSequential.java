// CSC 369 Winter 2016
// Waylin Wang, Myron Zhao Lab7

import org.json.JSONObject;
import org.json.JSONTokener;

import java.io.FileInputStream;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeMap;

public class histogramSequential {
    public static void main(String[] args) throws Exception{
        long starTime = System.currentTimeMillis();
        TreeMap<String, Integer> locations = new TreeMap<>();
        if (args.length < 2) {
            System.out.println("Usage: java histogramSequential [Path to input file] [Path to output file]");
            System.exit(1);
        }
        
        JSONTokener t;
        try {
            t = new JSONTokener(new FileInputStream(args[0]));
        } catch (Exception e) {
            System.out.println("Opening Json file failed, please make sure path is correct.");
            return;
        }

        while (t.skipTo('{') != 0) {
            JSONObject obj = new JSONObject(t);
            if(obj.has("action")) {
                JSONObject actionObject = obj.getJSONObject("action");
                if (actionObject.has("location")) {
                    JSONObject locationObject = actionObject.getJSONObject("location");
                    String loc = "(" + locationObject.getInt("x") +", " + locationObject.getInt("y") + ")";
                    if (locations.containsKey(loc)) {
                        locations.put(loc, locations.get(loc) + 1);
                    }
                    else {
                        locations.put(loc, 1);
                    }
                }
            }
        }

        Set<String> points = locations.keySet();
        Iterator<String> itr = points.iterator();
        PrintWriter file = new PrintWriter(args[1], "UTF-8");
        while(itr.hasNext()) {
            String loc = itr.next();
            int val = locations.get(loc);
            file.println(loc + "\t" + val );
        }

        file.close();

        long endTime = System.currentTimeMillis();
        long timeDiff = endTime - starTime;
        System.out.println("Time taken: " + timeDiff+ " ms.");
    }
}
