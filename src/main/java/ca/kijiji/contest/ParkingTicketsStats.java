package ca.kijiji.contest;

import java.io.InputStream;

import java.nio.file.Paths;

import java.util.SortedMap;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.Comparator;
import java.util.Map;

import org.jruby.CompatVersion;
import org.jruby.embed.ScriptingContainer;
import org.jruby.embed.LocalContextScope;
import org.jruby.embed.PathType;

public class ParkingTicketsStats {

   public static SortedMap<String, Integer> sortStreetsByProfitability(InputStream parkingTicketsStream) {
      // set up the JRuby environment
      // details on configuration options: https://github.com/jruby/jruby/wiki/RedBridge
      ScriptingContainer container = new ScriptingContainer(LocalContextScope.THREADSAFE);
      container.setCompatVersion(CompatVersion.RUBY2_0);
      container.setInput(parkingTicketsStream);

      HashMap<String, Integer> map = new HashMap<String, Integer>();
      ValueComparator cmp = new ValueComparator(map);
      SortedMap<String, Integer> result = new TreeMap<String, Integer>(cmp);

      container.put("result", map);

      String filename = Paths.get(System.getProperty("user.dir"), "src", "jruby", "parking_ticket_stats.rb").toString();

      container.runScriptlet(PathType.ABSOLUTE, filename);

      result.putAll(map);

      return result;
   }

   static class ValueComparator implements Comparator<String> {
      public Map<String, Integer> base;

      public ValueComparator(Map<String, Integer> base) {
         this.base = base;
      }

      public int compare(String a, String b) {
         return base.get(b) - base.get(a);
      }
   }
}