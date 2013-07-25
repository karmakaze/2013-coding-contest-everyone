package ca.kijiji.contest;

import java.io.InputStream;

import java.nio.file.Paths;

import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Comparator;
import java.util.Map;

import java.util.concurrent.ConcurrentHashMap;

import java.util.concurrent.atomic.AtomicLong;

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

      // run the script to get the data
      String filename = Paths.get(System.getProperty("user.dir"), "src", "jruby", "parking_ticket_stats.rb").toString();

      Map<?,?> map = (Map<?,?>) container.runScriptlet(PathType.ABSOLUTE, filename);

      // translate the result to the format expected by the test suite
      SortedMap<String, Integer> result = new TreeMap<String, Integer>(new ValueComparator(map));

      for (Map.Entry<?,?> entry : map.entrySet()) {
         String key = (String) entry.getKey();
         AtomicLong value = (AtomicLong) entry.getValue();

         result.put( key, (int)value.get() );
      }

      // return the translated result
      return result;
   }

   static class ValueComparator implements Comparator<String> {
      public Map<?, ?> base;

      public ValueComparator(Map<?, ?> base) {
         this.base = base;
      }

      public int compare(String ka, String kb) {
         AtomicLong va = (AtomicLong) base.get(ka);
         AtomicLong vb = (AtomicLong) base.get(kb);

         return (int)(vb.get() - va.get());
      }
   }
}