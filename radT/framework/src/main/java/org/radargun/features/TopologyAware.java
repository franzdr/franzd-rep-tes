package org.radargun.features;

import java.util.Date;
import java.util.List;

/**
 * The wrapper should be aware of the current topology and its history
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public interface TopologyAware {
   /**
    * @return Ordered list of events when the topology was changing (nodes were added/removed, coordinator changed)
    */
   List<Event> getTopologyChangeHistory();

   /**
    * @return Ordered list of events when the data distribution was changing (rebalancing data)
    */
   List<Event> getRehashHistory();

   /**
    * @return True if this slave has unique role in the cluster
    */
   boolean isCoordinator();

   /**
    * Topology event is a period of time
    */
   abstract class Event {
      /**
       * @return Date when this event started
       */
      public abstract Date getStarted();

      /**
       * @return Date when this event finished or null if it has not finished yet.
       */
      public abstract Date getEnded();

      /**
       * @return How many members were part of this when this event started.
       */
      public abstract int getMembersAtStart();

      /**
       * @return How many members were part of this when this event ended.
       */
      public abstract int getMembersAtEnd();

      /**
       * @return A deep copy of this event.
       */
      public abstract Event copy();

      @Override
      public boolean equals(Object o) {
         if (o == null) return false;
         if (!(o instanceof Event)) return false;
         Event e = (Event) o;
         return ((getStarted() == null && e.getStarted() == null)
                       || (getStarted() != null && getStarted().equals(e.getStarted())))
               && ((getEnded() == null && e.getEnded() == null)
                       || (getEnded() != null && getEnded().equals(e.getEnded())));
      }

      @Override
      public String toString() {
         return "[" + getStarted() + " - " + getEnded() + "]";
      }
   }
}
