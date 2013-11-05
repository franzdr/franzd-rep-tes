/* 
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */
package org.radargun.stages.helpers;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Range {
   
   private int start;
   private int end;

   public Range(int start, int end) {
      this.start = start;
      this.end = end;
   }

   public int getStart() {
      return start;
   }
   public int getEnd() {
      return end;
   }
   public int getSize() {
      return end - start;
   }

   public Range shift(int shift) {
      return new Range(start + shift, end + shift);
   }

   @Override
   public int hashCode() {
      return start + 31*(end - 1);
   }

   @Override
   public boolean equals(Object other) {
      if (other == null || !(other instanceof Range)) return false;
      Range otherRange = (Range) other;
      return start == otherRange.getStart() && end == otherRange.getEnd();
   }

   @Override
   public String toString() {
      return "[" + start + ", " + end + "]";
   }

   /**
    * 
    * Returns pair [startKey, endKey] that specifies a subrange { startKey, ..., endKey-1 } of key
    * range { 0, 1, ..., numKeys-1 } divideRange divides the keyset evenly to numParts parts with
    * difference of part lengths being max 1.
    * 
    * @param numKeys
    *           Total number of keys
    * @param numParts
    *           Number of parts we're dividing to
    * @param partIdx
    *           Index of part we want to get
    * @return The pair [startKey, endKey]
    */
   public static Range divideRange(int numKeys, int numParts, int partIdx) {
      int base = (numKeys / numParts) + 1;
      int mod = numKeys % numParts;
      if (partIdx < mod) {
         int startKey = partIdx * base;
         return new Range(startKey, startKey + base);
      } else {
         int startKey = base * mod + (partIdx - mod) * (base - 1);
         return new Range(startKey, startKey + base - 1);
      }
   }

   public static List<List<Range>> balance(List<Range> ranges, int numParts) {
      int totalSize = 0;
      for (Range range : ranges) {
         totalSize += range.getSize();
      }
      Iterator<Range> iterator = ranges.iterator();
      Range currentRange;
      if (iterator.hasNext()) {
         currentRange = iterator.next();
      } else {
         currentRange = new Range(0, 0);
      }

      List<List<Range>> balanced = new ArrayList<List<Range>>(numParts);

      for (int i = 0; i < numParts; ++i) {
         int myRangeSize = divideRange(totalSize, numParts, i).getSize();
         List<Range> myRanges = new ArrayList<Range>();

         while (currentRange.getSize() <= myRangeSize) {
            myRanges.add(currentRange);
            myRangeSize -= currentRange.getSize();
            if (iterator.hasNext()) {
               currentRange = iterator.next();
            } else {
               break; // this should happen only for the last range
            }
         }
         if (myRangeSize != 0) {
            myRanges.add(new Range(currentRange.getStart(), currentRange.getStart() + myRangeSize));
            currentRange = new Range(currentRange.getStart() + myRangeSize, currentRange.getEnd());
         }
         balanced.add(myRanges);
      }
      return balanced;
   }
}
