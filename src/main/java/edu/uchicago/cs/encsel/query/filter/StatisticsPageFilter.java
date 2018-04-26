/* 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package edu.uchicago.cs.encsel.query.filter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Operators.And;
import org.apache.parquet.filter2.predicate.Operators.Column;
import org.apache.parquet.filter2.predicate.Operators.Eq;
import org.apache.parquet.filter2.predicate.Operators.Gt;
import org.apache.parquet.filter2.predicate.Operators.GtEq;
import org.apache.parquet.filter2.predicate.Operators.LogicalNotUserDefined;
import org.apache.parquet.filter2.predicate.Operators.Lt;
import org.apache.parquet.filter2.predicate.Operators.LtEq;
import org.apache.parquet.filter2.predicate.Operators.Not;
import org.apache.parquet.filter2.predicate.Operators.NotEq;
import org.apache.parquet.filter2.predicate.Operators.Or;
import org.apache.parquet.filter2.predicate.Operators.UserDefined;
import org.apache.parquet.filter2.predicate.UserDefinedPredicate;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;

import static org.apache.parquet.Preconditions.checkArgument;
import static org.apache.parquet.Preconditions.checkNotNull;

/**
 * @author chunwei
 * Applies a {@link org.apache.parquet.filter2.predicate.FilterPredicate} to statistics about a datapage
 *
 * Note: the supplied predicate must not contain any instances of the not() operator as this is not
 * supported by this filter.
 *
 * the supplied predicate should first be run through {@link org.apache.parquet.filter2.predicate.LogicalInverseRewriter} to rewrite it
 * in a form that doesn't make use of the not() operator.
 *
 * the supplied predicate should also have already been run through
 * {@link org.apache.parquet.filter2.predicate.SchemaCompatibilityValidator}
 * to make sure it is compatible with the schema of this file.
 *
 * Returns true if all the records represented by the statistics in the provided column metadata can be dropped.
 *         false otherwise (including when it is not known, which is often the case).
 */

public class StatisticsPageFilter implements FilterPredicate.Visitor<Boolean> {

  private static final boolean BLOCK_MIGHT_MATCH = false;
  private static final boolean BLOCK_CANNOT_MATCH = true;

  private static int PAGECOUNT = 0;
  private static int PAGESKIPPED = 0;


  public static boolean canDrop(FilterPredicate pred, DataPage dataPage) {
    checkNotNull(pred, "pred");
    checkNotNull(dataPage, "dataPage");
    PAGECOUNT++;
    boolean drop = pred.accept(new StatisticsPageFilter(dataPage));
    if (drop)
      PAGESKIPPED++;
    //System.out.println(pred.accept(new StatisticsPageFilter(dataPage)) + " " + pred.toString() + " " + getStatisticsFromPageHeader(dataPage).toString());
    return drop;
  }

  public static int getPAGECOUNT() {
    return PAGECOUNT;
  }


  public static int getPAGESKIPPED() {
    return PAGESKIPPED;
  }
  
  private DataPage dataPage;
  private Statistics stats;

  private StatisticsPageFilter(DataPage dataPage) {
	this.dataPage = dataPage;
	this.stats = getStatisticsFromPageHeader(dataPage);
  }

  // is this column chunk composed entirely of nulls?
  // assumes the column chunk's statistics is not empty
  private boolean isAllNulls(DataPage dataPage) {
    return stats.getNumNulls() == dataPage.getValueCount();
  }

  // are there any nulls in this column chunk?
  // assumes the column chunk's statistics is not empty
  private boolean hasNulls(DataPage dataPage) {
    return stats.getNumNulls() > 0;
  }
  
  
  private static <T extends Comparable<T>> Statistics<T> getStatisticsFromPageHeader(DataPage page) {
    return page.accept(new DataPage.Visitor<Statistics<T>>() {
      @Override
      @SuppressWarnings("unchecked")
      public Statistics<T> visit(DataPageV1 dataPageV1) {
        return (Statistics<T>) dataPageV1.getStatistics();
      }

      @Override
      @SuppressWarnings("unchecked")
      public Statistics<T> visit(DataPageV2 dataPageV2) {
        return (Statistics<T>) dataPageV2.getStatistics();
      }
    });
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T extends Comparable<T>> Boolean visit(Eq<T> eq) {

    T value = eq.getValue();

    Statistics<T> stats = this.stats;

    if (stats.isEmpty()) {
      // we have no statistics available, we cannot drop any page
      return BLOCK_MIGHT_MATCH;
    }

    if (value == null) {
      // we are looking for records where v eq(null)
      // so drop if there are no nulls in this chunk
      return !hasNulls(dataPage);
    }

    if (isAllNulls(dataPage)) {
      // we are looking for records where v eq(someNonNull)
      // and this is a column of all nulls, so drop it
      return BLOCK_CANNOT_MATCH;
    }

    // drop if value < min || value > max
    return value.compareTo(stats.genericGetMin()) < 0 || value.compareTo(stats.genericGetMax()) > 0;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T extends Comparable<T>> Boolean visit(NotEq<T> notEq) {

    T value = notEq.getValue();
    
    Statistics<T> stats = this.stats;

    if (stats.isEmpty()) {
      // we have no statistics available, we cannot drop any page
      return BLOCK_MIGHT_MATCH;
    }

    if (value == null) {
      // we are looking for records where v notEq(null)
      // so, if this is a column of all nulls, we can drop it
      return isAllNulls(dataPage);
    }

    if (hasNulls(dataPage)) {
      // we are looking for records where v notEq(someNonNull)
      // but this chunk contains nulls, we cannot drop it
      return BLOCK_MIGHT_MATCH;
    }

    // drop if this is a column where min = max = value
    return value.compareTo(stats.genericGetMin()) == 0 && value.compareTo(stats.genericGetMax()) == 0;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T extends Comparable<T>> Boolean visit(Lt<T> lt) {

    Statistics<T> stats = this.stats;

    if (stats.isEmpty()) {
      // we have no statistics available, we cannot drop any page
      return BLOCK_MIGHT_MATCH;
    }

    if (isAllNulls(this.dataPage)) {
      // we are looking for records where v < someValue
      // this chunk is all nulls, so we can drop it
      return BLOCK_CANNOT_MATCH;
    }

    T value = lt.getValue();

    // drop if value <= min
    return  value.compareTo(stats.genericGetMin()) <= 0;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T extends Comparable<T>> Boolean visit(LtEq<T> ltEq) {

    Statistics<T> stats = this.stats;

    if (stats.isEmpty()) {
      // we have no statistics available, we cannot drop any page
      return BLOCK_MIGHT_MATCH;
    }

    if (isAllNulls(dataPage)) {
      // we are looking for records where v <= someValue
      // this chunk is all nulls, so we can drop it
      return BLOCK_CANNOT_MATCH;
    }

    T value = ltEq.getValue();

    // drop if value < min
    return value.compareTo(stats.genericGetMin()) < 0;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T extends Comparable<T>> Boolean visit(Gt<T> gt) {

    Statistics<T> stats = this.stats;

    if (stats.isEmpty()) {
      // we have no statistics available, we cannot drop any page
      return BLOCK_MIGHT_MATCH;
    }

    if (isAllNulls(dataPage)) {
      // we are looking for records where v > someValue
      // this chunk is all nulls, so we can drop it
      return BLOCK_CANNOT_MATCH;
    }

    T value = gt.getValue();

    // drop if value >= max
    return value.compareTo(stats.genericGetMax()) >= 0;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T extends Comparable<T>> Boolean visit(GtEq<T> gtEq) {
    
    Statistics<T> stats = this.stats;

    if (stats.isEmpty()) {
      // we have no statistics available, we cannot drop any page
      return BLOCK_MIGHT_MATCH;
    }

    if (isAllNulls(dataPage)) {
      // we are looking for records where v >= someValue
      // this chunk is all nulls, so we can drop it
      return BLOCK_CANNOT_MATCH;
    }

    T value = gtEq.getValue();

    // drop if value >= max
    return value.compareTo(stats.genericGetMax()) > 0;
  }

  @Override
  public Boolean visit(And and) {
    // seems unintuitive to put an || not an && here but we can
    // drop a chunk of records if we know that either the left or
    // the right predicate agrees that no matter what we don't
    // need this chunk.
    return and.getLeft().accept(this) || and.getRight().accept(this);
  }

  @Override
  public Boolean visit(Or or) {
    // seems unintuitive to put an && not an || here
    // but we can only drop a chunk of records if we know that
    // both the left and right predicates agree that no matter what
    // we don't need this chunk.
    return or.getLeft().accept(this) && or.getRight().accept(this);
  }

  @Override
  public Boolean visit(Not not) {
    throw new IllegalArgumentException(
        "This predicate contains a not! Did you forget to run this predicate through LogicalInverseRewriter? " + not);
  }

@Override
public <T extends Comparable<T>, U extends UserDefinedPredicate<T>> Boolean visit(UserDefined<T, U> udp) {
	// TODO Auto-generated method stub
	return null;
}

@Override
public <T extends Comparable<T>, U extends UserDefinedPredicate<T>> Boolean visit(LogicalNotUserDefined<T, U> udp) {
	// TODO Auto-generated method stub
	return null;
}


}
