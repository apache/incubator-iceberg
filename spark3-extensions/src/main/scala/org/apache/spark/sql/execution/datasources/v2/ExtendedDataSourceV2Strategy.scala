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

package org.apache.spark.sql.execution.datasources.v2

import org.apache.iceberg.spark.Spark3Util
import org.apache.iceberg.spark.SparkCatalog
import org.apache.iceberg.spark.SparkSessionCatalog
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.NamedRelation
import org.apache.spark.sql.catalyst.expressions.And
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.AddPartitionField
import org.apache.spark.sql.catalyst.plans.logical.Call
import org.apache.spark.sql.catalyst.plans.logical.CreateBranchField
import org.apache.spark.sql.catalyst.plans.logical.DropPartitionField
import org.apache.spark.sql.catalyst.plans.logical.DynamicFileFilter
import org.apache.spark.sql.catalyst.plans.logical.DynamicFileFilterWithCardinalityCheck
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.MergeInto
import org.apache.spark.sql.catalyst.plans.logical.ReplaceData
import org.apache.spark.sql.catalyst.plans.logical.SetWriteDistributionAndOrdering
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.catalog.TableCatalog
import org.apache.spark.sql.connector.iceberg.read.SupportsFileFilter
import org.apache.spark.sql.execution.FilterExec
import org.apache.spark.sql.execution.LeafExecNode
import org.apache.spark.sql.execution.ProjectExec
import org.apache.spark.sql.execution.SparkPlan
import scala.collection.JavaConverters._

case class ExtendedDataSourceV2Strategy(spark: SparkSession) extends Strategy {

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case c @ Call(procedure, args) =>
      val input = buildInternalRow(args)
      CallExec(c.output, procedure, input) :: Nil

    case AddPartitionField(IcebergCatalogAndIdentifier(catalog, ident), transform, name) =>
      AddPartitionFieldExec(catalog, ident, transform, name) :: Nil

    case DropPartitionField(IcebergCatalogAndIdentifier(catalog, ident), transform) =>
      DropPartitionFieldExec(catalog, ident, transform) :: Nil

    case SetWriteDistributionAndOrdering(
        IcebergCatalogAndIdentifier(catalog, ident), distributionMode, ordering) =>
      SetWriteDistributionAndOrderingExec(catalog, ident, distributionMode, ordering) :: Nil

    case DynamicFileFilter(scanPlan, fileFilterPlan, filterable) =>
      DynamicFileFilterExec(planLater(scanPlan), planLater(fileFilterPlan), filterable) :: Nil

    case DynamicFileFilterWithCardinalityCheck(scanPlan, fileFilterPlan, filterable, filesAccumulator) =>
      DynamicFileFilterWithCardinalityCheckExec(
        planLater(scanPlan),
        planLater(fileFilterPlan),
        filterable,
        filesAccumulator) :: Nil

    case PhysicalOperation(project, filters, DataSourceV2ScanRelation(_, scan: SupportsFileFilter, output)) =>
      // projection and filters were already pushed down in the optimizer.
      // this uses PhysicalOperation to get the projection and ensure that if the batch scan does
      // not support columnar, a projection is added to convert the rows to UnsafeRow.
      val batchExec = ExtendedBatchScanExec(output, scan)
      withProjectAndFilter(project, filters, batchExec, !batchExec.supportsColumnar) :: Nil

    case ReplaceData(relation, batchWrite, query) =>
      ReplaceDataExec(batchWrite, refreshCache(relation), planLater(query)) :: Nil

    case MergeInto(mergeIntoParams, output, child) =>
      MergeIntoExec(mergeIntoParams, output, planLater(child)) :: Nil

    case c @ CreateBranchField(branch, isBranch, catalog, reference) =>
      CreateBranchExec(c.output, branch, spark.sessionState.catalogManager.currentCatalog, isBranch, catalog,
        reference) :: Nil

    case _ => Nil
  }

  private def buildInternalRow(exprs: Seq[Expression]): InternalRow = {
    val values = new Array[Any](exprs.size)
    for (index <- exprs.indices) {
      values(index) = exprs(index).eval()
    }
    new GenericInternalRow(values)
  }

  private def withProjectAndFilter(
      project: Seq[NamedExpression],
      filters: Seq[Expression],
      scan: LeafExecNode,
      needsUnsafeConversion: Boolean): SparkPlan = {
    val filterCondition = filters.reduceLeftOption(And)
    val withFilter = filterCondition.map(FilterExec(_, scan)).getOrElse(scan)

    if (withFilter.output != project || needsUnsafeConversion) {
      ProjectExec(project, withFilter)
    } else {
      withFilter
    }
  }

  private def refreshCache(r: NamedRelation)(): Unit = {
    spark.sharedState.cacheManager.recacheByPlan(spark, r)
  }

  private object IcebergCatalogAndIdentifier {
    def unapply(identifier: Seq[String]): Option[(TableCatalog, Identifier)] = {
      val catalogAndIdentifier = Spark3Util.catalogAndIdentifier(spark, identifier.asJava)
      catalogAndIdentifier.catalog match {
        case icebergCatalog: SparkCatalog =>
          Some((icebergCatalog, catalogAndIdentifier.identifier))
        case icebergCatalog: SparkSessionCatalog[_] =>
          Some((icebergCatalog, catalogAndIdentifier.identifier))
        case _ =>
          None
      }
    }
  }
}
