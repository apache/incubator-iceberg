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

package org.apache.spark.sql.catalyst.optimizer

import java.util
import java.util.UUID
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.Ascending
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.EqualNullSafe
import org.apache.spark.sql.catalyst.expressions.EqualTo
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.expressions.Not
import org.apache.spark.sql.catalyst.expressions.Or
import org.apache.spark.sql.catalyst.expressions.PredicateHelper
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.expressions.SubExprUtils
import org.apache.spark.sql.catalyst.expressions.SubqueryExpression
import org.apache.spark.sql.catalyst.plans.logical.Aggregate
import org.apache.spark.sql.catalyst.plans.logical.DeleteFrom
import org.apache.spark.sql.catalyst.plans.logical.DeleteFromTable
import org.apache.spark.sql.catalyst.plans.logical.DynamicFileFilter
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.catalyst.plans.logical.RepartitionByExpression
import org.apache.spark.sql.catalyst.plans.logical.Sort
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.SupportsRead
import org.apache.spark.sql.connector.catalog.SupportsWrite
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.catalog.TableCapability
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.iceberg.catalog.ExtendedSupportsDelete
import org.apache.spark.sql.connector.iceberg.catalog.SupportsMerge
import org.apache.spark.sql.connector.iceberg.read.SupportsFileFilter
import org.apache.spark.sql.connector.iceberg.read.SupportsFileFilter.FilterFiles
import org.apache.spark.sql.connector.iceberg.write.MergeBuilder
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.LogicalWriteInfo
import org.apache.spark.sql.connector.write.LogicalWriteInfoImpl
import org.apache.spark.sql.connector.write.WriteBuilder
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

// TODO: should be part of early scan push down after the delete condition is optimized
object RewriteDelete extends Rule[LogicalPlan] with PredicateHelper with Logging {

  import org.apache.spark.sql.execution.datasources.v2.ExtendedDataSourceV2Implicits._

  private val FILE_NAME_COL = "_file"
  private val ROW_POS_COL = "_pos"

  private case class MergeTable(
      table: Table with SupportsMerge,
      operation: String) extends Table with SupportsRead with SupportsWrite {
    val mergeBuilder: MergeBuilder = table.newMergeBuilder(operation, newWriteInfo(table.schema))

    override def name: String = table.name
    override def schema: StructType = table.schema
    override def partitioning: Array[Transform] = table.partitioning
    override def properties: util.Map[String, String] = table.properties
    override def capabilities: util.Set[TableCapability] = table.capabilities
    override def toString: String = table.toString

    // TODO: refactor merge builder to accept options and info after construction
    override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = mergeBuilder.asScanBuilder()
    override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = mergeBuilder.asWriteBuilder()

    private def newWriteInfo(schema: StructType): LogicalWriteInfo = {
      val uuid = UUID.randomUUID()
      LogicalWriteInfoImpl(queryId = uuid.toString, schema, CaseInsensitiveStringMap.empty)
    }
  }

  private class DeletableMergeTable(
      table: Table with SupportsMerge with ExtendedSupportsDelete,
      operation: String) extends MergeTable(table, operation) with ExtendedSupportsDelete {
    override def canDeleteWhere(filters: Array[sources.Filter]): Boolean = table.canDeleteWhere(filters)
    override def deleteWhere(filters: Array[sources.Filter]): Unit = table.deleteWhere(filters)
  }

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case DeleteFromTable(r, Some(cond)) =>
      // TODO: do a switch based on whether we get BatchWrite or DeltaBatchWrite
      val relation = r.collectFirst {
        case v2: DataSourceV2Relation =>
          v2
      }.get

      val mergeTable = relation.table match {
        case withDelete: Table with SupportsMerge with ExtendedSupportsDelete =>
          new DeletableMergeTable(withDelete, "delete")
        case _ =>
          MergeTable(relation.table.asMergeable, "delete")
      }
      val scanPlan = buildScanPlan(mergeTable, relation, cond)

      val remainingRowFilter = not(cond)
      val remainingRowsPlan = Filter(remainingRowFilter, scanPlan)

      val writePlan = buildWritePlan(remainingRowsPlan, relation.output)
      val writeRelation = relation.copy(table = mergeTable, output = addFileAndPos(relation.output))

      if (SubqueryExpression.hasSubquery(cond)) {
        DeleteFrom(writeRelation, None, writePlan, None)
      } else {
        DeleteFrom(writeRelation, Some(cond), writePlan, None)
      }
  }

  private def buildScanPlan(
      mergeTable: MergeTable,
      tableRelation: DataSourceV2Relation,
      cond: Expression): LogicalPlan = {
    val mergeRelation = tableRelation.copy(table = mergeTable, output = addFileAndPos(tableRelation.output))

    mergeTable.mergeBuilder match {
      case filterable: SupportsFileFilter =>
        val filterRelation = tableRelation.copy(output = addFileAndPos(tableRelation.output))
        val filteredFiles = new FilterFiles()
        val matchingFilePlan = buildFileFilterPlan(cond, filterRelation)
        filterable.filterFiles(filteredFiles)
        DynamicFileFilter(mergeRelation, matchingFilePlan, filteredFiles)

      case _ =>
        mergeRelation
    }
  }

  private def buildWritePlan(
      remainingRowsPlan: LogicalPlan,
      output: Seq[AttributeReference]): LogicalPlan = {

    val fileNameCol = UnresolvedAttribute(FILE_NAME_COL)
    val rowPosCol = UnresolvedAttribute(ROW_POS_COL)
    val order = Seq(SortOrder(fileNameCol, Ascending), SortOrder(rowPosCol, Ascending))
    val numShufflePartitions = SQLConf.get.numShufflePartitions
    val repartition = RepartitionByExpression(Seq(fileNameCol), remainingRowsPlan, numShufflePartitions)
    val sort = Sort(order, global = false, repartition)
    Project(output, sort)
  }

  private def buildFileFilterPlan(cond: Expression, scanRelation: DataSourceV2Relation): LogicalPlan = {
    val matchingFilter = Filter(cond, scanRelation)
    val fileAttr = UnresolvedAttribute(FILE_NAME_COL)
    val agg = Aggregate(Seq(fileAttr), Seq(fileAttr), matchingFilter)
    Project(Seq(UnresolvedAttribute(FILE_NAME_COL)), agg)
  }

  private def not(expr: Expression): Expression = {
    expr match {
      case Not(child) =>
        child
      case _ =>
        splitConjunctivePredicates(expr).map {
          case Not(child) =>
            child
          case pred =>
            Not(EqualNullSafe(pred, Literal(true)))
        }.reduceLeft(Or)
    }
  }

  private def addFileAndPos(output: Seq[AttributeReference]): Seq[AttributeReference] = {
    output :+
        AttributeReference(FILE_NAME_COL, StringType, nullable = false)() :+
        AttributeReference(ROW_POS_COL, LongType, nullable = false)()
  }
}
