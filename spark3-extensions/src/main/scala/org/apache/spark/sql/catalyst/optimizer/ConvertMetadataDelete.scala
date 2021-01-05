/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.expressions.PredicateHelper
import org.apache.spark.sql.catalyst.expressions.SubqueryExpression
import org.apache.spark.sql.catalyst.plans.logical.DeleteFrom
import org.apache.spark.sql.catalyst.plans.logical.DeleteFromTable
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.iceberg.catalog.ExtendedSupportsDelete
import org.apache.spark.sql.execution.datasources.DataSourceStrategy
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.sources
import org.apache.spark.sql.types.BooleanType

object ConvertMetadataDelete extends Rule[LogicalPlan] with PredicateHelper {

  override def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case DeleteFrom(relation: DataSourceV2Relation, Some(Literal(const, BooleanType)), _, _) =>
      if (const != null && const == true) {
        DeleteFromTable(relation, Some(Literal(true, BooleanType)))
      } else {
        DeleteFromTable(relation, Some(Literal(false, BooleanType)))
      }

    case DeleteFrom(relation: DataSourceV2Relation, Some(cond), _, _) if isMetadataDelete(relation, cond) =>
      DeleteFromTable(relation, Some(cond))
  }

  private def isMetadataDelete(relation: DataSourceV2Relation, cond: Expression): Boolean = {
    relation.table match {
      case t: ExtendedSupportsDelete if !SubqueryExpression.hasSubquery(cond) =>
        val predicates = splitConjunctivePredicates(cond)
        val normalizedPredicates = DataSourceStrategy.normalizeExprs(predicates, relation.output)
        val dataSourceFilters = toDataSourceFilters(normalizedPredicates)
        val allPredicatesTranslated = normalizedPredicates.size == dataSourceFilters.length
        allPredicatesTranslated && t.canDeleteWhere(dataSourceFilters)
      case _ => false
    }
  }

  private def toDataSourceFilters(predicates: Seq[Expression]): Array[sources.Filter] = {
    predicates.flatMap { p =>
      val translatedFilter = DataSourceStrategy.translateFilter(p, supportNestedPredicatePushdown = true)
      if (translatedFilter.isEmpty) {
        logWarning(s"Cannot translate expression to source filter: $p")
      }
      translatedFilter
    }.toArray
  }
}
