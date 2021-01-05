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

package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.write.BatchWrite
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation

case class DeleteFrom(
    rel: DataSourceV2Relation,
    condition: Option[Expression],
    remainingRows: LogicalPlan,
    write: Option[BatchWrite]) extends Command with SupportsSubquery {
  override def output: Seq[Attribute] = Nil
  override def children: Seq[LogicalPlan] = remainingRows :: Nil
}
