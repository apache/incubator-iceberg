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

import com.dremio.nessie.client.NessieClient
import com.dremio.nessie.model.Branch
import com.dremio.nessie.model.ImmutableBranch
import com.dremio.nessie.model.ImmutableHash
import com.dremio.nessie.model.ImmutableTag
import com.dremio.nessie.model.Tag
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.catalog.CatalogPlugin
import org.apache.spark.unsafe.types.UTF8String


case class CreateBranchExec(
    output: Seq[Attribute],
    branch: String,
    currentCatalog: CatalogPlugin,
    isBranch: Boolean,
    catalog: Option[String],
    createdFrom: Option[String]) extends V2CommandExec {

  override protected def run(): Seq[InternalRow] = {
    val catalogName = catalog.getOrElse(currentCatalog.name)
    val catalogConf = SparkSession.active.sparkContext.conf.getAllWithPrefix(s"spark.sql.catalog.$catalogName.").toMap
    val nessieClient = NessieClient.withConfig(x => catalogConf.getOrElse(x.replace("nessie.",""), null))
    val hash = createdFrom.map(nessieClient.getTreeApi.getReferenceByName)
      .orElse(Option(nessieClient.getTreeApi.getDefaultBranch)).map(x => x.getHash).orNull
    val ref = if (isBranch) Branch.of(branch, hash) else Tag.of(branch, hash)
    nessieClient.getTreeApi.createReference(ref)
    val branchResult = nessieClient.getTreeApi.getReferenceByName(ref.getName)
    val refType = branchResult match {
      case _: ImmutableHash => "Hash"
      case _: ImmutableBranch => "Branch"
      case _: ImmutableTag => "Tag"
    }
    Seq(InternalRow(UTF8String.fromString(refType), UTF8String.fromString(branchResult.getName),
      UTF8String.fromString(branchResult.getHash)))
  }

  override def simpleString(maxFields: Int): String = {
    s"CreateBranchExec ${catalog.getOrElse(currentCatalog.name())} ${if (isBranch) "BRANCH" else "TAG" } ${branch} " +
      s"${createdFrom}"
  }
}
