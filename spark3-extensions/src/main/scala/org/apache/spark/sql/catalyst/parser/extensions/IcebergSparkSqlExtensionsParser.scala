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

package org.apache.spark.sql.catalyst.parser.extensions

import java.util.Locale
import org.antlr.v4.runtime._
import org.antlr.v4.runtime.atn.PredictionMode
import org.antlr.v4.runtime.misc.ParseCancellationException
import org.antlr.v4.runtime.tree.TerminalNodeImpl
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.parser.ParseErrorListener
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.parser.UpperCaseCharStream
import org.apache.spark.sql.catalyst.parser.extensions.IcebergSqlExtensionsParser.NonReservedContext
import org.apache.spark.sql.catalyst.parser.extensions.IcebergSqlExtensionsParser.QuotedIdentifierContext
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.trees.Origin
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.VariableSubstitution
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.StructType

class IcebergSparkSqlExtensionsParser(delegate: ParserInterface) extends ParserInterface {

  private lazy val substitutor = new VariableSubstitution(SQLConf.get)
  private lazy val astBuilder = new IcebergSqlExtensionsAstBuilder(delegate)

  /**
   * Parse a string to a DataType.
   */
  override def parseDataType(sqlText: String): DataType = {
    delegate.parseDataType(sqlText)
  }

  /**
   * Parse a string to a raw DataType without CHAR/VARCHAR replacement.
   */
  override def parseRawDataType(sqlText: String): DataType = {
    delegate.parseRawDataType(sqlText)
  }

  /**
   * Parse a string to an Expression.
   */
  override def parseExpression(sqlText: String): Expression = {
    delegate.parseExpression(sqlText)
  }

  /**
   * Parse a string to a TableIdentifier.
   */
  override def parseTableIdentifier(sqlText: String): TableIdentifier = {
    delegate.parseTableIdentifier(sqlText)
  }

  /**
   * Parse a string to a FunctionIdentifier.
   */
  override def parseFunctionIdentifier(sqlText: String): FunctionIdentifier = {
    delegate.parseFunctionIdentifier(sqlText)
  }

  /**
   * Parse a string to a multi-part identifier.
   */
  override def parseMultipartIdentifier(sqlText: String): Seq[String] = {
    delegate.parseMultipartIdentifier(sqlText)
  }

  /**
   * Creates StructType for a given SQL string, which is a comma separated list of field
   * definitions which will preserve the correct Hive metadata.
   */
  override def parseTableSchema(sqlText: String): StructType = {
    delegate.parseTableSchema(sqlText)
  }

  /**
   * Parse a string to a LogicalPlan.
   */
  override def parsePlan(sqlText: String): LogicalPlan = {
    val sqlTextAfterSubstitution = substitutor.substitute(sqlText)
    if (isIcebergCommand(sqlTextAfterSubstitution)) {
      parse(sqlTextAfterSubstitution) { parser => astBuilder.visit(parser.singleStatement()) }.asInstanceOf[LogicalPlan]
    } else {
      delegate.parsePlan(sqlText)
    }
  }

  private def isIcebergCommand(sqlText: String): Boolean = {
    val normalized = sqlText.toLowerCase(Locale.ROOT).trim()
    normalized.startsWith("call") || normalized.startsWith("create branch") || normalized.startsWith("create tag") || (
        normalized.startsWith("alter table") && (
            normalized.contains("add partition field") ||
            normalized.contains("drop partition field") ||
            normalized.contains("write ordered by") ||
            normalized.contains("write locally ordered by") ||
            normalized.contains("write distributed by") ||
            normalized.contains("write unordered")))
  }

  protected def parse[T](command: String)(toResult: IcebergSqlExtensionsParser => T): T = {
    val lexer = new IcebergSqlExtensionsLexer(new UpperCaseCharStream(CharStreams.fromString(command)))
    lexer.removeErrorListeners()
    lexer.addErrorListener(ParseErrorListener)

    val tokenStream = new CommonTokenStream(lexer)
    val parser = new IcebergSqlExtensionsParser(tokenStream)
    parser.addParseListener(IcebergSqlExtensionsPostProcessor)
    parser.removeErrorListeners()
    parser.addErrorListener(ParseErrorListener)

    try {
      try {
        // first, try parsing with potentially faster SLL mode
        parser.getInterpreter.setPredictionMode(PredictionMode.SLL)
        toResult(parser)
      }
      catch {
        case _: ParseCancellationException =>
          // if we fail, parse with LL mode
          tokenStream.seek(0) // rewind input stream
          parser.reset()

          // Try Again.
          parser.getInterpreter.setPredictionMode(PredictionMode.LL)
          toResult(parser)
      }
    }
    catch {
      case e: ParseException if e.command.isDefined =>
        throw e
      case e: ParseException =>
        throw e.withCommand(command)
      case e: AnalysisException =>
        val position = Origin(e.line, e.startPosition)
        throw new ParseException(Option(command), e.message, position, position)
    }
  }
}

/**
 * The post-processor validates & cleans-up the parse tree during the parse process.
 */
// while we reuse ParseErrorListener and ParseException, we have to copy and modify PostProcessor
// as it directly depends on classes generated from the extensions grammar file
case object IcebergSqlExtensionsPostProcessor extends IcebergSqlExtensionsBaseListener {

  /** Remove the back ticks from an Identifier. */
  override def exitQuotedIdentifier(ctx: QuotedIdentifierContext): Unit = {
    replaceTokenByIdentifier(ctx, 1) { token =>
      // Remove the double back ticks in the string.
      token.setText(token.getText.replace("``", "`"))
      token
    }
  }

  /** Treat non-reserved keywords as Identifiers. */
  override def exitNonReserved(ctx: NonReservedContext): Unit = {
    replaceTokenByIdentifier(ctx, 0)(identity)
  }

  private def replaceTokenByIdentifier(
      ctx: ParserRuleContext,
      stripMargins: Int)(
      f: CommonToken => CommonToken = identity): Unit = {
    val parent = ctx.getParent
    parent.removeLastChild()
    val token = ctx.getChild(0).getPayload.asInstanceOf[Token]
    val newToken = new CommonToken(
      new org.antlr.v4.runtime.misc.Pair(token.getTokenSource, token.getInputStream),
      IcebergSqlExtensionsParser.IDENTIFIER,
      token.getChannel,
      token.getStartIndex + stripMargins,
      token.getStopIndex - stripMargins)
    parent.addChild(new TerminalNodeImpl(f(newToken)))
  }
}
