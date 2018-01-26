/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License,
 *
 * Contributors:
 *     Hao Jiang - initial API and implementation
 *
 */

package edu.uchicago.cs.encsel.ptnmining

import edu.uchicago.cs.encsel.ptnmining.matching.{NamingVisitor, PatternMatcher, Record}
import edu.uchicago.cs.encsel.ptnmining.parser._
import edu.uchicago.cs.encsel.ptnmining.rule._

import scala.collection.mutable

/**
  * Created by harper on 3/16/17.
  */

object Pattern {

  val rules = Array(new CommonSeqRule, new SuccinctRule, new MergeSeqRule, new IntegerRangeRule, new UseAnyRule)

  def generate(in: Seq[Seq[Token]]): Pattern = {
    // Generate a direct pattern by translating tokens

    val translated = new PUnion(in.map(l => new PSeq(l.map(new PToken(_)): _*)))

    rules.foreach(rule => {
      rule match {
        case data: DataRewriteRule => data.generateOn(in)
        case _ => Unit
      }
    })
    // Repeatedly refine the pattern using supplied rules
    var toRefine: Pattern = translated
    var needRefine = true
    var refineResult: Pattern = toRefine
    while (needRefine) {
      val refined = refine(toRefine)
      if (refined._2) {
        toRefine = refined._1
      } else {
        needRefine = false
        refineResult = refined._1
      }
    }

    val validated = validate(refineResult)
    validated
  }

  protected def refine(root: Pattern): (Pattern, Boolean) = {
    var current = root

    rules.indices.foreach(i => {
      rules(i).reset
      current = rules(i).rewrite(current)
      if (rules(i).happened) {
        // Apply the first valid rule
        return (current, true)
      }
    })
    (root, false)
  }

  def validate(ptn: Pattern): Pattern = {
    ptn
  }
}

trait PatternVisitor {

  protected val path = new mutable.Stack[Pattern]

  def on(ptn: Pattern): Unit

  def enter(container: Pattern): Unit = path.push(container)

  def exit(container: Pattern): Unit = path.pop()
}


trait Pattern {
  private[ptnmining] var name = ""

  def getName = name

  /**
    * @return all leaf patterns
    */
  def flatten: Seq[Pattern] = Seq(this)

  def matchon(tokens: Seq[Token]): Option[Record] = PatternMatcher.matchon(this, tokens)

  def naming() = visit(new NamingVisitor)

  /**
    * Recursively visit the pattern elements starting from the root
    *
    * @param visitor
    */
  def visit(visitor: PatternVisitor): Unit = visitor.on(this)
}

class PToken(t: Token) extends Pattern {
  val token = t

  override def equals(obj: scala.Any): Boolean = {
    if (obj.isInstanceOf[AnyRef] && obj.asInstanceOf[AnyRef].eq(this))
      return true
    if (obj.isInstanceOf[PToken]) {
      val t = obj.asInstanceOf[PToken]
      return t.token.equals(token)
    }
    super.equals(obj)
  }

  override def hashCode(): Int = token.hashCode()
}

object PSeq {
  def make(content: Seq[Pattern]): Pattern = {
    val filtered = content.filter(_ != PEmpty)
    filtered.length match {
      case 0 => PEmpty
      case 1 => filtered.head
      case _ => new PSeq(filtered)
    }
  }
}

class PSeq(cnt: Pattern*) extends Pattern {
  val content = cnt

  def this(ps: Traversable[Pattern]) = this(ps.toSeq: _*)

  override def equals(obj: scala.Any): Boolean = {
    if (obj.isInstanceOf[AnyRef] && obj.asInstanceOf[AnyRef].eq(this))
      return true
    if (obj.isInstanceOf[PSeq]) {
      val seq = obj.asInstanceOf[PSeq]
      return seq.content.equals(content)
    }
    super.equals(obj)
  }

  override def hashCode(): Int =
    content.isEmpty match {
      case true => 0
      case false => content.map(_.hashCode).sum
    }

  override def flatten: Seq[Pattern] = content.flatMap(_.flatten)

  override def visit(visitor: PatternVisitor): Unit = {
    visitor.on(this)
    visitor.enter(this)
    content.foreach(_.visit(visitor))
    visitor.exit(this)
  }
}

object PUnion {
  def make(content: Seq[Pattern]) = {
    content.toSet.size match {
      case 0 => PEmpty
      case 1 => content.head
      case _ => new PUnion(content)
    }
  }
}

class PUnion(cnt: Pattern*) extends Pattern {
  val content = cnt.toSet.toSeq

  def this(c: Traversable[Pattern]) = this(c.toSeq: _*)

  override def equals(obj: scala.Any): Boolean = {
    if (obj == this)
      return true
    if (obj.isInstanceOf[PUnion]) {
      val union = obj.asInstanceOf[PUnion]
      return union.content.equals(content)
    }
    super.equals(obj)
  }

  override def flatten: Seq[Pattern] = content.flatMap(_.flatten)

  override def visit(visitor: PatternVisitor): Unit = {
    visitor.on(this)
    visitor.enter(this)
    content.foreach(_.visit(visitor))
    visitor.exit(this)
  }

  override def hashCode(): Int = content.isEmpty match {
    case true => 0
    case false => content.map(_.hashCode()).sum
  }
}

object PEmpty extends Pattern

trait PAny extends Pattern {
  override def equals(obj: scala.Any): Boolean = {
    if (obj.isInstanceOf[AnyRef] && obj.asInstanceOf[AnyRef].eq(this))
      return true
    obj match {
      case any: PAny => getClass == any.getClass
      case _ => super.equals(obj)
    }
  }

  override def hashCode(): Int = getClass.hashCode()

  def maxLength: Int
}

class PWordAny extends PAny {
  var _minLength = 1
  var _maxLength = -1

  def this(min: Int, max: Int) = {
    this()
    _minLength = min
    _maxLength = max
  }

  def this(limit: Int) = this(limit, limit)

  def minLength: Int = _minLength

  def maxLength: Int = _maxLength
}

class PIntAny extends PAny {
  var _minLength = 1
  var _maxLength = -1

  def this(min: Int, max: Int) = {
    this()
    _maxLength = max
    _minLength = min
  }

  def this(limit: Int) = this(limit, limit)

  def maxLength: Int = _maxLength

  def minLength: Int = _minLength
}

class PDoubleAny extends PAny {
  var maxLength = 8

  def this(ml: Int) = {
    this()
    maxLength = ml
  }
}

class PIntRange extends Pattern {
  var min: BigInt = BigInt(0)
  var max: BigInt = BigInt(0)

  def this(min: BigInt, max: BigInt) {
    this()
    this.min = min
    this.max = max
  }

  def this(min: Int, max: Int) {
    this(BigInt(min), BigInt(max))
  }

  override def hashCode(): Int = this.min.hashCode() * 13 + this.max.hashCode()

  override def equals(obj: scala.Any): Boolean = {
    if (obj.isInstanceOf[AnyRef] && obj.asInstanceOf[AnyRef].eq(this))
      return true
    obj match {
      case range: PIntRange => {
        this.min == range.min && this.max == range.max
      }
      case _ => super.equals(obj)
    }
  }
}