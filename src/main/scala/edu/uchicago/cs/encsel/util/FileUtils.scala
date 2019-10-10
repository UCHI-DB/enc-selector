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
package edu.uchicago.cs.encsel.util

import java.io.File
import java.net.URI
import java.nio.file.{Files, Path, Paths}
import java.util.concurrent.{Callable, Executor, ExecutorService, TimeUnit}

import scala.collection.JavaConverters._
import scala.sys.process._

object FileUtils {
  def recursive: (Path => Iterable[Path]) = (p: Path) => {
    p match {
      case nofile if !Files.exists(nofile) => {
        Iterable[Path]()
      }
      case dir if Files.isDirectory(dir) => {
        Files.list(dir).iterator().asScala.toIterable.flatMap {
          recursive(_)
        }
      }
      case _ => {
        Iterable(p)
      }
    }
  }


  def multithread_scan[T](root: URI, function: (Path => T), executor: ExecutorService): Unit = {
    val target = Paths.get(root)
    val tasks = FileUtils.recursive(target).map { path =>
      new Callable[T] {
        def call: T = {
          function(path)
        }
      }
    }.asJavaCollection
    executor.invokeAll(tasks)
    executor.shutdown()
    executor.awaitTermination(Long.MaxValue, TimeUnit.SECONDS)
  }

  def scan[T](root: URI, function: (Path => Unit)): Unit = {
    val target = Paths.get(root)
    FileUtils.recursive(target).foreach(function(_))
  }

  def isDone(file: URI, suffix: String): Boolean = {
    Files.exists(Paths.get(new URI("%s.%s".format(file.toString, suffix))))
  }

  def markDone(file: URI, suffix: String): Unit = {
    Files.createFile(Paths.get(new URI("%s.%s".format(file.toString, suffix))))
  }

  def addExtension(source: URI, ext: String): URI = {
    new URI(source.getScheme, source.getHost,
      "%s.%s".format(source.getPath, ext), null)
  }

  def replaceExtension(source: URI, ext: String): URI = {
    new URI(source.getScheme, source.getHost,
      source.getPath.replaceAll("\\.[\\d\\w]+$", ".%s".format(ext)), null)
  }

  def numLine(file: URI): Int = {
    val result: String = ("wc -l %s".format(new File(file).getAbsolutePath)) !!;
    result.trim.split("\\s+")(0).toInt
  }

  def numNonEmptyLine(file: URI): Int = {
    val result: String = ("grep -cve '^\\s*$' %s".format(new File(file).getAbsolutePath)) !!;
    result.trim.toInt
  }
}