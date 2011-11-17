/*
 * Copyright 2011 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License. You may obtain
 * a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.twitter.libkestrel

import com.twitter.logging._
import java.util.{logging => jlogging}
import org.scalatest._

trait TestLogging extends AbstractSuite { self: Suite =>
  val logLevel = Logger.levelNames(Option[String](System.getenv("log")).getOrElse("FATAL").toUpperCase)

  private val rootLog = Logger.get("")
  private var oldLevel: jlogging.Level = _

  abstract override def withFixture(test: NoArgTest) {
    oldLevel = rootLog.getLevel()
    rootLog.setLevel(logLevel)
    rootLog.addHandler(new ConsoleHandler(new Formatter(), None))
    try {
      super.withFixture(test)
    } finally {
      rootLog.clearHandlers()
      rootLog.setLevel(oldLevel)
    }
  }
}
