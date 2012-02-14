/*
 * Copyright 2012 Twitter, Inc.
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

import org.scalatest.{Spec, BeforeAndAfterEach}

class ResourceCheckingSuite extends Spec with BeforeAndAfterEach {
  override def afterEach() {
    if (!MemoryMappedFile.openFiles.isEmpty) {
      Console.err.println("MemoryMappedFile.openFiles is not empty:")
      Console.err.println(MemoryMappedFile.openFiles.mkString("\n"))
      throw new RuntimeException("MemoryMappedFile.openFiles is not empty")
    }
  }
}
