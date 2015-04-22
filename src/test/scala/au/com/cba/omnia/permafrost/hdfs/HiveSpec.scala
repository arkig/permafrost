//   Copyright 2014 Commonwealth Bank of Australia
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.

package au.com.cba.omnia.permafrost.hdfs

import scalaz._, Scalaz._

import java.io.File

import org.apache.hadoop.fs.Path

import au.com.cba.omnia.permafrost.test.HdfsTest

class HiveSpec extends HdfsTest { def is = s2"""
HDFS Utility Operations on hive Managed Data
============================================

We should be able to:
  Get the min dataset within a partition          $min
  Get the max dataset within a partition          $max
  Get the min dataset invalid partition naming    $neg

Using the safe comparitor:
  Sort Mixed List correctly                       $negSort

"""

  // This helper function is required because the arbitrary Path 'p' is relative, but the Path returned
  // by min/maxPartitionM is absolute. It assumes that the tests are using the local filesystem.
  def absolutePath(path: Path) = new Path("file", null, new File(path.toString).getAbsoluteFile().toString)

  def min = prop((p: Path) =>
    (Hdfs.mkdirs(p) >>
      Hdfs.mkdirs(p.suffix("/4")) >>
      Hdfs.mkdirs(p.suffix("/2")) >>
      Hdfs.mkdirs(p.suffix("/3")) >>
      Hdfs.mkdirs(p.suffix("/11")) >>
      Hive.minPartitionM(p) ) must beValue(Some(absolutePath(p.suffix("/2")))))


  def max = prop((p: Path) =>
    (Hdfs.mkdirs(p) >>
      Hdfs.mkdirs(p.suffix("/1")) >>
      Hdfs.mkdirs(p.suffix("/2")) >>
      Hdfs.mkdirs(p.suffix("/3")) >>
      Hdfs.mkdirs(p.suffix("/12")) >>
      Hive.maxPartitionM(p) ) must beValue(Some(absolutePath(p.suffix("/12")))))

  def neg = prop((p: Path) =>
    (Hdfs.mkdirs(p) >>
      Hdfs.mkdirs(p.suffix("/4")) >>
      Hdfs.mkdirs(p.suffix("/2")) >>
      Hdfs.mkdirs(p.suffix("/sdfsdf")) >>
      Hive.minPartitionM(p) ) must beValue((Some(absolutePath(p.suffix("/2"))))))

  val prefix = "/root"

  def negSort = List(s"$prefix/2",s"$prefix/asdfsdfs",s"$prefix/11a")
    .map(dir => new Path(dir))
    .foldLeft(Option.empty[Path])(Hive.safeCompare(_ < _)).map(_.toString) mustEqual Some(s"$prefix/2")
}
