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

import java.io.{File, FileWriter}

import scalaz._, Scalaz._

import org.scalacheck.Arbitrary, Arbitrary.arbitrary

import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration

import org.apache.avro.Schema

import au.com.cba.omnia.omnitool.{Result, Ok}
import au.com.cba.omnia.omnitool.test.OmnitoolProperties.resultantMonad
import au.com.cba.omnia.omnitool.test.Arbitraries._

import au.com.cba.omnia.permafrost.test.{HdfsTest, Identifier}
import au.com.cba.omnia.permafrost.test.Arbitraries._

class HdfsSpec extends HdfsTest { def is = s2"""
Hdfs Operations
===============

Hdfs operations should:
  obey resultant monad laws (monad and plus laws)         ${resultantMonad.laws[Hdfs]}

Hdfs io:
  hdfs handles exceptions                                 $safeHdfs
  isFile should always be false on new path               $isFile
  isDirectory should always be false on new path          $isDirectory
  exists should always be false on new path               $exists
  notExists should always be true on new path             $notExists
  create -> isFile should always be true                  $create
  mkdirs -> isDirectory should always be true             $mkdirs
  create -> exists should always be true                  $createExists
  mkdirs -> exists should always be true                  $mkdirsExists
  write -> read should be symmetric                       $readwrite
  create -> move -> exist should always be true           $move
  can get paths from a glob pattern                       $glob
  read / write lines should be like String#lines          $lines
  copy to local file should download the file             $copyToLocalFile
  copy to local file should not create crc                $copyToLocalFileNoCrc
  copy to temp local should download the files            $copyToTempLocal
  copy from local file should download the file           $copyFromLocalFile
  createTempDir -> exists should always be true           $createTempDir
  createTempDir doesn't create the same dir twice         $createTempDir2
  withTempDir should create a temp dir and then delete it $withTempDir

Hdfs avro:
  can read / write avro records                   $avro

"""

  def safeHdfs = prop((t: Throwable) =>
    Hdfs.hdfs(_ => throw t) must beResult { Result.exception(t) })

  def isFile = prop((p: Path) =>
    Hdfs.isFile(p) must beValue { false })

  def isDirectory = prop((p: Path) =>
    Hdfs.isDirectory(p) must beValue { false })

  def exists = prop((p: Path) =>
    Hdfs.exists(p) must beValue { false })

  def notExists = prop((p: Path) =>
    Hdfs.notExists(p) must beValue { true })

  def create = prop((p: Path) =>
    (Hdfs.create(p) >> Hdfs.isFile(p)) must beValue { true })

  def mkdirs = prop((p: Path) =>
    (Hdfs.mkdirs(p) >> Hdfs.isDirectory(p)) must beValue { true  })

  def createExists = prop((p: Path) =>
    (Hdfs.create(p) >> Hdfs.exists(p)) must beValue { true })

  def mkdirsExists = prop((p: Path) =>
    (Hdfs.mkdirs(p) >> Hdfs.exists(p)) must beValue { true })

  def readwrite = prop((p: Path, s: String) =>
    (Hdfs.write(p, s) >> Hdfs.read(p)) must beValue { s })

  def move = prop((src: Path, trg: Path) => (for {
    _        <- Hdfs.create(src)
    before   <- Hdfs.exists(src)
    _        <- Hdfs.move(src, trg)
    after    <- Hdfs.exists(trg)
    gone     <- Hdfs.notExists(src)
  } yield (before, after, gone)) must beValue { (true, true, true) })

  def glob = prop((r: Path, a: Identifier, b: Identifier) => (a != b) ==> { (for {
    fs    <- Hdfs.filesystem
    p1    = fs.makeQualified(r.suffix(s"/${a.value}"))
    p2    = fs.makeQualified(r.suffix(s"/${b.value}"))
    _     <- Hdfs.mkdirs(r)
    _     <- Hdfs.create(p1)
    _     <- Hdfs.create(p2)
    files <- Hdfs.glob(r.suffix(s"/{${a.value},${b.value}}"))
  } yield (files, p1, p2)) must beValueLike {
    case (files, p1, p2) => files must containTheSameElementsAs(List(p1, p2))
  }})

  def copyToLocalFile = prop((hdfsSrc: Path, data: String) => (for {
    remote     <- Hdfs.create(hdfsSrc)
    _          <- Hdfs.value({ remote.writeBytes(data); remote.close() })
    written    <- Hdfs.read(hdfsSrc, "ISO8859_1")
    local       = File.createTempFile("local", ".test")
    _           = local.delete()
    copied     <- Hdfs.copyToLocalFile(hdfsSrc, local)
    _           = local.deleteOnExit()
    _          <- Hdfs.withFilesystem(_.delete(hdfsSrc, false))
  } yield (written, copied)) must beValueLike {
    case (written, local: File) =>
      scala.io.Source.fromFile(local)(scala.io.Codec.ISO8859).mkString must equalTo(written)
  })

  def copyToLocalFileNoCrc = prop((hdfsSrc: Path, data: String) => (for {
    remote     <- Hdfs.create(hdfsSrc)
    _          <- Hdfs.value({ remote.writeBytes(data); remote.close() })
    written    <- Hdfs.read(hdfsSrc, "ISO8859_1")
    local       = File.createTempFile("local", ".test")
    _           = local.delete()
    _          <- Hdfs.copyToLocalFile(hdfsSrc, local)
    _           = local.deleteOnExit()
    crc         = new File(local.getParent, "." + local.getName + ".crc")
    _          <- Hdfs.withFilesystem(_.delete(hdfsSrc, false))
  } yield (crc)) must beValueLike {
    case (crc: File) => crc.isFile must beFalse
  })


  def copyToTempLocal = prop((srcs: List[(Path, String)]) => (for {
    remotes    <- srcs.map({ case (path, data) =>
                    Hdfs.create(path)
                        .map(f => { f.writeBytes(data); f.close(); path })
                  }).sequence
    written    <- srcs.map({ case (path, _) =>
                    Hdfs.read(path, "ISO8859_1").map(d => path -> d)
                  }).sequence
    locals     <- Hdfs.copyToTempLocal(remotes)
  } yield (written, locals)) must beValueLike {
    case (written, locals) => locals.map({ case (path, file) =>
        path -> scala.io.Source.fromFile(file)(scala.io.Codec.ISO8859).mkString
      }) must containTheSameElementsAs(written)
  })

  def copyFromLocalFile = prop { (p: Path, content: String) =>
    val f = File.createTempFile("local", ".txt")
    val w = new FileWriter(f)
    w.write(content)
    w.close

    Hdfs.copyFromLocalFile(f, p) >> Hdfs.read(p) must beValue(content)
  }

  def lines = prop((p: Path, lines: List[Identifier]) =>
    (Hdfs.write(p, lines.map(_.value).mkString("\n")) >> Hdfs.lines(p)) must beValue {
      lines.map(_.value).mkString("\n").lines.toList })

  def createTempDir = {
    (Hdfs.createTempDir() >>= Hdfs.exists) must beValue(true)
  }

  def createTempDir2 = {
    val isSame = for {
      p1 <- Hdfs.createTempDir()
      p2 <- Hdfs.createTempDir()
    } yield p1 == p2

    isSame must beValue(false)
  }

  def withTempDir = {
    val hdfs = for {
      x               <- Hdfs.withTempDir(p => Hdfs.exists(p).map((p, _)))
      (path, existed) = x
      exists          <- Hdfs.exists(path)
    } yield (existed, exists)

    hdfs must beValue((true, false))
  }

  def avro = prop((p: Path, s1: String, s2: String) =>
    (Hdfs.writeAvro[String](p, List(s1, s2), Schema.create(Schema.Type.STRING)) >> Hdfs.readAvro[String](p)) must beResultLike {
      case Ok(v) => v must_== List(s1, s2)
      case _     => failure
  })

  /** Note these are not general purpose, specific to testing laws. */

  implicit def HdfsArbirary[A: Arbitrary]: Arbitrary[Hdfs[A]] =
    Arbitrary(arbitrary[Result[A]] map (Hdfs.result(_)))

  implicit def HdfsEqual: Equal[Hdfs[Int]] =
    Equal.equal[Hdfs[Int]]((a, b) =>
      a.run(new Configuration) must_== b.run(new Configuration))
}
