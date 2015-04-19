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

import scala.util.control.NonFatal
import scala.collection.JavaConverters._

import java.io.File
import java.util.UUID

import scalaz._, Scalaz._
import scalaz.\&/.These

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ChecksumFileSystem, FileSystem, Path, FSDataInputStream, FSDataOutputStream, PathFilter}

import org.apache.avro.Schema
import org.apache.avro.file.{DataFileReader, DataFileWriter}
import org.apache.avro.mapred.FsInput
import org.apache.avro.specific.{SpecificDatumReader, SpecificDatumWriter}
import org.apache.avro.util.Utf8

import au.com.cba.omnia.omnitool.{Result, ResultantMonad, ResultantOps, ToResultantMonadOps}
import au.com.cba.omnia.omnitool.ResultantMonadSyntax._

import au.com.cba.omnia.permafrost.io.Streams

/**
 * A data-type that represents a HDFS operation.
 *
 * HDFS operations use a hadoop configuration as context,
 * and produce a (potentially failing) result.
 */
case class Hdfs[A](run: Configuration => Result[A])

/** Hdfs operations */
object Hdfs extends ResultantOps[Hdfs] with ToResultantMonadOps {
  /** Build a HDFS operation from a function. The resultant HDFS operation will not throw an exception. */
  def hdfs[A](f: Configuration => A): Hdfs[A] =
    Hdfs(c => Result.safe(f(c)))

  /** Get the HDFS FileSystem for the current configuration. */
  def filesystem: Hdfs[FileSystem] =
    Hdfs.hdfs(c => FileSystem.get(c))

  /** Produce a value based upon a HDFS FileSystem. */
  def withFilesystem[A](f: FileSystem => A): Hdfs[A] =
    Hdfs.filesystem.map(fs => f(fs))

  /** List all files matching `globPattern` under `dir` path. */
  def files(dir: Path, globPattern: String = "*") = for {
    fs    <- Hdfs.filesystem
    isDir <- Hdfs.isDirectory(dir)
    _     <- Hdfs.fail(s"'$dir' must be a directory!").unlessM(isDir)
    files <- Hdfs.glob(new Path(dir, globPattern))
  } yield files

  /** Get all files/directories from `globPattern`. */
  def glob(globPattern: Path) = for {
    fs    <- Hdfs.filesystem
    files <- Hdfs.value { fs.globStatus(globPattern).toList.map(_.getPath) }
  } yield files

  /** Check the specified `path` exists on HDFS. */
  def exists(path: Path) =
    Hdfs.withFilesystem(_.exists(path))

  /** Check the specified `path` does _not_ exist on HDFS. */
  def notExists(path: Path) =
    Hdfs.exists(path).map(!_)

  /** Create file on HDFS with specified `path`. */
  def create(path: Path): Hdfs[FSDataOutputStream] =
    Hdfs.withFilesystem(_.create(path))

  /** Delete the specified path on HDFS */
  def delete(path: Path, recDelete: Boolean = false): Hdfs[Boolean] =
    Hdfs.withFilesystem(_.delete(path, recDelete)).setMessage(s"Could not delete path $path")

  /** Create directory on HDFS with specified `path`. */
  def mkdirs(path: Path): Hdfs[Boolean] =
    Hdfs.withFilesystem(_.mkdirs(path)).setMessage(s"Could not create dir $path")

  /** Check the specified `path` exists on HDFS and is a directory. */
  def isDirectory(path: Path) =
    Hdfs.withFilesystem(_.isDirectory(path))

  /** Check the specified `path` exists on HDFS and is a file. */
  def isFile(path: Path) =
    Hdfs.withFilesystem(_.isFile(path))

  /** Check if the given path is a final directory - ie no more sub directories. */
  def checkIfNoSubDir(path: Path, pathFilter: Option[PathFilter]) =
    for {
      isDir   <- Hdfs.isDirectory(path)
      _       <- Hdfs.fail(s"'${path.toString}' must be a directory!").unlessM(isDir)
      count   <- pathFilter match {
                   case Some(pathFilter) =>
                     Hdfs.withFilesystem(_.listStatus(path, pathFilter).filter(_.isDirectory).length)
                   case None             =>
                     Hdfs.withFilesystem(_.listStatus(path).filter(_.isDirectory).length)
                 }
      res     =  if (count == 0) true else false
  } yield res

  /** Open file with specified path. */
  def open(path: Path): Hdfs[FSDataInputStream] =
    Hdfs.withFilesystem(_.open(path))

  /** Read contents of file at specified path to a string. */
  def read(path: Path, encoding: String = "UTF-8"): Hdfs[String] =
    open(path).map(in => Streams.read(in, encoding))

  /** Write the string `content` to file at `path` on HDFS. */
  def write(path: Path, content: String, encoding: String = "UTF-8") =
    Hdfs.create(path).map(out => Streams.write(out, content, encoding))

  /** Move file at `src` to `dest` on HDFS. */
  def move(src: Path, dest: Path): Hdfs[Path] =
    Hdfs.filesystem.map(_.rename(src, dest)).as(dest).setMessage(s"Could not move $src to $dest")

  /** Downloads file at `hdfsPath` from HDFS to `localPath` on the local filesystem */
  def copyToLocalFile(hdfsPath: Path, localPath: File): Hdfs[File] =
    Hdfs.filesystem.map(_ match {
      case cfs : ChecksumFileSystem => {
        val dest = new Path(localPath.getAbsolutePath)
        val checksumFile = cfs.getChecksumFile(dest)
        cfs.copyToLocalFile(hdfsPath, dest)
        if (cfs.exists(checksumFile)) cfs.delete(checksumFile, false)
      }
      case fs =>
        fs.copyToLocalFile(hdfsPath, new Path(localPath.getAbsolutePath))
    })
      .as(localPath)
      .setMessage(s"Could not download $hdfsPath to $localPath")

  /** Copy file from the local filesystem at `localPath` to `hdfsPath` on HDFS*/
  def copyFromLocalFile(localPath: File, hdfsPath: Path): Hdfs[Path] =
    Hdfs.filesystem.map(_.copyFromLocalFile(new Path(localPath.getAbsolutePath), hdfsPath))
      .as(hdfsPath)
      .setMessage(s"Could not copy $localPath to $hdfsPath")

  /** Read lines of a file into a list. */
  def lines(path: Path, encoding: String = "UTF-8"): Hdfs[List[String]] =
    Hdfs.read(path).map(_.lines.toList)

  /** Copies all of the files on HDFS to the local filesystem. These files will be deleted on exit. */
  def copyToTempLocal(paths: List[Path]): Hdfs[List[(Path, File)]] = paths.map(hdfsPath => {
      val tmpFile = File.createTempFile("hdfs_", ".hdfs")
      tmpFile.deleteOnExit()
      copyToLocalFile(hdfsPath, tmpFile).map(f => hdfsPath -> f)
    }).sequence

  /** Creates a temp HDFS directory */
  def createTempDir(prefix: String = "tmp_", suffix: String = ""): Hdfs[Path] = {
    val tmpPath = new Path(s"/tmp/${prefix}${UUID.randomUUID}${suffix}")
    for {
      exists <- Hdfs.exists(tmpPath)
      path   <- if (exists) Hdfs.createTempDir(prefix, suffix) // Try again if the random path already exists this should be extremly rare.
                else 
                  Hdfs.mandatory(mkdirs(tmpPath), s"Can't create tmp dir $tmpPath")
                    .map(_ => tmpPath)
    } yield path
  }

  /** Performs the given action on a temporary directory and then deletes the temporary directory. */
  def withTempDir[A](action: Path => Hdfs[A]): Hdfs[A] =
    Hdfs.createTempDir().bracket(
      p => Hdfs.mandatory(delete(p, true), s"Was not able to delete tmp dir $p")
    )(action)

  /** Convenience for constructing `Path` types from strings. */
  def path(path: String): Path =
    new Path(path)

  /** Read avro records from a file at specified path */
  def readAvro[A](path: Path)(implicit mf: Manifest[A]): Hdfs[List[A]] = for {
    fsIn    <- Hdfs.hdfs(c => new FsInput(path, c))
    freader <- Hdfs.value(new DataFileReader[A](fsIn, new SpecificDatumReader[A](mf.runtimeClass.asInstanceOf[Class[A]])))
    objs    <- Hdfs.value(freader.asInstanceOf[java.lang.Iterable[A]].asScala.toList).map(convertUtf8s) // strings are read as utf8's
    _        = freader.close()
    _        = fsIn.close()
  } yield objs

  /** Write avro records to a file at specified path */
  def writeAvro[A](path: Path, records: List[A], schema: Schema)(implicit mf: Manifest[A]): Hdfs[Unit] = for {
    out     <- Hdfs.create(path)
    fwriter <- Hdfs.value(new DataFileWriter[A](new SpecificDatumWriter[A](mf.runtimeClass.asInstanceOf[Class[A]])))
    _       <- Hdfs.value(fwriter.create(schema, out))
    _       <- records.map(r => Hdfs.value(fwriter.append(r))).sequence
    _        = fwriter.close()
  } yield ()

  /** When you tell Avro to read a String, it gives you a Utf8. This is a convienence
      function to convert the Utf8 back to a String */
  def convertUtf8s[A](records: List[A]): List[A] =
    records.map(r => if(r.isInstanceOf[Utf8]) r.toString.asInstanceOf[A] else r)

  implicit val monad: ResultantMonad[Hdfs] = new ResultantMonad[Hdfs] {
    def rPoint[A](v: => Result[A]): Hdfs[A] = Hdfs[A](_ => v)
    def rBind[A, B](ma: Hdfs[A])(f: Result[A] => Hdfs[B]): Hdfs[B] =
      Hdfs(c => f(ma.run(c)).run(c))
  }
}
