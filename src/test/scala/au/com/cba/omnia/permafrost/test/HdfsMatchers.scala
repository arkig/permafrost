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

package au.com.cba.omnia.permafrost.test

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.scalacheck.{Gen, Arbitrary}, Arbitrary._

import org.specs2.matcher.{Expectable, Matcher}
import org.specs2.execute.{Result => SpecResult}

import au.com.cba.omnia.omnitool.{Result, Ok, Error}

import au.com.cba.omnia.permafrost.hdfs.Hdfs

/**
 * This trait is designed to be mixed into a base Specification
 * class. It contains a set of matchers and arbitraries useful
 * for testing on HDFS.
 *
 * It also requires additional context to be mixed into that
 * base specification. Specifically it requires a hadoop
 * configuration object for accessing HDFS and and a unique test
 * identifier to isolate paths on HDFS.
 *
 * To use the matchers use the `must be{type}` form of assertion.
 */
trait HdfsMatchers { self: Spec with ConfigurationContext with UniqueContext =>
  lazy val pathId = new java.util.concurrent.atomic.AtomicInteger(0)

  def beResult[A](expected: Result[A]): Matcher[Hdfs[A]] =
    (h: Hdfs[A]) => h.run(conf) must_== expected

  def beResultLike[A](expected: Result[A] => SpecResult): Matcher[Hdfs[A]] =
    (h: Hdfs[A]) => expected(h.run(conf))

  def beValue[A](expected: A): Matcher[Hdfs[A]] =
    beResult(Result.ok(expected))

  def beValueLike[A](expected: A => SpecResult): Matcher[Hdfs[A]] =
    beResultLike[A]({
      case Ok(v) =>
        expected(v)
      case Error(_) =>
        failure
    })

  implicit def PathArbitrary: Arbitrary[Path] =
    Arbitrary(for {
      n     <- Gen.choose(1, 4)
      parts <- Gen.listOfN(n, Gen.identifier)
    } yield testpath.suffix(s"""/arbitrary/${pathId.getAndIncrement}/${parts.mkString("/")}"""))
 }
