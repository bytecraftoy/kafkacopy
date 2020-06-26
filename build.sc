// SPDX-FileCopyrightText: 2020 Juha Heljoranta <juha.heljoranta@iki.fi
//
// SPDX-License-Identifier: Apache-2.0

import mill._
import mill.modules.Jvm
import mill.scalalib._
import mill.scalalib.scalafmt._

object kafkacopy extends ScalaModule with ScalafmtModule {
  def scalaVersion = "2.13.3"

  // https://nathankleyn.com/2019/05/13/recommended-scalac-flags-for-2-13/
  override def scalacOptions = Seq(
    "-deprecation", // Emit warning and location for usages of deprecated APIs.
    "-explaintypes", // Explain type errors in more detail.
    "-feature", // Emit warning and location for usages of features that should be imported explicitly.
    "-language:existentials", // Existential types (besides wildcard types) can be written and inferred
    "-language:experimental.macros", // Allow macro definition (besides implementation and application)
    "-language:higherKinds", // Allow higher-kinded types
    "-language:implicitConversions", // Allow definition of implicit functions called views
    "-unchecked", // Enable additional warnings where generated code depends on assumptions.
    "-Xcheckinit", // Wrap field accessors to throw an exception on uninitialized access.
    //"-Xfatal-warnings", // Fail the compilation if there are any warnings.
    "-Xlint:adapted-args", // Warn if an argument list is modified to match the receiver.
    "-Xlint:constant", // Evaluation of a constant arithmetic expression results in an error.
    "-Xlint:delayedinit-select", // Selecting member of DelayedInit.
    "-Xlint:doc-detached", // A Scaladoc comment appears to be detached from its element.
    "-Xlint:inaccessible", // Warn about inaccessible types in method signatures.
    "-Xlint:infer-any", // Warn when a type argument is inferred to be `Any`.
    "-Xlint:missing-interpolator", // A string literal appears to be missing an interpolator id.
    "-Xlint:nullary-unit", // Warn when nullary methods return Unit.
    "-Xlint:option-implicit", // Option.apply used implicit view.
    "-Xlint:package-object-classes", // Class or object defined in package object.
    "-Xlint:poly-implicit-overload", // Parameterized overloaded implicit methods are not visible as view bounds.
    "-Xlint:private-shadow", // A private field (or class parameter) shadows a superclass field.
    "-Xlint:stars-align", // Pattern sequence wildcard must align with sequence component.
    "-Xlint:type-parameter-shadow", // A local type parameter shadows a type already in scope.
    "-Ywarn-dead-code", // Warn when dead code is identified.
    "-Ywarn-extra-implicit", // Warn when more than one implicit parameter section is defined.
    "-Ywarn-numeric-widen", // Warn when numerics are widened.
    "-Ywarn-unused:implicits", // Warn if an implicit parameter is unused.
    "-Ywarn-unused:imports", // Warn if an import selector is not referenced.
    "-Ywarn-unused:locals", // Warn if a local definition is unused.
    "-Ywarn-unused:params", // Warn if a value parameter is unused.
    "-Ywarn-unused:patvars", // Warn if a variable bound in a pattern is unused.
    "-Ywarn-unused:privates", // Warn if a private member is unused.
    "-Ywarn-value-discard", // Warn when non-Unit expression results are unused.
    "-Ybackend-parallelism", "8", // Enable paralellisation — change to desired number!
    "-Ycache-plugin-class-loader:last-modified", // Enables caching of classloaders for compiler plugins
    "-Ycache-macro-class-loader:last-modified" // and macro definitions. This can lead to performance improvements.
  )

  override def manifest = T{
    // https://issues.apache.org/jira/browse/LOG4J2-2537
    Jvm.createManifest(finalMainClassOpt().toOption).add("Multi-Release" -> "true")
  }

  private val log4j_version = "2.13.3"
  private val kafka_version = "2.5.0"

  override def ivyDeps =
    Agg(
      ivy"org.apache.logging.log4j:log4j-slf4j-impl:$log4j_version",
      // todo, kafka-clients has provided jackson dependency to this, if it has no other uses we should replace it with something else
      ivy"com.fasterxml.jackson.core:jackson-databind:2.10.5",
      ivy"org.apache.kafka:kafka-clients:$kafka_version",
      ivy"com.github.scopt::scopt:3.7.1",
      ivy"commons-io:commons-io:2.7",
    )

  object test extends Tests {
    override def ivyDeps = Agg(
      ivy"org.scalatest::scalatest:3.2.1",
      ivy"org.apache.kafka::kafka:$kafka_version",
      ivy"org.apache.kafka::kafka:$kafka_version;classifier=test",
      ivy"org.springframework.kafka:spring-kafka-test:$kafka_version.RELEASE".exclude(("org.apache.kafka", "kafka_2.12")),
      ivy"org.apache.logging.log4j:log4j-slf4j-impl:$log4j_version"
    )

    def testFrameworks = Seq("org.scalatest.tools.Framework")
  }

}