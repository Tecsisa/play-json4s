/*
 * Copyright 2013 Toshiyuki Takahashi
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.tototoshi.play2.json4s.core

import akka.stream._
import akka.stream.scaladsl.{Flow, Sink}
import akka.stream.stage.{GraphStageWithMaterializedValue, GraphStageLogic, OutHandler, InHandler}
import akka.util.ByteString
import org.json4s.{JValue => Json4sJValue, _}
import play.api._
import play.api.http._
import play.api.libs.streams.Accumulator
import play.api.mvc._

import scala.concurrent.{Promise, Future}
import scala.language.reflectiveCalls
import scala.util.control.NonFatal

class Json4sParser[T] (configuration: Configuration, methods: JsonMethods[T]) {

  import methods._

  implicit def writeableOf_NativeJValue(implicit codec: Codec): Writeable[Json4sJValue] = {
    Writeable((jval: Json4sJValue) => codec.encode(compact(render(jval))))
  }

  implicit def contentTypeOf_JsValue(implicit codec: Codec): ContentTypeOf[Json4sJValue] = {
    ContentTypeOf(Some(ContentTypes.JSON))
  }

  def DEFAULT_MAX_TEXT_LENGTH: Int =
    configuration.getBytes("parsers.text.maxLength").map(_.toInt).getOrElse(1024 * 100)

  final type ParseErrorHandler = (RequestHeader, Array[Byte], Throwable) => Future[Result]

  private val logger = Logger(this.getClass)

  protected def defaultParseErrorMessage = "Invalid Json"
  protected def defaultParseErrorHandler: ParseErrorHandler = {
    (header, _, _) => createBadResult(defaultParseErrorMessage)(header)
  }

  /**
    * Create a body parser that uses the given parser and enforces the given max length.
    *
    * @param name The name of the body parser.
    * @param maxLength The maximum length of the body to buffer.
    * @param errorMessage The error message to prepend to the exception message if an error was encountered.
    * @param parser The parser.
    */
  private def tolerantBodyParser[A](name: String, maxLength: Long, errorMessage: String)(parser: (RequestHeader, ByteString) => A): BodyParser[A] =
    BodyParser(name + ", maxLength=" + maxLength) { request =>
      import play.api.libs.iteratee.Execution.Implicits.trampoline

      enforceMaxLength(request, maxLength, Accumulator(
        Sink.fold[ByteString, ByteString](ByteString.empty)((state, bs) => state ++ bs)
      ) mapFuture { bytes =>
        try {
          Future.successful(Right(parser(request, bytes)))
        } catch {
          case NonFatal(e) =>
            logger.debug(errorMessage, e)
            createBadResult(errorMessage + ": " + e.getMessage)(request).map(Left(_))
        }
      })
    }

  /**
    * Enforce the max length on the stream consumed by the given accumulator.
    */
  private[this] def enforceMaxLength[A](request: RequestHeader, maxLength: Long, accumulator: Accumulator[ByteString, Either[Result, A]]): Accumulator[ByteString, Either[Result, A]] = {
    val takeUpToFlow = Flow.fromGraph(new TakeUpTo(maxLength))
    Accumulator(takeUpToFlow.toMat(accumulator.toSink) { (statusFuture, resultFuture) =>
      import play.api.libs.iteratee.Execution.Implicits.trampoline
      val defaultCtx = play.api.libs.concurrent.Execution.Implicits.defaultContext

      statusFuture.flatMap {
        case MaxSizeExceeded(_) =>
          val badResult = Future.successful(()).flatMap(_ => createBadResult("Request Entity Too Large", Status.REQUEST_ENTITY_TOO_LARGE)(request))(defaultCtx)
          badResult.map(Left(_))
        case MaxSizeNotExceeded => resultFuture
      }
    })
  }

  private[this] val defaultParser: (RequestHeader, Array[Byte]) => Json4sJValue = {
    (request, bytes) =>
      parse(new String(bytes, request.charset.getOrElse("utf-8")))
  }

  /**
    * Parse the body as Json without checking the Content-Type.
    *
    * @param maxLength Max length allowed or returns EntityTooLarge HTTP response.
    */
  def tolerantJson(maxLength: Int): BodyParser[Json4sJValue] =
    tolerantBodyParser[Json4sJValue]("json", maxLength, "Invalid Json") { (request, bytes) =>
      // Encoding notes: RFC 4627 requires that JSON be encoded in Unicode, and states that whether that's
      // UTF-8, UTF-16 or UTF-32 can be auto detected by reading the first two bytes. So we ignore the declared
      // charset and don't decode, we passing the byte array as is because Jackson supports auto detection.
      parse(bytes.iterator.asInputStream)
    }

  /**
   * Parse the body as Json without checking the Content-Type.
   */
  def tolerantJson: BodyParser[Json4sJValue] = tolerantJson(DEFAULT_MAX_TEXT_LENGTH)

  private[this] def takeUpTo(maxLength: Long): Graph[FlowShape[ByteString, ByteString], Future[MaxSizeStatus]] = new TakeUpTo(maxLength)

  private[this] class TakeUpTo(maxLength: Long) extends GraphStageWithMaterializedValue[FlowShape[ByteString, ByteString], Future[MaxSizeStatus]] {

    private val in = Inlet[ByteString]("TakeUpTo.in")
    private val out = Outlet[ByteString]("TakeUpTo.out")

    override def shape: FlowShape[ByteString, ByteString] = FlowShape.of(in, out)

    override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[MaxSizeStatus]) = {
      val status = Promise[MaxSizeStatus]()
      var pushedBytes: Long = 0

      val logic = new GraphStageLogic(shape) {
        setHandler(out, new OutHandler {
          override def onPull(): Unit = {
            pull(in)
          }
          override def onDownstreamFinish(): Unit = {
            status.success(MaxSizeNotExceeded)
            completeStage()
          }
        })
        setHandler(in, new InHandler {
          override def onPush(): Unit = {
            val chunk = grab(in)
            pushedBytes += chunk.size
            if (pushedBytes > maxLength) {
              status.success(MaxSizeExceeded(maxLength))
              // Make sure we fail the stream, this will ensure downstream body parsers don't try to parse it
              failStage(new MaxLengthLimitAttained)
            } else {
              push(out, chunk)
            }
          }
          override def onUpstreamFinish(): Unit = {
            status.success(MaxSizeNotExceeded)
            completeStage()
          }
          override def onUpstreamFailure(ex: Throwable): Unit = {
            status.failure(ex)
            failStage(ex)
          }
        })
      }

      (logic, status.future)
    }
  }

  private[this] class MaxLengthLimitAttained extends RuntimeException(null, null, false, false)

  private def createBadResult(msg: String, statusCode: Int = Status.BAD_REQUEST): RequestHeader => Future[Result] = { request =>
    LazyHttpErrorHandler.onClientError(request, statusCode, msg)
  }

  def jsonWithErrorHandler(maxLength: Int)(errorHandler: ParseErrorHandler): BodyParser[Json4sJValue] = BodyParsers.parse.when(
    _.contentType.exists(m => m.equalsIgnoreCase("text/json") || m.equalsIgnoreCase("application/json")),
    tolerantJson(maxLength),
    createBadResult("Expecting text/json or application/json body")
  )

  def json(maxLength: Int): BodyParser[Json4sJValue] =
    jsonWithErrorHandler(maxLength)(defaultParseErrorHandler)

  def json: BodyParser[Json4sJValue] = json(BodyParsers.parse.DefaultMaxTextLength)
}
