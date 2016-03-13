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

package com.github.tototoshi.play2.json4s.jackson

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import play.api._
import play.api.inject.guice.GuiceApplicationBuilder
import play.api.libs.ws.ahc.AhcWSClient
import play.api.mvc._
import play.api.test._
import play.api.test.Helpers._

import org.json4s._
import org.json4s.jackson.JsonMethods._
import com.github.tototoshi.play2.json4s.test.jackson.Helpers._

import org.scalatest.{BeforeAndAfter, FunSpec, ShouldMatchers}

import scala.concurrent.Await
import scala.concurrent.duration._


case class Person(id: Long, name: String, age: Int)

class TestApplication(json4s: Json4s) extends Controller {

  implicit val formats = DefaultFormats

  import json4s._

  def get = Action { implicit request =>
    Ok(Extraction.decompose(Person(1, "ぱみゅぱみゅ", 20)))
  }

  def post = Action(json) { implicit request =>
    Ok(request.body.extract[Person].name)
  }

}



class PlayModuleSpec extends FunSpec with ShouldMatchers with BeforeAndAfter {

  val configuration = Configuration.empty

  val json4s = new Json4s(configuration)

  var system: ActorSystem = _

  before {
    system = ActorSystem()
  }

  after {
    system.terminate()
  }

  import json4s._

  describe ("Json4sPlayModule") {

    describe ("With controllers") {

      it ("allow you to use json4s-jackson value as response") {
        val app = new TestApplication(json4s)
        val res = app.get(FakeRequest("GET", ""))
        contentType(res) should be (Some("application/json"))
        contentAsJson4s(res) should be (JObject(List(("id",JInt(1)), ("name",JString("ぱみゅぱみゅ")), ("age",JInt(20)))))
      }

      it ("accept json4s-jackson request") {
        val app = new TestApplication(json4s)
        val res = app.post(FakeRequest().withJson4sBody(parse("""{"id":1,"name":"ぱみゅぱみゅ","age":20}""")))
        contentAsString(res) should be ("ぱみゅぱみゅ")
      }

    }

    describe ("With WS") {

      it ("should enable you to use json4s objects as request body") {
        import scala.language.postfixOps

        implicit val formats = DefaultFormats
        implicit val context = play.api.libs.concurrent.Execution.Implicits.defaultContext
        implicit val materializer = ActorMaterializer()(system)

        def withSimpleServer[T](block: => T): T = withServer {
          case (method, path) => Action(json4s.json) { request =>
            val person = request.body.extract[Person]
            play.api.mvc.Results.Ok(Extraction.decompose(person))
          }
        }(block)

        def withServer[T](routes: (String, String) => Handler)(block: => T): T = {
          val app = GuiceApplicationBuilder().routes({
            case (method, path) => routes(method, path)
          }).build()
          running(TestServer(testServerPort, app))(block)
        }

        withSimpleServer {
          val wSClient = AhcWSClient()
          val res = Await.result(
            wSClient.url("http://localhost:" + testServerPort )
                .post(Extraction.decompose(Person(1, "ぱみゅぱみゅ", 20))),
            5.seconds
          )
          res.body should be ("""{"id":1,"name":"ぱみゅぱみゅ","age":20}""")
        }
      }

    }

  }

}


