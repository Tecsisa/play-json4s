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

package com.github.tototoshi.play2.json4s.test

import org.json4s._
import play.api.mvc._
import play.api.test._

object Implicits {

  implicit class Json4sFakeRequest[A](fakeRequest: FakeRequest[A]) {
    def withJson4sBody(jval: JValue): Request[JValue] = fakeRequest.withBody(body = jval)
  }

}
