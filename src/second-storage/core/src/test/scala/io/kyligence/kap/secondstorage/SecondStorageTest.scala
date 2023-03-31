/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.kyligence.kap.secondstorage

import java.lang.reflect.InvocationTargetException
import java.util.ArrayList

import org.apache.kylin.common.{ForceToTieredStorage, QueryContext}
import org.apache.kylin.metadata.cube.model.NDataflow
import org.apache.kylin.metadata.model.NDataModel
import org.apache.spark.sql.common.{LocalMetadata, SparderBaseFunSuite}
import org.mockito.Mockito

import scala.reflect.runtime.{universe => ru}

import io.kyligence.kap.secondstorage.enums.LockTypeEnum

class SecondStorageTest extends SparderBaseFunSuite with LocalMetadata {

  test("SecondStorage.enabled should not throw execution") {
    overwriteSystemProp("kylin.second-storage.class", "X")
    assertResult(false)(SecondStorage.enabled)

    SecondStorage.init(false)
    assertResult(false)(SecondStorage.enabled)
    assertResult(null)(SecondStorage.load("org.apache.kylin.secondstorage.metadata.TableFlow"))

    val testStorage = SecondStorage.load(classOf[TestStorage].getCanonicalName)
    assertResult(true)(testStorage != null)
    assertResult(true)(testStorage.isInstanceOf[TestStorage])
  }

  test("SecondStorage.configLoader") {
    val thrown = intercept[RuntimeException] {
      SecondStorage.configLoader
    }
    assert(thrown.getMessage === "second storage plugin is null")
  }

  def invokeObjectPrivateMethod[R](methodName: String, args: AnyRef*): R = {
    val rm = ru.runtimeMirror(getClass.getClassLoader)
    val instanceMirror = rm.reflect(SecondStorage)
    val methodSymbol = ru.typeOf[SecondStorage.type].decl(ru.TermName(methodName)).asMethod
    val method = instanceMirror.reflectMethod(methodSymbol)
    method(args: _*).asInstanceOf[R]
  }

  test("SecondStorage.throwException") {
    assertResult(None)(invokeObjectPrivateMethod("tryCreateDataFrame", null, null, null, null))

    ForceToTieredStorage.values().foreach(f => {
      QueryContext.current().setForcedToTieredStorage(f)
      if (f == ForceToTieredStorage.CH_FAIL_TO_DFS) {
        assertResult(None)(invokeObjectPrivateMethod("tryCreateDataFrame", null, null, null, null))
      } else {
        val thrown = intercept[InvocationTargetException] {
          invokeObjectPrivateMethod("tryCreateDataFrame", null, null, null, null)
        }
      }
    })
  }

  test("SecondStorage.lock1") {
    var dataModel: NDataModel = Mockito.mock(classOf[NDataModel])
    var dataflow: NDataflow = Mockito.mock(classOf[NDataflow])
    Mockito.when(dataflow.getProject).thenReturn("project")
    Mockito.when(dataflow.getModel).thenReturn(dataModel)
    Mockito.when(dataModel.getId).thenReturn("modelid")
    var listString = new ArrayList[String](2)
    listString.add(0, "a")
    listString.add(1, "b")

    val utility1 = Mockito.mockStatic(classOf[SecondStorageUtil])
    utility1.when(() => SecondStorageUtil.isModelEnable("project", "modelid")).thenReturn(true)
    assertResult(true)(SecondStorageUtil.isModelEnable("project", "modelid"))
    utility1.when(() => SecondStorageUtil.getProjectLocks("project")).thenReturn(listString)

    val utility2 = Mockito.mockStatic(classOf[LockTypeEnum])
    utility2.when(() => LockTypeEnum.locked(LockTypeEnum.QUERY.name(), listString)).thenReturn(true)

    assertResult(Option.empty)(SecondStorage.trySecondStorage(null, dataflow, null, ""))
    utility1.close()
    utility2.close()
  }

  test("SecondStorage.lock2") {
    var dataModel: NDataModel = Mockito.mock(classOf[NDataModel])
    var dataflow: NDataflow = Mockito.mock(classOf[NDataflow])
    Mockito.when(dataflow.getProject).thenReturn("project")
    Mockito.when(dataflow.getModel).thenReturn(dataModel)
    Mockito.when(dataModel.getId).thenReturn("modelid")
    var listString = new ArrayList[String](2)
    listString.add(0, "a")
    listString.add(1, "b")

    val utility1 = Mockito.mockStatic(classOf[SecondStorageUtil])
    utility1.when(() => SecondStorageUtil.isModelEnable("project", "modelid")).thenReturn(true)
    assertResult(true)(SecondStorageUtil.isModelEnable("project", "modelid"))
    utility1.when(() => SecondStorageUtil.getProjectLocks("project")).thenReturn(listString)

    val utility2 = Mockito.mockStatic(classOf[LockTypeEnum])
    utility2.when(() => LockTypeEnum.locked(LockTypeEnum.QUERY.name(), listString)).thenReturn(false)
    assertResult(None)(SecondStorage.trySecondStorage(null, dataflow, null, ""))

    utility1.close()
    utility2.close()
  }
}
