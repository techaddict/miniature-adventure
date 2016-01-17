package datastore

import org.scalatest.{BeforeAndAfterEach, FunSuite}

import scala.collection.mutable

class FileBackedDataStoreSpec extends FunSuite with BeforeAndAfterEach {
  var dataStore: FileBackedDataStore = _

  override def beforeEach() = {
    dataStore = new FileBackedDataStore(Config())
  }

  override def afterEach() = {
    dataStore.close()
    dataStore.clean()
  }

  test("test init()") {
    assert(dataStore.init())
    assert(dataStore.locationInfo == mutable.Map[String, Long]())
  }

  test("test close() if initialized") {
    dataStore.init()
    assert(dataStore.close())
  }

  test("test close() if not initialized") {
    assert(!dataStore.close())
  }

  test("test iterator() with empty dataStore") {
    dataStore.init()
    assert(dataStore.iterator().toSet == Set.empty[(String, String)])
  }

  test("test iterator() with data") {
    dataStore.init()
    dataStore.put("k1", "v1")
    dataStore.put("k2", "v2")
    assert(dataStore.iterator().toSet == Iterator(("k1", "v1"), ("k2", "v2")).toSet)
  }

  test("test get()") {
    dataStore.init()
    dataStore.put("k1", "v1")

    assert(dataStore.get("k1").isDefined)
    assert(dataStore.get("k1").contains("v1"))

    assert(dataStore.get("k2").isEmpty)
  }

  test("test put()") {
    dataStore.init()

    assert(dataStore.put("k1", "v1"))
    assert(!dataStore.put("k1", "v2"))
  }

  test("test recover") {
    dataStore.init()
    val filename = dataStore.underlyingFileName

    dataStore.put("k1", "v1")
    dataStore.put("k2", "v2")
    dataStore.put("k3", "v3")
    dataStore.close()

    dataStore.recover(filename)
    assert(dataStore.get("k1").contains("v1"))
    assert(dataStore.get("k2").contains("v2"))
    assert(dataStore.get("k3").contains("v3"))
    assert(dataStore.iterator().toSet == Iterator(("k1", "v1"), ("k2", "v2"), ("k3", "v3")).toSet)
  }
}
