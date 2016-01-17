import org.scalatest.FunSuite

import scala.collection.mutable

class InMemoryDataStoreSpec extends FunSuite {
  test("test init()") {
    val dataStore = new InMemoryDataStore
    dataStore.init()
    assert(dataStore == mutable.Map[String, String]())
  }

  test("test close() if initialized") {
    val dataStore = new InMemoryDataStore
    dataStore.init()
    assert(dataStore.close())
  }

  test("test close() if not initialized") {
    val dataStore = new InMemoryDataStore
    assert(!dataStore.close())
  }

  test("test iterator() with empty dataStore") {
    val dataStore = new InMemoryDataStore
    dataStore.init()
    assert(dataStore.iterator().toSet == Set.empty[(String, String)])
  }

  test("test iterator() with data") {
    val dataStore = new InMemoryDataStore
    dataStore.init()
    dataStore.put("k1", "v1")
    dataStore.put("k2", "v2")
    assert(dataStore.iterator().toSet == Iterator(("k1", "v1"), ("k2", "v2")).toSet)
  }

  test("test get()") {
    val dataStore = new InMemoryDataStore
    dataStore.init()
    dataStore.put("k1", "v1")

    assert(dataStore.get("k1").isDefined)
    assert(dataStore.get("k1") == Some("v1"))

    assert(dataStore.get("k2").isEmpty)
  }

  test("test put()") {
    val dataStore = new InMemoryDataStore
    dataStore.init()

    assert(dataStore.put("k1", "v1"))
    assert(dataStore.put("k1", "v2"))
  }
}
