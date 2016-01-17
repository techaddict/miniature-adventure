package datastore

import scala.collection.mutable

class InMemoryDataStore extends DataStore {
  private[datastore] var underlying: mutable.Map[String, String] = _

  def init(): Boolean = {
    if(underlying == null) {
      underlying = mutable.Map[String, String]()
      true
    } else {
      false
    }
  }

  def close(): Boolean = {
    if(underlying == null) {
      false
    } else {
      underlying = mutable.Map.empty[String, String]
      true
    }
  }

  def iterator(): Iterator[(String, String)] = {
    assertOpen()
    underlying.iterator
  }

  def get(key: String): Option[String] = {
    assertOpen()
    underlying.get(key)
  }

  //Updates not allowed for now.
  def put(key: String, value: String): Boolean = {
    assertOpen()
    if(underlying.contains(key)) {
      false
    } else {
      underlying.put(key, value)
      true
    }
  }

  def assertOpen(): Unit = assert(underlying != null, "Initialize datastore.DataStore before Accessing Data")
}
