import scala.collection.mutable

class InMemoryDataStore extends DataStore {
  private var underlying: mutable.Map[String, String] = _

  def init(): Boolean = {
    underlying = mutable.Map[String, String]()
    true
  }

  def close(): Boolean = {
    underlying = mutable.Map.empty[String, String]
    true
  }

  def iterator(): Iterator[(String, String)] = {
    underlying.iterator
  }

  def get(key: String): Option[String] = {
    underlying.get(key)
  }

  def put(key: String, value: String): Boolean = {
    if(underlying.contains(key)) {
      false
    } else {
      underlying.put(key, value)
      true
    }
  }
}
