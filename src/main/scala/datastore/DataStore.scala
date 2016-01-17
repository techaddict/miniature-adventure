package datastore

trait DataStore {
  def init(): Boolean
  def close(): Boolean
  def iterator(): Iterator[(String, String)]
  def get(key: String): Option[String]
  def put(key: String, value: String): Boolean
}
