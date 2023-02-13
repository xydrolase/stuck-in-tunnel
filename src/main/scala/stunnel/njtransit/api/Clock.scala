package stunnel.njtransit.api

trait Clock:
  def setOffset(offset: Long): Unit
  def currentTimeMillis: Long
