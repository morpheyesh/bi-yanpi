package bi.megam



trait Connectors
object Connectors {
  def apply(source: String) = source match {
    case "mysql" => new Mysql()
    case "postgresql" => new Postgresql()
  }
}

  private class Mysql extends Connectors {}
  private class Postgresql extends Connectors {}
