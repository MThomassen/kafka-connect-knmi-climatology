case class DTO(valueA: String, valueB: String, valueC: Int)

val a: DTO = DTO("", "", 1)


val i = a.productIterator

i.next()
val elements = for {i <- a.productIterator} yield i;

elements.toList