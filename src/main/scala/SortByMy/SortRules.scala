
package SortByMy

object SortRules {
//用ordered不行，难道是因为定义在bean类里才使用ordered（java里的compareTo方法）
  implicit object WomenOrdring extends Ordering[Women]{
  override def compare(x: Women, y: Women): Int = {
    if(x.fv==y.fv){
      x.age-y.age
    }else{
      y.fv-x.fv
    }
  }}
}