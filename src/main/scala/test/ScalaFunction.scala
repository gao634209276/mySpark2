package test
//class内部类似于java的构造器，可以直接声明变量和函数，通过new一个对象实例调用
class ScalaFunction extends Serializable{
	val str = "This is a test"// 常量，在初始化一个对象时候自动执行
	// 可定义一个普通函数，因为函数和值都是一等公民
	def simpleFun(i: Int) = i + 1
	// 值函数即返回值是一个函数，比python的lamda表达式更加灵活，可以用val和def定义
	val valFun1 = (i: Int) => i.toString // 这里调用java的toString方法
	def valFun2: (Int) => String = _.toString
}
// object是class的伴生类，定义类似java的static的方法和常量,外部可以通过类名直接调用
object ScalaFunction {
	// 高级参数函数，参数中定义一种类型函数，在函数体中使用
	def argFun(fun: (Int) => String) = fun(1)
	def main(args: Array[String]) {
		val scala = new ScalaFunction
		argFun(scala.valFun1)//在main函数中调用
	}
}
