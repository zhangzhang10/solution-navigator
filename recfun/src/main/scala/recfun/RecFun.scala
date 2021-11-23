package recfun

object RecFun extends RecFunInterface {

  def main(args: Array[String]): Unit = {
    println("Pascal's Triangle")
    for (row <- 0 to 10) {
      for (col <- 0 to row)
        print(s"${pascal(col, row)} ")
      println()
    }
  }

  /**
   * Exercise 1
   */
  def pascal(c: Int, r: Int): Int = {
    if (c == 0 || c == r) 1
    else pascal(c, r-1) + pascal(c-1, r-1)
  }

  /**
   * Exercise 2
   */
  def balance(chars: List[Char]): Boolean = {
    def foo(chars: List[Char], count: Int): Boolean = {
      if (chars.isEmpty) {
        if (count == 0) true
        else false
      } else {
        if (chars.head == '(') 
          foo(chars.tail, count+1)
        else if (chars.head == ')') 
          if (count == 0) false else foo(chars.tail, count-1)
        else
          foo(chars.tail, count)
      }
    }
    foo(chars, 0)
  }

  /**
   * Exercise 3
   */
  def countChange(money: Int, coins: List[Int]): Int = {
    if (money == 0) 1
    else if (money < 0) 0
    else if (coins.isEmpty) 0
    else {
      countChange(money - coins.head, coins) + countChange(money, coins.tail)
    }
  }
}
