package funsets

/**
 * This class is a test suite for the methods in object FunSets.
 *
 * To run this test suite, start "sbt" then run the "test" command.
 */
class FunSetSuite extends munit.FunSuite:

  import FunSets.*

  test("contains is implemented") {
    assert(contains(x => true, 100))
  }

  /**
   * When writing tests, one would often like to re-use certain values for multiple
   * tests. For instance, we would like to create an Int-set and have multiple test
   * about it.
   *
   * Instead of copy-pasting the code for creating the set into every test, we can
   * store it in the test class using a val:
   *
   *   val s1 = singletonSet(1)
   *
   * However, what happens if the method "singletonSet" has a bug and crashes? Then
   * the test methods are not even executed, because creating an instance of the
   * test class fails!
   *
   * Therefore, we put the shared values into a separate trait (traits are like
   * abstract classes), and create an instance inside each test method.
   *
   */

  trait TestSets:
    val s1 = singletonSet(1)
    val s2 = singletonSet(2)
    val s3 = singletonSet(3)

  /**
   * This test is currently disabled (by using @Ignore) because the method
   * "singletonSet" is not yet implemented and the test would fail.
   *
   * Once you finish your implementation of "singletonSet", remove the
   * .ignore annotation.
   */
  test("singleton set one contains one") {

    /**
     * We create a new instance of the "TestSets" trait, this gives us access
     * to the values "s1" to "s3".
     */
    new TestSets:
      /**
       * The string argument of "assert" is a message that is printed in case
       * the test fails. This helps identifying which assertion failed.
       */
      assert(contains(s1, 1), "Singleton")
  }

  test("union contains all elements of each set") {
    new TestSets:
      val s = union(s1, s2)
      assert(contains(s, 1), "Union 1")
      assert(contains(s, 2), "Union 2")
      assert(!contains(s, 3), "Union 3")
  }

  test("intersect contains mutual elements") {
    new TestSets:
      val s = union(s1, s2)
      val t = union(s2, s3)
      val x = intersect(s, t)
      assert(contains(x, 2), "Intersect 1")
      assert(!contains(x, 1), "Intersect 2")
      assert(!contains(x, 3), "Intersect 3")
  }

  test("diff works properly") {
    new TestSets:
      val s = diff(s1, s2)
      assert(contains(s, 1), "Diff 1")
      assert(!contains(s, 2), "Diff 2")
  }

  test("filter works properly") {
    new TestSets:
      val s = union(union(s1, s2), s3)
      val t = filter(s, (x: Int) => x > 1)
      assert(contains(t, 2), "Filter 1")
      assert(contains(t, 3), "Filter 2")
      assert(!contains(t, 1), "Filter 3")
  }

  test("forall works properly") {
    new TestSets:
      val s4 = singletonSet(-1000)
      val s = union(union(union(s1, s2), s3), s4)
      assert(forall(s, (x: Int) => x < 7), "Forall 1")
  }

  test("exists works properly") {
    new TestSets:
      val s4 = singletonSet(-1000)
      val s = union(union(union(s1, s2), s3), s4)
      assert(exists(s, (x: Int) => x < 0), "Exists 1")
      assert(exists(s, (x: Int) => x < 7), "Exists 2")
      assert(!exists(s, (x: Int) => x > 3), "Exists 3")
  }

  test("map works properly") {
    new TestSets:
      val s = union(union(s1, s2), s3)
      val t = map(s, (x: Int) => x * 3)
      assert(contains(t, 3), "Map 1")
      assert(contains(t, 6), "Map 2")
      assert(contains(t, 9), "Map 3")
      val w = intersect(s1, s2)
      val v = map(w, (x: Int) => x + 1)
      assert(empty(v), "Map 4")
  }

  import scala.concurrent.duration.*
  override val munitTimeout = 10.seconds
