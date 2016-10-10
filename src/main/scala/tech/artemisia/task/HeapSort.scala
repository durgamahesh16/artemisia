package org.examples.sort

/**
 * Created by chlr on 3/27/16.
 */
object HeapSort extends Sortable {

  /**
   *
   * @param xs list to be sorted
   * @return sorted list
   */
  override def sort(xs: List[Int]): List[Int] = {
    val heap = new Heap(xs)
    val output = for (item <- heap) yield item
    output.toList
  }

  /**
   * An Implementation of Heap data-structure
   * @param xs List of Integers
   */

  class Heap(xs: List[Int]) extends Iterable[Int] {

    var data: List[Node] = buildBinaryTree(xs)
    this.minHeapify()

    /**
     * Node data structure which holds the value and the optional left and right nodes of the tree
     * @param value value of the node
     * @param right Optional right child node
     * @param left Optional left child node
     */
    case class Node(var value: Int, var right: Option[Node] = None, var left: Option[Node] = None) {
      override def toString = {
        s"${left.toString} <-- $value --> ${right.toString}"
      }
    }

    /**
     * build a balanced binary tree from a list of Nodes
     *
     * @param list input list of Ints
     * @return List of Nodes with value and references to its Left and Right child nodes
     */
    private def buildBinaryTree(list: List[Int]) = {
      val node_list = list map { Node(_) }
      for ( (item, index) <- node_list zip (1 to node_list.length) ) {
        item.left = node_list.lift(2 * index - 1 )
        item.right = node_list.lift(2 * index)
      }
       node_list
    }

    /**
     * convert the binary tree to Heap tree with the least minimum value node being the root node
     *
     */
    private def minHeapify(): Unit = {

      var swapFlag = false

      for (item <- data) {
          if (item.left.isDefined)
            swapNodeValues(item, item.left.get)
          if (item.right.isDefined)
            swapNodeValues(item, item.right.get)
       }

      def swapNodeValues(parent: Node, child: Node): Unit = {
        if (parent.value > child.value) {
          val temp = parent.value
          parent.value = child.value
          child.value = temp
          swapFlag = true
        }
      }
      if (swapFlag)
        minHeapify()
    }


    /**
     * Fetch and remove the root node from the heap (smallest value in the Heap)
     * and rebuild the heap with remaining nodes
     *
     * @return the value of the root node in the heap before reHeapfying process
     */
    private def fetchHeapRootAndReHeapify: Int = {
      val output = data.headOption map { _.value }
      data = this.buildBinaryTree(data.tail map (_.value))
      this.minHeapify()
      output.get
    }

    /**
     * handy utility function to iterate over the Heap to get the data in sorted order.
     * @return Iterator object to iterate the Heap
     */
    override def iterator: Iterator[Int] = {
      new Iterator[Int] {
        override def hasNext: Boolean = data.nonEmpty
        override def next(): Int = fetchHeapRootAndReHeapify
      }
    }
  }
}
