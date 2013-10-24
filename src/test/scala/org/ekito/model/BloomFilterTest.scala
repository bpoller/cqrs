package org.ekito.model

import org.scalatest.{Matchers, FlatSpec}
import org.ekito.service.CountingBloomFilter
import CountingBloomFilter._


class BloomFilterTest extends FlatSpec with Matchers {

  "When adding 2 elements then " should "there should be 2 elements in the filter" in {
    val filter = CountingBloomFilter(0.0001, 4000000)
    filter <++ "Hello"
    filter <++ "Bert"

    filter.elementCount should be(2)
  }

  "When adding 2 elements and removing one then " should "there should be 1 element in the filter" in {
    val filter = CountingBloomFilter(0.0001, 4000000)
    filter <++ "Hello"
    filter <++ "Bert"
    filter --> "Hello"

    filter.elementCount should be(1)
  }

  "When adding 5 elements " should "the elements should be found back" in {
    val filter = CountingBloomFilter(0.0001, 4000000)
    val elements = "Hello" :: "Bert" :: "Where" :: "are" :: "you" :: List()
    elements.foreach(filter <++ _)
    elements.map(filter.contains(_)).foldLeft(true)((r, c) => r && c) should be(true)
  }

  "When adding 5 elements and asking for an inexistent element then it" should "not be found" in {
    val filter = CountingBloomFilter(0.0001, 4000000)
    val elements = "Hello" :: "Bert" :: "Where" :: "are" :: "you" :: List()
    elements.foreach(filter <++ _)
    filter.contains("Rolf") should be(false)
  }

}
