package org.ekito.model

import org.scalatest.{FlatSpec, Matchers}

class OTPRecordTest extends FlatSpec with Matchers {

  "An OTP Record" should "reply with your name when being greeted" in {
    val record = new OTPRecord
    record.hello("Bert") should be("Hello Bert")
  }
}
