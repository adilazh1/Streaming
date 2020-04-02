package org.adilazh1.utils

import java.io.IOException
import java.net.{DatagramPacket, DatagramSocket, InetAddress}

class WriterClient {

  private val socket : DatagramSocket = new DatagramSocket()
  private val buffer: Array[Byte] = new Array[Byte](4096);
  private val address : InetAddress = InetAddress.getByName("localhost");

  def close():Unit ={
    this.socket.close()
  }

  def write(arrayBytes:Array[Byte]):Unit = {

    for(i <- 0 until arrayBytes.length) {
      this.buffer(i) = arrayBytes(i)
    }

    val packet = new DatagramPacket(this.buffer, arrayBytes.length, this.address, 4444)

    try{

      val reply = new DatagramPacket(packet.getData(), packet.getLength(), packet.getAddress(), packet.getPort());
      this.socket.send(reply);

    }catch {
      case e : IOException => println(s"Error sending packet + ${e.getMessage}")
    }
  }

}
