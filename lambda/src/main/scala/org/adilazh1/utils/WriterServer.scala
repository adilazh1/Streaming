package org.adilazh1.utils

import java.net.{DatagramPacket, DatagramSocket, InetAddress}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}

class WriterServer extends Thread{

  private val config = new Configuration()
  config.set("fs.defaultFS", "hdfs://quickstart.cloudera:8020")
  config.addResource(new Path("/etc/hadoop/conf/core-site.xml"))
  private val fileSystem : FileSystem =  FileSystem.get(config)
  private val FILENAME = s"/user/cloudera/lambda/+${String.valueOf(System.currentTimeMillis())}"
  private var writer: FSDataOutputStream = fileSystem.create(new Path(FILENAME))
  private val socket = new DatagramSocket(4444)
  private var running = true
  private val address : InetAddress = InetAddress.getByName("localhost");

  def close():Unit ={
    this.socket.close
    this.writer.flush
    this.writer.close
    this.fileSystem.close

  }

  def finish(): Unit = {
    this.running = false
  }

  import java.io.IOException
  import java.net.SocketException

  override def run(): Unit = {
    try {
      val buffer = new Array[Byte](4096)

      while ( this.running ) {

        val packet = new DatagramPacket(buffer, buffer.length)
        packet.setAddress(address)
        packet.setPort(4444)
        socket.receive(packet)
        val str = new String(packet.getData(), 0, packet.getLength());
        val bytes = packet.getLength
        val data = packet.getData
        val toWrite = new Array[Byte](bytes)
        for (i <- 0 until bytes) {
          toWrite(i) = data(i)
        }
        this.writer.write(toWrite)
      }
      this.close
    } catch {
      case e: SocketException =>
        e.printStackTrace()
      case e: IOException =>
        e.printStackTrace()
    }
  }
}
