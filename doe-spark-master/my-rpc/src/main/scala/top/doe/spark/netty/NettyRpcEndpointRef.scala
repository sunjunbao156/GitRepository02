package top.doe.spark.netty

import top.doe.spark.network.client.TransportClient
import top.doe.spark.rpc.{AbortableRpcFuture, RpcEndpointAddress, RpcEndpointRef, RpcTimeout}
import top.doe.spark.util.{ByteBufferInputStream, ByteBufferOutputStream, RpcAddress}

import java.io.{DataInputStream, DataOutputStream, ObjectInputStream, ObjectOutputStream}
import java.nio.ByteBuffer
import scala.concurrent.Future
import scala.reflect.ClassTag

class NettyRpcEndpointRef(
  endpointAddress: RpcEndpointAddress,
  @transient var nettyEnv: NettyRpcEnv
) extends RpcEndpointRef{

  var client: TransportClient = _

  override def name: String = endpointAddress.name

  override def address: RpcAddress = endpointAddress.rpcAddress

  //发送异步消息
  override def send(message: Any): Unit = {
    nettyEnv.send(new RequestMessage(nettyEnv.address, this, message))
  }

  //ask 发送同步消息
  override def ask[T: ClassTag](message: Any, timeout: RpcTimeout): Future[T] = {
    askAbortable(message, timeout).future
  }

  //发送同步请求，如果出现异常，对异常进行处理
  override def askAbortable[T: ClassTag](message: Any, timeout: RpcTimeout): AbortableRpcFuture[T] = {
    //使用nettyEvn发消息
    nettyEnv.askAbortable(new RequestMessage(nettyEnv.address, this, message), timeout)
  }

  private def readObject(in: ObjectInputStream): Unit = {
    in.defaultReadObject()
    nettyEnv = NettyRpcEnv.currentEnv.value
    client = NettyRpcEnv.currentClient.value
  }

  private def writeObject(out: ObjectOutputStream): Unit = {
    out.defaultWriteObject()
  }

}

class RequestMessage(
  val senderAddress: RpcAddress,
  val receiver: NettyRpcEndpointRef,
  val content: Any
) {
  //如果是远程的消息必须序列化，本地消息可不必序列化
  //将消息进行序列化，返回ByteBuffer
  def serialize(nettyEnv: NettyRpcEnv): ByteBuffer = {
    val bos = new ByteBufferOutputStream()
    val out = new DataOutputStream(bos)
    try {
      writeRpcAddress(out, senderAddress)
      writeRpcAddress(out, receiver.address)
      out.writeUTF(receiver.name)
      // 调用NettyRpcEnv的serializeStream方法
      val s = nettyEnv.serializeStream(out)
      try {
        s.writeObject(content)
      } finally {
        s.close()
      }
    } finally {
      out.close()
    }
    bos.toByteBuffer
  }

  private def writeRpcAddress(out: DataOutputStream, rpcAddress: RpcAddress): Unit = {
    if (rpcAddress == null) {
      out.writeBoolean(false)
    } else {
      out.writeBoolean(true)
      out.writeUTF(rpcAddress.host)
      out.writeInt(rpcAddress.port)
    }
  }

  override def toString: String = s"RequestMessage($senderAddress, $receiver, $content)"
}

object RequestMessage {

  private def readRpcAddress(in: DataInputStream): RpcAddress = {
    val hasRpcAddress = in.readBoolean()
    if (hasRpcAddress) {
      RpcAddress(in.readUTF(), in.readInt())
    } else {
      null
    }
  }

  def apply(nettyEnv: NettyRpcEnv, client: TransportClient, bytes: ByteBuffer): RequestMessage = {
    val bis = new ByteBufferInputStream(bytes)
    val in = new DataInputStream(bis)
    try {
      val senderAddress = readRpcAddress(in)
      val endpointAddress = RpcEndpointAddress(readRpcAddress(in), in.readUTF())
      val ref = new NettyRpcEndpointRef(endpointAddress, nettyEnv)
      ref.client = client
      new RequestMessage(
        senderAddress,
        ref,
        nettyEnv.deserialize(client, bytes))
    } finally {
      in.close()
    }
  }
}
