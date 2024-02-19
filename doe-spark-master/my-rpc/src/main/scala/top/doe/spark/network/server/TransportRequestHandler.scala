package top.doe.spark.network.server

import com.google.common.base.Throwables
import io.netty.channel.{Channel, ChannelFuture}
import io.netty.util.concurrent.Future
import top.doe.spark.network.RpcHandler
import top.doe.spark.network.buffer.NioManagedBuffer
import top.doe.spark.network.client.{RpcResponseCallBack, TransportClient}
import top.doe.spark.network.protocol.{Encodable, OneWayMessage, RequestMessage, RpcFailure, RpcRequest, RpcResponse}

import java.nio.ByteBuffer

class TransportRequestHandler(
  val channel: Channel,
  val client: TransportClient,
  val rpcHandler: RpcHandler
) {

  def channelActive(): Unit = {
    rpcHandler.channelActive(client)
  }

  def handle(requestMessage: RequestMessage): Unit = {
    requestMessage match {
      //匹配同步类型的消息
      case _: RpcRequest =>
        processRpcRequest(requestMessage.asInstanceOf[RpcRequest])
      //匹配异步类型的消息
      case _: OneWayMessage =>
        processOneWayMessage(requestMessage.asInstanceOf[OneWayMessage])
      case _ =>
        throw new IllegalArgumentException("消息没有匹配上：" + requestMessage)
    }

  }

  //处理同步消息
  private def processRpcRequest(request: RpcRequest): Unit = {
    //同步的消息会有回调
    try {
      rpcHandler.receive(client, request.body().nioByteBuffer(), new RpcResponseCallBack {
        override def onSuccess(response: ByteBuffer): Unit = {
          respond(new RpcResponse(request.requestId, new NioManagedBuffer(response)))
        }

        override def onFailure(e: Throwable): Unit = {
          respond(new RpcFailure(request.requestId, Throwables.getStackTraceAsString(e)))
        }

      })
    } catch {
      case e: Exception => {
        respond(new RpcFailure(request.requestId, Throwables.getStackTraceAsString(e)))
      }
    } finally {
      request.body().release()
    }

  }

  //处理异步消息
  private def processOneWayMessage(message: OneWayMessage): Unit = {
    //同步的消息会没有回调
    try {
      rpcHandler.receive(client, message.body().nioByteBuffer())
    } catch {
      case e: Exception => {
        println("出现了溢出：" + e)
      }
    } finally {
      message.body().release()
    }
  }

  //返回个客户端的响应信息
  private def respond(result: Encodable): ChannelFuture = {
    val address = channel.remoteAddress()
    channel.writeAndFlush(result).addListener((future: Future[_ >: Void]) => {
      if(future.isSuccess) {
        println(s"消息：$result, 返回给客户端成功, 发送到了 $address" )
      } else {
        println("消息返回给客户端失败")
        channel.close()
      }

    })


  }


}
