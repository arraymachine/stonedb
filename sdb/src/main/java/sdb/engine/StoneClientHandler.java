/**
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package sdb.engine;

import org.apache.log4j.Logger;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;

import sdb.engine.StoneProtocol.Vector;
import sdb.engine.StoneProtocol.QueryReply;

public class StoneClientHandler extends SimpleChannelUpstreamHandler {

  private static final Logger logger = Logger.getLogger(StoneClientHandler.class.getName());

  private volatile Channel channel;
  private final BlockingQueue<QueryReply> answer = new LinkedBlockingQueue<QueryReply>();

  private void dumpReply(QueryReply reply) {
    logger.debug("got col count: " + reply.getVectorsCount());
    for (int col = 0; col < reply.getVectorsCount(); col++) {
      Vector v = reply.getVectors(col);
      logger.debug(v);
    }
  }

  public QueryReply exec_query(StoneQueryBuilder.Query query) {
    channel.write(query.getBuilder().build());

    boolean interrupted = false;
    QueryReply reply; 
    for (;;) {
      try {
        reply = answer.take();
        break;
      } catch (InterruptedException e) {
        interrupted = true;
      }
    }

    if (interrupted) {
      Thread.currentThread().interrupt();
    }
    return reply;
  }

  @Override
  public void handleUpstream(
  ChannelHandlerContext ctx, ChannelEvent e) throws Exception {
    if (e instanceof ChannelStateEvent) {
      logger.debug(e.toString());
    }
    super.handleUpstream(ctx, e);
  }

  @Override
  public void channelOpen(ChannelHandlerContext ctx, ChannelStateEvent e)
  throws Exception {
    channel = e.getChannel();
    super.channelOpen(ctx, e);
  }

  @Override
  public void messageReceived(
  ChannelHandlerContext ctx, final MessageEvent e) {
    logger.debug("message received");
    boolean offered = answer.offer((QueryReply) e.getMessage());
    assert offered;
  }

  @Override
  public void exceptionCaught(
  ChannelHandlerContext ctx, ExceptionEvent e) {
    logger.warn(
               "Unexpected exception from downstream.",
               e.getCause());
    e.getChannel().close();
  }
}
