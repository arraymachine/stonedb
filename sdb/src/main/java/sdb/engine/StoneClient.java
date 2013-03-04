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

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.ExecutionException;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;

import sdb.engine.StoneProtocol.Vector;
import sdb.engine.StoneProtocol.QueryReply;
import sdb.engine.StoneQueryBuilder.Query;

public class StoneClient {

  private static Logger logger = Logger.getLogger(StoneClient.class.getName());
  InetSocketAddress[] svr_addrs;
  ExecutorService pool;
  ClientBootstrap bootstrap;
  Channel[] channels;
  StoneClientHandler handlers[];

  final class QueryTask implements Callable<QueryReply> {

    int srv_id;
    Query query;
    
    public QueryTask(Query query, int srv_id) {
      this.srv_id = srv_id;
      this.query = query;
    }

    public QueryReply call() {
      StoneClientHandler handler = handlers[srv_id];
      QueryReply reply = handler.exec_query(query);
      return reply;
    }

  }
  
  public StoneClient(String[] hosts, int[] ports) {
    svr_addrs = new InetSocketAddress[hosts.length];
    for (int i = 0; i < hosts.length; i++) {
      svr_addrs[i] = new InetSocketAddress(hosts[i], ports[i]);
    }
  }

  private static void dumpReply(QueryReply reply) {
    logger.info("got col count: " + reply.getVectorsCount());
    for (int col = 0; col < reply.getVectorsCount(); col++) {
      Vector v = reply.getVectors(col);
      System.out.println(v);
    }
  }

  public Object run_distributed_query(Query query) {
    ArrayList<QueryTask> tasks = new ArrayList<QueryTask>(svr_addrs.length);

    StoneMapReduce mapReduce = new StoneMapReduce(query);
    Query map_query = mapReduce.get_map_query();

    try {
      int count = svr_addrs.length;
      for (int id = 0; id < count; id++) 
        tasks.add(new QueryTask(map_query, id));
      ArrayList<QueryReply> replies = new ArrayList<QueryReply>(count);
      Object[] futures = pool.invokeAll(tasks).toArray();

      int reply_count = 0;

      while (reply_count < count) {
        for (int i = 0; i < count; i++) {
          Future<QueryReply> f = (Future<QueryReply>)futures[i];
          if (f !=null) {
            if (f.isDone()) {
              replies.add(f.get());
              futures[i] = null;
              reply_count++;
            }
          }
        }
      }
      return mapReduce.reduce(replies);
    } catch (InterruptedException e) {
      System.out.println(e);
    } catch (ExecutionException e) {
      e.printStackTrace();
    }
    return null;
  }
  
  public QueryReply run_single_query(int id, Query query) {
    StoneClientHandler handler = handlers[id];
    QueryReply reply = handler.exec_query(query);
    return reply;
  }


  public void prep() {
    bootstrap = new ClientBootstrap(
                                    new NioClientSocketChannelFactory(
                                        Executors.newCachedThreadPool(),
                                        Executors.newCachedThreadPool()));
    bootstrap.setPipelineFactory(new StoneClientPipelineFactory());
    pool = Executors.newFixedThreadPool(svr_addrs.length);
    channels = new Channel[svr_addrs.length];
    handlers = new StoneClientHandler[svr_addrs.length];
    for (int i = 0; i < svr_addrs.length; i++) {
      ChannelFuture connectFuture = bootstrap.connect(svr_addrs[i]); 
      channels[i] = connectFuture.awaitUninterruptibly().getChannel();
      handlers[i] =  channels[i].getPipeline().get(StoneClientHandler.class);
    }
  }

  protected void finalize() throws Throwable {
    try {
      shutdown(); 
    } finally {
      super.finalize();
    }
  }
  
  public void shutdown() {
    if (channels != null) {
      for (int i = 0; i < svr_addrs.length; i++) {
        channels[i].close().awaitUninterruptibly();
      }
      channels = null;
    }
    
    if (bootstrap != null) {
      bootstrap.releaseExternalResources();
      bootstrap = null;
    }
    if (pool != null) {
      pool.shutdownNow();
      pool = null;
    }
  }
}
