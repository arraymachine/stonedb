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

import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;

public class StoneAdminServerHandler extends SimpleChannelUpstreamHandler {

  private static Logger logger = Logger.getLogger(StoneAdminServerHandler.class.getName());

  @Override
  public void handleUpstream(
  ChannelHandlerContext ctx, ChannelEvent e) throws Exception {
    if (e instanceof ChannelStateEvent) {
      logger.info(e.toString());
    }
    super.handleUpstream(ctx, e);
  }

  private StoneAdminProtocol.CommandResult.Builder runCommand(StoneAdminProtocol.ExecuteCommand cmd) {
    StoneAdminProtocol.CommandResult.Builder result = StoneAdminProtocol.CommandResult.newBuilder();
    StoneAdminProtocol.Command command = cmd.getCmd();
    StoneAdminProtocol.Entry.Builder entry;

    switch (command) {
      case GET_MASTER:
        logger.debug("Execute Command: GET_MASTER");
        entry = StoneAdminProtocol.Entry.newBuilder();
        entry.setKey("master");
        entry.setStringValue("master");
        result.addResults(entry.build());
        break;
      case GET_SERVER_PROPERTIES:
          logger.debug("Execute Command: GET_SERVER_PROPERTIES");
          for (StoneAdminProtocol.Entry arg : cmd.getArgsList()){
          try {
            String key = arg.getKey();
            logger.debug("Get server property: " + key);
            Runtime runtime = Runtime.getRuntime();
            Object r = runtime.getClass().getMethod(key).invoke(runtime);
            if (r != null) {
              if (r.getClass() == Long.class){
                entry = StoneAdminProtocol.Entry.newBuilder();
                entry.setKey(key);
                entry.setDoubleValue((Long)r);
                result.addResults(entry.build());
              }
              if (r.getClass() == Integer.class){
                entry = StoneAdminProtocol.Entry.newBuilder();
                entry.setKey(key);
                entry.setIntValue((Integer)r);
                result.addResults(entry.build());
              }
            }
          } catch (Exception e){
            e.printStackTrace();
          }
        }
        break;
      case GET_DATASERVERS:
        logger.debug("Execute Command: GET_DATASERVERS");
        break;
      case CREATE_DATASERVER:
        logger.debug("Execute Command: CREATE_DATASERVER");
        break;
      case DELETE_DATASERVER:
        logger.debug("Execute Command: DELETE_DATASERVER");
        break;
      case START_SERVER:
        logger.debug("Execute Command: START_SERVER");
        break;
      case SHUTDOWN_SERVER:
        logger.debug("Execute Command: SHUTDOWN_SERVER");
        (new Thread(new shutdownServer())).start();
        entry = StoneAdminProtocol.Entry.newBuilder();
        entry.setKey("shutdown");
        entry.setStringValue("shutdown");
        result.addResults(entry.build());
        break;
    }
    return result;
  }

  private class shutdownServer implements Runnable {
      public void run(){
        logger.debug("Run: shutdown server");
        try{
          Thread.sleep(1000);
          Runtime.getRuntime().exit(1);
        }catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
  }

  @Override
  public void messageReceived(
  ChannelHandlerContext ctx, MessageEvent e) {
    StoneAdminProtocol.ExecuteCommand cmd = (StoneAdminProtocol.ExecuteCommand) e.getMessage();
    logger.debug("Command: " + cmd.getCmd());
    logger.debug("Arguments count" + cmd.getArgsCount());

    StoneAdminProtocol.CommandResult.Builder result = runCommand(cmd);
    logger.debug("Write: result");
    e.getChannel().write(result.build());
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
