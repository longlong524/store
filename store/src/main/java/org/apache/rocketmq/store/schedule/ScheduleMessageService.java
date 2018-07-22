/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.store.schedule;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.store.ConsumeQueue;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.DelayMessageStore;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.store.MappedFile;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.store.TimeWheel;
import org.apache.rocketmq.store.config.StorePathConfigHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ScheduleMessageService  implements Runnable{
    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    public static final String SCHEDULE_TOPIC = "SCHEDULE_TOPIC_XXXX";
    
    public static final String SCHEDULE_TOPIC2 = "SCHEDULE_CONSUME_TOPIC_XXXX";

    //last flush time
    long lastTime=System.currentTimeMillis();
    //current execute time
    long time=System.currentTimeMillis();

    private  ExecutorService executor;
    
    private ConsumeQueue consumeQueue;
    //consume queue startIndex
    private int startIndex;
    

    private MappedByteBuffer mappedByteBuffer;

    private final DelayMessageStore delayMessageStore;
    private final DefaultMessageStore defaultMessageStore;

    //now timeWheel ID
    private long nowQueueId;
    //now timeWheel
    private TimeWheel timeWheel;
    //next timeWheel ID
    private long nowQueueId2;
    //next timewheel
    private TimeWheel timeWheel2;
    
    int size=0;
    int size2=0;
    int delaysize=0;
    int delaysize2=0;
    int delaysize3=0;
    int error=0;
    
    //is shutdown?
    private boolean shutdown=false;
    //read data to timewheel, file position.
    private long nextPosition=0;
    
    
    public void append(DispatchRequest req) {
	    final int tranType = MessageSysFlag.getTransactionValue(req.getSysFlag());
	    if (tranType == MessageSysFlag.TRANSACTION_NOT_TYPE
	        || tranType == MessageSysFlag.TRANSACTION_COMMIT_TYPE) {
	        // Delay Delivery
	    		int ll=this.getDelayTimeLevel(req);
	        if ( ll> 0) {
				this.consumeQueue.putMessagePositionInfoWrapper(req);
				this.consumeQueue.flush(8);
	        }
	    }
}

    private void append2(long time,MessageExt msgExt) {
    		
        
    		int ll=msgExt.getDelayTimeLevel();
        if ( ll> 0) {
        		
        		long queueId=((ll*1000+msgExt.getBornTimestamp())/1000)/this.defaultMessageStore
        				.getMessageStoreConfig().getDelayLogInterval();
        		
        		if(queueId>nowQueueId&&queueId>nowQueueId+1) {
        			
        			this.delayMessageStore.putMessage(DeliverDelayedMessageTimerTask.messageTimeup(msgExt));
        			this.delaysize3++;
        		}else {
        			if((time>=(ll*1000+msgExt.getBornTimestamp()))||(queueId<nowQueueId)||
        					(time/1000>=(ll*1000+msgExt.getBornTimestamp())/1000)) {
        				// expired
        				MessageExtBrokerInner msgInner=DeliverDelayedMessageTimerTask.messageTimeup(msgExt);
			        
        				clearMsg(msgInner);
        				
        				PutMessageResult putMessageResult =
                                ScheduleMessageService.this.defaultMessageStore
                                    .putMessage(msgInner);
        				if (putMessageResult != null
                                && putMessageResult.getPutMessageStatus() == PutMessageStatus.PUT_OK) {
        					this.size++;
        					//this.log.error("put size "+(this.size++)+":"+this.delaysize
        					//		+":"+this.delaysize2
        					//		+":"+this.delaysize3);
        					return;
                    } else {
                        // XXX: warn and notify me
                        log.error(
                            "ScheduleMessageService, a message time up append2,"+putMessageResult.getPutMessageStatus() +" but reput it failed, topic: {} msgId {}",
                            msgExt.getTopic(), msgExt.getMsgId());
                        
                        return;
                    }
        			}else {
        				// not expired
        				if(queueId==nowQueueId) {
        					this.log.error("put timewheel size "+(this.delaysize++));
        					this.timeWheel.put(ll*1000+msgExt.getBornTimestamp(), msgExt);
        					PutMessageResult putMessageResult =
                                    ScheduleMessageService.this.delayMessageStore
                                        .putMessage(DeliverDelayedMessageTimerTask.messageTimeup(msgExt));
            				if (putMessageResult != null
                                    && putMessageResult.getPutMessageStatus() == PutMessageStatus.PUT_OK) {
            					return;
                        } else {
                            // XXX: warn and notify me
                            log.error(
                                "ScheduleMessageService, a message put it to delay log failed,"+putMessageResult.getPutMessageStatus() +" topic: {} msgId {}",
                                msgExt.getTopic(), msgExt.getMsgId());
                            
                            return;
                        }
        				}else {
        					if(queueId==nowQueueId+1&&(nowQueueId2==-1||this.timeWheel2==null)) {
        						PutMessageResult putMessageResult =
                                        ScheduleMessageService.this.delayMessageStore
                                            .putMessage(DeliverDelayedMessageTimerTask.messageTimeup(msgExt));
            					this.log.error("put delayMessageStore size "+(this.delaysize2++));

        						if (putMessageResult != null
                                        && putMessageResult.getPutMessageStatus() == PutMessageStatus.PUT_OK) {
                					return;
                            } else {
                                // XXX: warn and notify me
                                log.error(
                                    "ScheduleMessageService, a message put it to delay log failed,"+putMessageResult.getPutMessageStatus() +" topic: {} msgId {}",
                                    msgExt.getTopic(), msgExt.getMsgId());
                                
                                return;
                            }
            				}else {
            					this.log.error("put delayMessageStore size "+(this.delaysize3++));

            					this.timeWheel2.put(ll*1000+msgExt.getBornTimestamp(), msgExt);
            					PutMessageResult putMessageResult =
                                        ScheduleMessageService.this.delayMessageStore
                                            .putMessage(DeliverDelayedMessageTimerTask.messageTimeup(msgExt));
                				if (putMessageResult != null
                                        && putMessageResult.getPutMessageStatus() == PutMessageStatus.PUT_OK) {
                					return;
                            } else {
                                // XXX: warn and notify me
                                log.error(
                                    "ScheduleMessageService, a message put it to delay log "+this.timeWheel2+" failed,"+putMessageResult.getPutMessageStatus() +" topic: {} msgId {}",
                                    msgExt.getTopic(), msgExt.getMsgId());
                                
                                return;
                            }
            				}
        				}
        			}
        		}
            		
        }
    }
    
    
    
    
	public  void persist() {
		this.mappedByteBuffer.force();
		this.consumeQueue.flush(0);
		this.delayMessageStore.flush();
	}

	private void clearMsg(MessageExtBrokerInner msgInner) {
    		MessageAccessor.clearProperty(msgInner, MessageConst.PROPERTY_DELAY_TIME_LEVEL);
		msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));

    }
   
    private void clearExpiredLog(long queueId) {
    		this.persist();
    		this.delayMessageStore.remove(queueId);
    		this.consumeQueue.deleteExpiredFileByIndex(startIndex);
    		
    }
    
    @Override
	public void run() {
    		try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    		if(this.mappedByteBuffer.getLong(8)!=0) {
    			this.time=this.mappedByteBuffer.getLong(8);
    			if(this.timeWheel!=null&&this.time!=0) {
        			this.timeWheel.setStartIndex(this.time);
        		}
    		}
    		
    		System.out.println("wheel "+this.timeWheel.getSize());

    		while(!shutdown) {
    			time=time+1000;
    			if(System.currentTimeMillis()<time) {
    				time=System.currentTimeMillis();
    			}
    			if(System.currentTimeMillis()-lastTime>=this.defaultMessageStore.getMessageStoreConfig().getFlushDelayLogInterval()) {
    				lastTime=System.currentTimeMillis();
    				this.persist();
    			}
    			SelectMappedBufferResult smb=this.consumeQueue.getIndexBuffer(startIndex);
    			if(smb!=null&&smb.getByteBuffer()!=null) {
    				ByteBuffer buffer=smb.getByteBuffer();
    				long phyOffset = buffer.getLong();
                int size = buffer.getInt();
                if(size>0) {
                		MessageExt msgExt =
                        ScheduleMessageService.this.defaultMessageStore.lookMessageByOffset(
                        		phyOffset, size);
                		if(msgExt!=null) {
                			this.append2(time, msgExt);
    	    					startIndex++;
        					this.log.error("startIndex size "+(startIndex));
        					this.log.error("put size "+(this.size)+":"+this.delaysize
        							+":"+this.delaysize2
        							+":"+this.delaysize3);

    	    					this.mappedByteBuffer.putLong(0,startIndex);
                		}else {
                			this.log.error("NULL MessageExt "+(startIndex)+":"+phyOffset
                					+":"+size);
                		}
	    				
	    			}
    			}
    			if(this.timeWheel!=null) {
    				List<Object> obs=this.timeWheel.get(time);
    				if(obs!=null&&obs.size()>0) {
    					for(Object o:obs) {
    						MessageExt msgE=(MessageExt)o;
    						MessageExtBrokerInner msgInner=DeliverDelayedMessageTimerTask.messageTimeup(msgE);
    						clearMsg(msgInner);
    						PutMessageResult putMessageResult =
                                    ScheduleMessageService.this.defaultMessageStore
                                        .putMessage(msgInner);
    						this.size2++;
                        	this.log.error("put size "+(this.size)+":"+(this.size2)+":"
                        		+this.delaysize
        							+":"+this.delaysize2
        							+":"+this.delaysize3+":"+error);
                        if (putMessageResult != null
                            && putMessageResult.getPutMessageStatus() == PutMessageStatus.PUT_OK) {
                        		
                        } else {
                        		error++;
                            // XXX: warn and notify me
                            log.error(
                                "ScheduleMessageService, a message time up,  run2 but reput it failed,"+putMessageResult.getPutMessageStatus() +" topic: {} msgId {}",
                                msgInner.getTopic(), msgInner.getMsgId());
                            
                        }
    					}
    				}
    				
    			}
			this.mappedByteBuffer.putLong(8,time);

    			
    			long nowId=time/1000/this.defaultMessageStore.getMessageStoreConfig().getDelayLogInterval();
    			if(nowId>this.nowQueueId&&this.nowQueueId2!=-1) {
    				clearExpiredLog(nowQueueId);
    				this.nowQueueId=this.nowQueueId2;
    				this.timeWheel=this.timeWheel2;
    				this.timeWheel2=null;
    				this.nowQueueId2=-1;
    				this.nextPosition=0;
    			}
    			if((nowQueueId+1)*1000*this.defaultMessageStore
    					.getMessageStoreConfig().getDelayLogInterval()-time<=
    					this.defaultMessageStore.getMessageStoreConfig().getDelayLogInterval()/5*1000
    					&&this.nowQueueId2==-1) {
    				if(this.timeWheel2==null) {
    					this.timeWheel2=new TimeWheel(this.defaultMessageStore.getMessageStoreConfig(), 
    							(nowQueueId+1)*1000*this.defaultMessageStore.getMessageStoreConfig()
    							.getDelayLogInterval(),this.time);
    					
    				}
    				if(this.nowQueueId2==-1) {
    					boolean me=loadNextTimeWheel(nowQueueId+1,this.timeWheel2);
    					if(!me) {
    						this.nowQueueId2=nowQueueId+1;
    						this.nextPosition=0;
    					}
    				}
    			}
    		}
    		this.persist();
	}
    
    
    private boolean loadNextTimeWheel(long l,TimeWheel tw) {
    		for(int i=0;i<this.delayMessageStore.getMessageStoreConfig().getReadDelayLogOnce();i++) {
	    		MessageExt me=this.delayMessageStore.lookMessageByOffset(l, nextPosition);
	    		if(me==null) {
	    			return false;
	    		}
	    		nextPosition=nextPosition+me.getStoreSize();
	    		tw.put(me.getBornTimestamp()+me.getDelayTimeLevel()*1000, me);
    		}
    		return true;
	}

	public int getDelayTimeLevel(DispatchRequest req) {
    		Map<String,String> pros=req.getPropertiesMap();
    		if(pros!=null) {
	        String t = pros.get(MessageConst.PROPERTY_DELAY_TIME_LEVEL);
	        if (t != null) {
	            return Integer.parseInt(t);
	        }
    		}

        return 0;
    }
	
	public void destroy() {
		this.delayMessageStore.destroy();
		this.consumeQueue.destroy();
		this.shutdown();
	}
    
    
    public ScheduleMessageService(final DefaultMessageStore defaultMessageStore) {
        this.defaultMessageStore = defaultMessageStore;
        this.delayMessageStore=new DelayMessageStore(defaultMessageStore);
        this.executor=Executors.newSingleThreadExecutor();
        consumeQueue=new ConsumeQueue(SCHEDULE_TOPIC2, 
        		0, defaultMessageStore.getMessageStoreConfig().getStorePathConsumeDelayLog(),
        		defaultMessageStore.getMessageStoreConfig().getMapedFileSizeConsumeQueue(),
        		defaultMessageStore);
        
    }

    
   


    public void start() {
    		this.shutdown=false;
    		Long time=this.mappedByteBuffer.getLong(8);
    		if(time==null) {
    			time=0L;
    		}
    		long nowTime=System.currentTimeMillis();
    		if(time==0) {
    			File dirLogic = new File(this.defaultMessageStore.getMessageStoreConfig().getStorePathDelayLog());
    	        
    	        File[] fileQueueIdList = dirLogic.listFiles();
    	        if (fileQueueIdList != null) {
        	        Arrays.sort(fileQueueIdList);
    	            for (File fileQueueId : fileQueueIdList) {
    	                long queueId;
    	                try {
    	                    queueId = Integer.parseInt(fileQueueId.getName());
    	                } catch (NumberFormatException e) {
    	                		e.printStackTrace();
    	                    continue;
    	                }
    	                this.time=queueId*this.defaultMessageStore.getMessageStoreConfig().getDelayLogInterval()*1000;
    	                timeWheel=new TimeWheel(this.defaultMessageStore.getMessageStoreConfig(), 
    	                		queueId*this.defaultMessageStore.getMessageStoreConfig()
    	                		.getDelayLogInterval()*1000,0);
    	                while(true) {
    	                		boolean re=this.loadNextTimeWheel(queueId, timeWheel);
    	                		if(!re) {
    	                			break;
    	                		}
    	                }
    	                nowQueueId=queueId;
    	                nowQueueId2=-1;
    	                timeWheel2=null;
    	                nextPosition=0;
    	            }
    	        }else {
    	        		long queueId;
                
                queueId = nowTime/1000/this.defaultMessageStore.getMessageStoreConfig().getDelayLogInterval();

                timeWheel=new TimeWheel(this.defaultMessageStore.getMessageStoreConfig(), 
                		queueId*this.defaultMessageStore.getMessageStoreConfig().getDelayLogInterval()*1000,0);
                while(true) {
	            		boolean re=this.loadNextTimeWheel(queueId, timeWheel);
	            		if(!re) {
	            			break;
	            		}
	            }
                
                nowQueueId=queueId;
                nowQueueId2=-1;
                timeWheel2=null;
                nextPosition=0;
    	        }
    	            
    		}else {
    			long queueId;
            queueId = time/1000/this.defaultMessageStore.getMessageStoreConfig().getDelayLogInterval();

            timeWheel=new TimeWheel(this.defaultMessageStore.getMessageStoreConfig(), 
            		queueId*this.defaultMessageStore.getMessageStoreConfig().getDelayLogInterval()*1000,0);
            while(true) {
            		boolean re=this.loadNextTimeWheel(queueId, timeWheel);
            		if(!re) {
            			break;
            		}
            }
            nowQueueId=queueId;
            nowQueueId2=-1;
            timeWheel2=null;
            nextPosition=0;
    		}
        this.executor.execute(this);
        
        
    }

    public void shutdown() {
    		this.persist();
    		this.shutdown=true;
    		this.executor.shutdown();
    		try {
				this.executor.awaitTermination(90, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    }



    public boolean load(final boolean lastExitOK) {
        
        this.delayMessageStore.load();
        this.consumeQueue.load();
        this.consumeQueue.recover();
        File file = new File(this.configFilePath());

        MappedFile.ensureDirOK(file.getParent());

       try {
        FileChannel fileChannel = new RandomAccessFile(file, "rw").getChannel();
        this.mappedByteBuffer = fileChannel.map(MapMode.READ_WRITE, 0, 1024*4);
        this.startIndex=(int)this.mappedByteBuffer.getLong(0);
       }catch(Exception e) {
    	   	e.printStackTrace();
    	   	return false;
       }
        
        return true;
    }

    
    private String configFilePath() {
        return StorePathConfigHelper.getDelayOffsetStorePath(this.defaultMessageStore.getMessageStoreConfig()
            .getStorePathRootDir());
    }

   



    static class DeliverDelayedMessageTimerTask{
       


        public static MessageExtBrokerInner messageTimeup(MessageExt msgExt) {
            MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
            msgInner.setBody(msgExt.getBody());
            msgInner.setFlag(msgExt.getFlag());
            MessageAccessor.setProperties(msgInner, msgExt.getProperties());

            TopicFilterType topicFilterType = MessageExt.parseTopicFilterType(msgInner.getSysFlag());
            long tagsCodeValue =
                MessageExtBrokerInner.tagsString2tagsCode(topicFilterType, msgInner.getTags());
            msgInner.setTagsCode(tagsCodeValue);
            msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgExt.getProperties()));

            msgInner.setSysFlag(msgExt.getSysFlag());
            msgInner.setBornTimestamp(msgExt.getBornTimestamp());
            msgInner.setBornHost(msgExt.getBornHost());
            msgInner.setStoreHost(msgExt.getStoreHost());
            msgInner.setReconsumeTimes(msgExt.getReconsumeTimes());

            msgInner.setWaitStoreMsgOK(false);

            msgInner.setTopic(msgInner.getProperty(MessageConst.PROPERTY_REAL_TOPIC));

            String queueIdStr = msgInner.getProperty(MessageConst.PROPERTY_REAL_QUEUE_ID);
            int queueId = Integer.parseInt(queueIdStr);
            msgInner.setQueueId(queueId);
            

            return msgInner;
        }
        

    }



	public long computeDeliverTimestamp(int delayLevel, long storeTimestamp) {
		return delayLevel*1000+storeTimestamp;
	}

}
