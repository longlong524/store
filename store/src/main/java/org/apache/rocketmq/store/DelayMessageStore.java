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
package org.apache.rocketmq.store;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.config.StorePathConfigHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DelayMessageStore{
    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private final MessageStoreConfig messageStoreConfig;
    
    private final DefaultMessageStore defaultMessageStore;

    private final Map<Long/* time 30 mins */, DelayMessageLog> logTable=new HashMap<Long,DelayMessageLog>();;

    

    public DelayMessageStore(final DefaultMessageStore defaultMessageStore)  {
        this.defaultMessageStore=defaultMessageStore;
        this.messageStoreConfig = defaultMessageStore.getMessageStoreConfig();

    }

  

    /**
     * @throws IOException
     */
    public boolean load() {
        boolean result = true;

        try {
            
        		boolean lastExitOK = !this.isTempFileExist();
           
            // load Delay Log
            result = result && this.loadDelayLog();

            if (result) {
                this.recover(lastExitOK);

            }
        } catch (Exception e) {
            log.error("load exception", e);
            result = false;
        }
        return result;
    }

    public void remove(long queueId) {
    		DelayMessageLog dml=this.logTable.remove(queueId);
    		if(dml!=null) {
    			dml.destroy();
    		}
    }
    /**
     * @throws Exception
     */
    public void start() throws Exception {
    }

    public void shutdown() {
       this.flush();
    }

    public void destroy() {
        this.destroyLogics();
    }
    
    public MessageExt lookMessageByOffset(long queueId,long commitLogOffset) {
    		DelayMessageLog dml=this.logTable.get(queueId);
    		if(dml==null) {
    			return null;
    		}
        SelectMappedBufferResult sbr = dml.getMessage(commitLogOffset, 4);
        if (null != sbr) {
            try {
                // 1 TOTALSIZE
                int size = sbr.getByteBuffer().getInt();
                if(size==0) {
                		return null;
                }
                return lookMessageByOffset(dml,commitLogOffset, size);
            } finally {
                sbr.release();
            }
        }

        return null;
    }
    
    
    public MessageExt lookMessageByOffset(DelayMessageLog dml,long commitLogOffset, int size) {
        SelectMappedBufferResult sbr = dml.getMessage(commitLogOffset, size);
        if (null != sbr) {
            try {
                return MessageDecoder.decode(sbr.getByteBuffer(), true, false);
            } finally {
                sbr.release();
            }
        }

        return null;
    }

    public void destroyLogics() {
        for (DelayMessageLog dsl : this.logTable.values()) {
            dsl.destroy();
        }
    }

    public PutMessageResult putMessage(MessageExtBrokerInner msg) {
        
    		PutMessageResult result=null;
        final int tranType = MessageSysFlag.getTransactionValue(msg.getSysFlag());
        if (tranType == MessageSysFlag.TRANSACTION_NOT_TYPE
            || tranType == MessageSysFlag.TRANSACTION_COMMIT_TYPE) {
            // Delay Delivery
            if (msg.getDelayTimeLevel() > 0) {
            		long queueId=((msg.getDelayTimeLevel()*1000+msg.getBornTimestamp())/1000)/this.messageStoreConfig.getDelayLogInterval();
            		DelayMessageLog dml=this.logTable.get(queueId);
            		if(dml!=null) {
            			result=dml.putMessage(msg);
            		}else {
            			dml=new DelayMessageLog(defaultMessageStore, queueId);
            			result=dml.putMessage(msg);
            			this.logTable.put(queueId, dml);
            		}
            }
        }
        return result;
    }

    private boolean loadDelayLog() {
        File dirLogic = new File(this.messageStoreConfig.getStorePathDelayLog());
        
        File[] fileQueueIdList = dirLogic.listFiles();
        if (fileQueueIdList != null) {
            for (File fileQueueId : fileQueueIdList) {
                long queueId;
                try {
                    queueId = Integer.parseInt(fileQueueId.getName());
                } catch (NumberFormatException e) {
                    continue;
                }
                DelayMessageLog logic = new DelayMessageLog(
                    this.defaultMessageStore,
                    queueId);
                this.logTable.put(queueId, logic);
                if (!logic.load()) {
                    return false;
                }
            }
        }
            

        log.info("load delaylog queue all over, OK");

        return true;
    }

    private void recover(final boolean lastExitOK) {
        for(DelayMessageLog dsl:this.logTable.values()) {
	        	if (lastExitOK) {
	        		dsl.recoverNormally();
            } else {
            		dsl.recoverAbnormally();
            }
        		
        }
    }
    
    private boolean isTempFileExist() {
        String fileName = StorePathConfigHelper.getAbortFile(this.messageStoreConfig.getStorePathRootDir());
        File file = new File(fileName);
        return file.exists();
    }

    public MessageStoreConfig getMessageStoreConfig() {
        return messageStoreConfig;
    }



	public void flush() {
		for(DelayMessageLog dml:this.logTable.values()) {
			dml.flush();
		}
	}

 
}
