// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.  The ASF
// licenses this file to You under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
// License for the specific language governing permissions and limitations
// under the License.

package rmqremote

// RequestCode integers, confirmed against
// rocketmq-4.9.8 common/.../protocol/RequestCode.java (see CODES.md).
const (
	RequestPullMessage             = 11
	RequestGetAllTopicListFromNameSrv = 206
	RequestGetBrokerClusterInfo    = 106
	RequestGetTopicStatsInfo        = 202
	RequestGetRouteInfoByTopic     = 105
	RequestQueryTopicConsumeByWho = 300
	RequestGetConsumerListByGroup = 38     // returns {consumerIdList:[...]} (used by queryConsumeList, NOT by examineConsumerConnectionInfo)
	RequestGetConsumerConnectionList = 203  // examineConsumerConnectionInfo — returns full ConsumerConnection (connectionSet, messageModel)
	RequestGetConsumeStats        = 208
	RequestViewBrokerStatsData   = 315
	RequestGetBrokerRuntimeInfo   = 28
	RequestGetAllProducerInfo     = 328
	RequestGetConsumerRunningInfo = 307
)

// Pull response codes map to PullStatus (MQClientAPIImpl.processPullResponse).
const (
	ResponsePullNotFound        = 19  // -> NO_NEW_MSG
	ResponsePullRetryImmediately = 20 // -> NO_MATCHED_MSG
	ResponsePullOffsetMoved      = 21 // -> OFFSET_ILLEGAL
)

// ResponseCode integers, confirmed against rocketmq-4.9.8
// remoting/.../protocol/RemotingSysResponseCode.java (SUCCESS, SYSTEM_ERROR)
// and common/.../protocol/ResponseCode.java (the rest).
const (
	ResponseSuccess           = 0
	ResponseSystemError        = 1
	ResponseNoPermission       = 16
	ResponseTopicNotExist      = 17
	ResponseConsumerNotOnline  = 206
)
