
# Copyright IBM Corp. 2016 All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import os
import re
import subprocess
import devops_pb2
import fabric_pb2
import chaincode_pb2

import orderer_util
from grpc.framework.interfaces.face.face import NetworkError
from grpc.beta.interfaces import StatusCode

from grpc.beta import implementations

@given(u'user "{enrollId}" is an authorized user of the ordering service')
def step_impl(context, enrollId):
	secretMsg = {
		"enrollId": enrollId,
		"enrollSecret" : enrollId
	}
	orderer_util.registerUser(context, secretMsg, "N/A")


@when(u'user "{enrollId}" broadcasts "{numMsgsToBroadcast}" unique messages on "{composeService}"')
def step_impl(context, enrollId, numMsgsToBroadcast, composeService):
	userRegistration = orderer_util.getUserRegistration(context, enrollId)
	userRegistration.broadcastMessages(context, numMsgsToBroadcast, composeService)


@when(u'user "{enrollId}" connects to deliver function on "{composeService}"')
def step_impl(context, enrollId, composeService):
	# First get the properties
	assert 'table' in context, "table (Start | End) not found in context"
	userRegistration = orderer_util.getUserRegistration(context, enrollId)
	streamHelper = userRegistration.connectToDeliverFunction(context, composeService)


@then(u'user "{enrollId}" should get a delivery from "{composeService}" of "{expectedBlocks}" blocks with "{numMsgsToBroadcast}" messages within "{batchTimeout}" seconds')
def step_impl(context, enrollId, expectedBlocks, numMsgsToBroadcast, batchTimeout, composeService):
	userRegistration = orderer_util.getUserRegistration(context, enrollId)
	streamHelper = userRegistration.getDelivererStreamHelper(context, composeService)
	blocks = streamHelper.getBlocks()
	# Verify block count
	assert len(blocks) == int(expectedBlocks), "Expected {0} blocks, received {1}".format(expectedBlocks, len(blocks))


def convertSeek(utfString):
	try:
		return int(utfString)
	except ValueError:
		return str(utfString)

@when(u'user "{enrollId}" sends deliver a seek request on "{composeService}" with properties')
def step_impl(context, enrollId, composeService):
	row = context.table.rows[0]
	start, end, = convertSeek(row['Start']), convertSeek(row['End'])

	userRegistration = orderer_util.getUserRegistration(context, enrollId)
	streamHelper = userRegistration.getDelivererStreamHelper(context, composeService)
        streamHelper.seekToRange(start = start, end = end)
