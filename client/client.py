import asyncio
from datetime import timedelta
from openleadr import OpenADRClient, enable_default_logging
from openleadr.utils import certificate_fingerprint
from functools import partial
import os
import logging
from http import HTTPStatus

import aiohttp
from lxml.etree import XMLSyntaxError
from signxml.exceptions import InvalidSignature
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from openleadr import utils
from openleadr import enums, objects, errors
from openleadr.messaging import create_message, parse_message, \
                                validate_xml_schema, validate_xml_signature
enable_default_logging()

CA_CERT = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'certificates', 'dummy_ca.crt')
VTN_CERT = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'certificates', 'dummy_vtn.crt')
VTN_KEY = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'certificates', 'dummy_vtn.key')
VEN_CERT = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'certificates', 'dummy_ven.crt')
VEN_KEY = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'certificates', 'dummy_ven.key')

logger = logging.getLogger('openleadr')
logger.setLevel(logging.INFO)

with open(VEN_CERT) as file:
    ven_fingerprint = certificate_fingerprint(file.read())

with open(VTN_CERT) as file:
    vtn_fingerprint = certificate_fingerprint(file.read())
    print("VTN fingerprint", vtn_fingerprint)

class FDRClient:
    def __init__(self):
        print("Initiazling FDR client")
        self.loop = asyncio.get_event_loop()
        self.loop.set_debug(True)
        self.ven_id = "ven123"
        self.received_events = []               # Holds the events that we received.
        self.responded_events = {}              # Holds the events that we already saw.
        self.client = OpenADRClient(ven_name=self.ven_id,
                       vtn_url="http://127.0.0.1:8081/OpenADR2/Simple/2.0b",
                        )
        self.client.add_handler('on_event', self.on_handle_event)
        
   
    async def on_event(self, event):
        return 'optIn'

    async def on_update_event(self, event):
        """
        Placeholder for the on_update_event handler.
        """
        logger.warning("An Event was updated, but you don't have an on_updated_event handler configured. "
                        "You should implement your own on_update_event handler. This handler receives "
                        "an Event dict and should return either 'optIn' or 'optOut' based on your "
                        "choice. Will re-use the previous opt status for this event_id for now")
        if event['event_descriptor']['event_id'] in self.responded_events:
            return self.responded_events.get(event['event_descriptor']['event_id'])    

    async def on_handle_event(self, message):
        logger.debug("The VEN received an event")
        #self.client._on_event(self, message)
        events = message['events']

        try:
            results = []
            for event in message['events']:
                event_id = event['event_descriptor']['event_id']
                event_status = event['event_descriptor']['event_status']
                modification_number = event['event_descriptor']['modification_number']
                received_event = utils.find_by(self.received_events, 'event_descriptor.event_id', event_id)
                if received_event:
                    if received_event['event_descriptor']['modification_number'] == modification_number:
                        # Re-submit the same opt type as we already had previously
                        result = self.responded_events[event_id]
                    else:
                        # Replace the event with the fresh copy
                        utils.pop_by(self.received_events, 'event_descriptor.event_id', event_id)
                        self.received_events.append(event)
                        # Wait for the result of the on_update_event handler
                        result = await utils.await_if_required(self.on_update_event(event))
                else:
                    # Wait for the result of the on_event
                    self.received_events.append(event)
                    result = self.on_event(event)
                if asyncio.iscoroutine(result):
                    result = await result
                results.append(result)
                if event_status in (enums.EVENT_STATUS.COMPLETED, enums.EVENT_STATUS.CANCELLED) \
                        and event_id in self.responded_events:
                    self.responded_events.pop(event_id)
                else:
                    self.responded_events[event_id] = result
            for i, result in enumerate(results):
                if result not in ('optIn', 'optOut') and events[i]['response_required'] == 'always':
                    logger.error("Your on_event or on_update_event handler must return 'optIn' or 'optOut'; "
                                 f"you supplied {result}. Please fix your on_event handler.")
                    results[i] = 'optOut'
        except Exception as err:
            logger.error("Your on_event handler encountered an error. Will Opt Out of the event. "
                         f"The error was {err.__class__.__name__}: {str(err)}")
            results = ['optOut'] * len(events)

        print("Results is", results)

        event_responses = [{'response_code': 200,
                            'response_description': 'OK',
                            'opt_type': results[i],
                            'request_id': message['request_id'],
                            'modification_number': events[i]['event_descriptor']['modification_number'],
                            'event_id': events[i]['event_descriptor']['event_id']}
                           for i, event in enumerate(events)
                           if event['response_required'] == 'always'
                           and not utils.determine_event_status(event['active_period']) == 'completed']

        if len(event_responses) > 0:
            logger.info(f"Total event_responses: {len(event_responses)}")
            response = {'response_code': 200,
                        'response_description': 'OK',
                        'request_id': message['request_id']}
            message = self.client._create_message('oadrCreatedEvent',
                                           response=response,
                                           event_responses=event_responses,
                                           ven_id=self.ven_id)
            service = 'EiEvent'
           # print("Going to perform request with message", message)
            response_type, response_payload = await self.client._perform_request(service, message)
            logger.info(response_type, response_payload)
        else:
            logger.info("Not sending any event responses, because a response was not required/allowed by the VTN.")

    
    async def handlePoll(self):
     response_type, response_payload = await self.client.poll()
     if response_type is None:
            return
     elif response_type == 'oadrDistributeEvent':
            if 'events' in response_payload and len(response_payload['events']) > 0:
                await self.on_handle_event(response_payload)
     else:
            logger.warning(f"No handler implemented for incoming message "
                           f"of type {response_type}, ignoring.")

    async def collect_data(future=None):
        print("Collect Data")
        value = 100 * random()
        if future:
            future.set_result(value)
        return value

    async def sendReports(self):
        self.client.add_report(callback=self.collect_data,
                        report_specifier_id='CurrentReport',
                        resource_id='Device001',
                        measurement='current',
                        unit='A')
        self.client.add_report(callback=self.collect_data,
                        report_specifier_id='CurrentReport',
                        resource_id='Device002',
                        measurement='current',
                        unit='A')
        self.client.add_report(callback=self.collect_data,
                        report_specifier_id='VoltageReport',
                        resource_id='Device001',
                        measurement='voltage',
                        unit='V')
        self.client.add_report(callback=self.collect_data,
                        report_specifier_id='VoltageReport',
                        resource_id='Device002',
                        measurement='voltage',
                        unit='V')
        self.client.ven_id = "ven123"
        response = await self.client.register_reports(self.client.reports)
        logger.info(f"response is {response}")

    # Immediately poll again, because there might be more messages
    #await self.handlePoll() 
    async def periodic_polling(self):
      
        # Your asynchronous operations here
        while True:
            await self.handlePoll()
            await asyncio.sleep(60)
            await self.sendReports()
            await asyncio.sleep(60)
            await self.client.cancel_party_registration()
            print("Async method executed")
            break
            # await asyncio.sleep(200)
    async def registration(self):
        response_type, response_payload = await self.client.create_party_registration(ven_id= "ven123")
        print('responsetype', response_type)
        print('response_payload', response_payload)
        await asyncio.sleep(60)
        await self.periodic_polling()
           
    def run(self):
        print("Initiazling FDR client Run")
        #self.loop.create_task(self.registration())
        self.loop.run_until_complete(self.registration())
        #self.registration_loop.run_until_complete(self.registration())

if __name__ == "__main__":
    obj = FDRClient()
    obj.run()