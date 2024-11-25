import asyncio
from asyncio import iscoroutine
from datetime import datetime, timezone, timedelta
from openleadr import OpenADRServer, enable_default_logging, objects
from functools import partial
from openleadr.utils import generate_id, group_targets_by_type
from openleadr.utils import certificate_fingerprint
from openleadr.messaging import create_message, parse_message, validate_xml_schema
import logging
import os
from dataclasses import asdict
from openleadr.objects import Event, EventDescriptor, EventSignal, Target, Interval
enable_default_logging(level=logging.INFO)

logger = logging.getLogger('openleadr')
logger.setLevel(logging.INFO)

CA_CERT = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'certificates', 'dummy_ca.crt')
VTN_CERT = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'certificates', 'dummy_vtn.crt')
VTN_KEY = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'certificates', 'dummy_vtn.key')
VEN_CERT = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'certificates', 'dummy_ven.crt')
VEN_KEY = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'certificates', 'dummy_ven.key')

with open(VEN_CERT) as file:
    ven_fingerprint = certificate_fingerprint(file.read())
    print("VEN fingerprint", ven_fingerprint)

with open(VTN_CERT) as file:
    vtn_fingerprint = certificate_fingerprint(file.read())

def create_dummy_event(ven_id):
    """
    Creates a dummy event
    """
    now = datetime.now()
    event = objects.Event(event_descriptor=objects.EventDescriptor(event_id='event001',
                                                                   modification_number=0,
                                                                   event_status='far',
                                                                   market_context='http://marketcontext01'),
                          event_signals=[objects.EventSignal(signal_id='signal001',
                                                             signal_type='level',
                                                             signal_name='simple',
                                                             intervals=[objects.Interval(dtstart=now,
                                                                                         duration=timedelta(minutes=10),
                                                                                         signal_payload=1)])
                                       ],
                          targets=[objects.Target(ven_id='ven123')])
    return event

async def lookup_fingerprint(ven_id):
    return ven_fingerprint

async def on_register_report(ven_id, resource_id, measurement, unit, scale,
                             min_sampling_interval, max_sampling_interval):
    """
    Inspect a report offering from the VEN and return a callback and sampling interval for receiving the reports.
    """
    callback = partial(on_update_report, ven_id=ven_id, resource_id=resource_id, measurement=measurement)
    sampling_interval = min_sampling_interval
    return callback, sampling_interval

async def on_update_report(data, ven_id, resource_id, measurement):
    """
    Callback that receives report data from the VEN and handles it.
    """
    for time, value in data:
        print(f"Ven {ven_id} reported {measurement} = {value} at time {time} for resource {resource_id}")

async def event_response_callback(ven_id, event_id, opt_type):
    """
    Callback that receives the response from a VEN to an Event.
    """
    print(f"VEN {ven_id} responded to Event {event_id} with: {opt_type}")

async def on_created_event(ven_id, event_id, opt_type):
    logger.info("opt_type is", opt_type)
    logger.info(f"Ven {ven_id} returned {opt_type} for event {event_id}")
    return ven_id

async def on_poll(ven_id):
    message_type = "oadrDistributeEvent"
    event = objects.Event(event_descriptor=objects.EventDescriptor(event_id='event001',
                                                                   modification_number=0,
                                                                   event_status='far',
                                                                   market_context='http://marketcontext01'),
                          event_signals=[objects.EventSignal(signal_id='signal001',
                                                             signal_type='level',
                                                             signal_name='simple',
                                                             intervals=[objects.Interval(dtstart=datetime.now(),
                                                                                         duration=timedelta(minutes=10),
                                                                                         signal_payload=1)])
                                       ],
                          targets=[objects.Target(ven_id='ven123')])

    message_payload = {"events": [asdict(event)]}   
    return message_type, message_payload

async def on_create_party_registration(payload):
    """
    Inspect the registration info and return a ven_id and registration_id.
    """
    print("registration info", payload)
    # if payload['fingerprint'] != ven_fingerprint:
    #     raise errors.FingerprintMismatch("The fingerprint of your TLS connection does not match the expected fingerprint. Your VEN is not allowed to register.")
    if payload['ven_name'] == 'ven123':
        ven_id = 'ven123'
        registration_id = 'reg_id_123'
        print("Returning values from here", registration_id)
        return ven_id, registration_id
    else:
        return False
    

async def on_cancel_party_registration(payload):
      print("Ven id:", payload)
      message_type = "oadrCanceledPartyRegistration"
      message_payload = None
      return message_type, message_payload
      

server = OpenADRServer(vtn_id='MyVTN',
                       requested_poll_freq=timedelta(seconds=60),
                       http_port = 8081,
                       verify_message_signatures=False,
                       show_server_cert_domain=False
                       #http_cert=VTN_CERT,
                       #http_key=VTN_KEY,
                      # http_ca_file=CA_CERT,
                      # cert=VTN_CERT,
                       #key=VTN_KEY,
                       #fingerprint_lookup=lookup_fingerprint
                       )

loop = asyncio.get_event_loop()
registration_future = loop.create_future()

# Add the handler for client (VEN) registrations
server.add_handler('on_create_party_registration', partial(on_create_party_registration))
# Add the handler for report registrations from the VEN
server.add_handler('on_register_report', on_register_report)
server.add_handler('on_poll', on_poll)
server.add_handler('on_created_event', on_created_event)
server.add_handler('on_cancel_party_registration', partial(on_cancel_party_registration))

# Run the server on the asyncio event loop

loop.create_task(server.run())
loop.run_forever()

