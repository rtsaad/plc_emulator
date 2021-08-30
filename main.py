
from asyncio.windows_events import ProactorEventLoop
import time
import argparse
from typing import Dict
from time import sleep
from signal import signal, SIGINT
from multiprocessing import Barrier, Event, Queue, Manager

# Local imports
from plc_emulator.opc_server import OpcUAServer
from plc_emulator.csv_vars import OpcCsv
from plc_emulator.opc_gateway import OpcGateway
from plc_emulator.plc import Plc
from plc_emulator.logger import Logger 

current_milli_time = lambda: int(round(time.time() * 1000))

def _set_endpoint(endpoint, port):
    return 'opc.tcp://{}:{}/opc-tunnel/server'.format(endpoint, port)

# Constants
# Ignore command line arguments to set the values directly here
IGNORE_COMMAND_ARGUMENTS = True
# Default values
PERIODIC_TIME = 0.125
PLC_SCAN_CYCLE = 0.050
VERBOSE = 3
SLIDING_WINDOW = 3
ENDPOINT = 'localhost'
PORT     = '4840'
NAMESPACE = 'opcua-tunnel'
SERVERNAME = 'http://test-plc.com/tunnel'

 
def main(): 

    # Set arguments
    if IGNORE_COMMAND_ARGUMENTS:
        peridic_time_value  = PERIODIC_TIME
        scan_cycle_value    = PLC_SCAN_CYCLE
        verbose_value       = VERBOSE
        endpoint            = ENDPOINT
        port                = PORT
        namespace           = NAMESPACE
        server_name         = SERVERNAME

    else:
        parser = argparse.ArgumentParser()
        parser.add_argument('-pt','--periodic_time', help='Set the periodic constraint to be respected by all processors.', default=PERIODIC_TIME)
        parser.add_argument('-sc','--scan_cycle', help='PLC scan cycle constraint.', default=PLC_SCAN_CYCLE)
        parser.add_argument('-v', '--verbose', help='Print debug logs')
        parser.add_argument('-ep', '--endpoint', help='Server end point', default=ENDPOINT)
        parser.add_argument('-p', '--port', help='Server port', default=PORT)
        parser.add_argument('-ns', '--namespace', help='Node namespace to registers', default=NAMESPACE)
        parser.add_argument('-sn', '--servername', help='Server name', default=SERVERNAME)
        args = parser.parse_args()
        verbose_value = VERBOSE
        if(args.verbose):
            verbose_value = 1
        peridic_time_value  = args.periodic_time
        scan_cycle_value    = args.scan_cycle
        endpoint            = args.endpoint
        port                = args.port
        namespace           = args.namespace
        server_name         = args.servername


    # Start Process
    end_event: Event   = None
    barrier:   Barrier = None
    endpoint_address = _set_endpoint(endpoint, port)
    try:
        with Manager() as manager: 

            # Communication channels
            log_channel : Queue = Queue()
            opc_conn    : Dict  = manager.dict()
            end_event   : Event = Event()
            barrier     : Barrier = Barrier(2)

            # Start logger
            logger = Logger(log_channel, verbose_value, end_event)
            logger.start()

            # Opcua Server
            opcua = OpcUAServer(endpoint_address, server_name, namespace, opc_conn, barrier, end_event)
            opcua.start()

            # Process to emulate the sis (pcl)
            sis = Plc(scan_cycle_value, opc_conn, log_channel, barrier, end_event)
            sis.start()                 

            # Never stops, must be halted by the user.
            logger.join()        
            sis.join()
            opcua.join()
    except KeyboardInterrupt:
        if end_event:
            end_event.set()
        if barrier:
            barrier.reset()
        print('Closing....')


if __name__ == "__main__":
    main()
