import logging
from asyncio import sleep, run
from multiprocessing.context import Process
from asyncua import Server
from typing import Any, Callable, Dict
from multiprocessing import Barrier, Event
from itertools import chain

# Local imports
from plc_emulator.opc_gateway import OpcGateway
from plc_emulator.csv_vars import OpcCsv

# Helpers
def _node_to_id(node) -> str:
    return F'ns={node.nodeid.NamespaceIndex};i={node.nodeid.Identifier}'

# Handler
class OpcUASubHandler():
    """
    Subscription Handler to receive events regarding data updates.
    """ 

    def __init__(self,  callback: Callable[[str, Any],None]) -> None:
        super().__init__()
        self._callback = callback

    def datachange_notification(self, node, val, data):
        node_id = _node_to_id(node)
        self._callback(node_id, val)
 
    def event_notification(self, event):
        pass

class OpcUAServer(Process):

    def __init__(self, 
            endpoint: str,
            name: str,
            namespace: str,
            opc_conn: Dict,
            barrier: Barrier,
            end_event: Event
        ) -> None:

        super(OpcUAServer, self).__init__()
        self._endpoint: str = endpoint
        self._name: str = name
        self._namespace: str = namespace
        self._opc_conn: Dict = opc_conn
        self._barrier: Barrier = barrier
        self._end_event: Event = end_event
                

    # Function to start server
    async def _start_server(self,
            endpoint: str,
            name: str,
            namespace: str,
            opc_conn: Dict,
            end_event: Event
        ):

        logging.info("Initial Configuration")
        server = Server()
        await server.init() 
        server.set_endpoint(endpoint)
        server.set_server_name(name)
        idx = await server.register_namespace(namespace)

        logging.info("Opc variables configuration -- started")
        # Configure opc variables
        opc_gateway = OpcGateway()
        # Recover variable list
        opc_vars = OpcCsv()
        # Configure local holders
        input_variables = opc_vars.list_variables('INPUT')
        reset_variables = opc_vars.list_variables('RESET')
        output_variables = opc_vars.list_variables('OUTPUT')
        opc_gateway.create_group(input_variables, 'input')
        opc_gateway.create_group(reset_variables, 'reset')
        opc_gateway.create_group(output_variables, 'output')
        opc_gateway_storage = {}
        opc_objects = {}
        opc_variables_to_obj = {}
        opc_variables_to_node_id = {}
        opc_obj_to_variables = {}

        # Callback function for opc updates
        def _callback_update(node_id: str, val: Any):        
            opc_gateway_storage[node_id] = val
        
        logging.info("Opc variables publishing -- started")
        # Register variables in OPCUA namespace
        first_write = []
        for inp in opc_vars.OPC_VARIABLES_LIST:
            path1 = inp[0]
            path2 = inp[1].split('.')
            var_type = inp[2]
            permission = inp[3]
            if path1 not in opc_objects:
                obj = await server.nodes.objects.add_object(idx, path1)
                opc_objects[path1] = obj
            prefix_path = path1
            if len(path2) > 1:            
                for path in path2[0:-1]:    
                    if not F'{prefix_path}.{path}' in opc_objects:
                        prefix_obj = opc_objects[prefix_path]
                        obj = await prefix_obj.add_object(idx, path)
                        prefix_path = F'{prefix_path}.{path}'
                        opc_objects[prefix_path] = obj
                    else:
                        prefix_path = F'{prefix_path}.{path}'
            # Add variable
            obj = opc_objects[prefix_path]        
            name = path2[-1]
            var_path = F'{prefix_path}.{name}'
            var = await obj.add_variable(idx, name, opc_vars.release_value(inp))
            if permission == 'INPUT' or permission == 'RESET':
                await var.set_writable(True)
            opc_variables_to_obj[var_path] = var
            opc_variables_to_node_id[var_path] = _node_to_id(var) 
            opc_obj_to_variables[_node_to_id(var)] = (inp[0], inp[1], name)
            opc_gateway_storage[_node_to_id(var)] = opc_vars.release_value(inp)
            first_write.append((inp[0] + '.' + inp[1], opc_vars.release_value(inp)))
        # Register variable to updates
        handler = OpcUASubHandler(_callback_update)        
        subscriber = await server.create_subscription(250, handler)
        await subscriber.subscribe_data_change(list(opc_variables_to_obj.values()))
        # Publish variables to plc
        opc_conn.update(opc_gateway.write_batch(first_write, opc_conn))
        first_write = None
        logging.info("Opc variables configuration -- finished")

        # Loop Cycle
        logging.info("Opc update loop cycle -- started ")
        self._barrier.wait()
        async with server:
            while not end_event.is_set():
                write = []
                table_input  = opc_gateway.read_group('input', opc_conn)       
                table_reset  = opc_gateway.read_group('reset', opc_conn)       
                table_output = opc_gateway.read_group('output', opc_conn)       
                # Input variables
                for variable in chain(table_input, table_reset): 
                    var = variable[0]
                    var_value = variable[1]
                    node_id = opc_variables_to_node_id[var]                    
                    value = opc_gateway_storage[node_id]                             
                    write.append((var, value))
                # Output variables
                for variable in table_output: 
                    var = variable[0]
                    var_value = variable[1]
                    node_id = opc_variables_to_node_id[var]                    
                    value = opc_gateway_storage[node_id]                                    
                    write.append((var, var_value))
                    obj = opc_variables_to_obj[var]
                    output = 1 if var_value else 0
                    await obj.write_value(output)                
                opc_conn.update(opc_gateway.write_batch(write, opc_conn))
                #logging.info("Opc write variables")
                await sleep(0.01)
                    
        await server.close_server()
        server = None
        return


    def run(self) -> None:       
        logging.basicConfig(level=logging.CRITICAL)  
        run(
            self._start_server(
                self._endpoint,
                self._name,
                self._namespace,
                self._opc_conn,
                self._end_event
                )
        )

        return
            
