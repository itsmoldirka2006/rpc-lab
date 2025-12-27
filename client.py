import socket
import json
import time
import uuid
import logging
from typing import Any, Optional, Dict
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class RPCClient:
    def __init__(self, server_host: str, server_port: int = 5000):
        self.server_host = server_host
        self.server_port = server_port
        self.timeout = 2
        self.max_retries = 3
        self.retry_delay = 1
        self.client_id = str(uuid.uuid4())[:8]
        
        logging.info(f"RPC Client {self.client_id} initialized")
        logging.info(f"Server: {self.server_host}:{self.server_port}")
        logging.info(f"Timeout: {self.timeout}s, Max retries: {self.max_retries}")
    
    def call(
        self,
        method: str,
        params: Any,
        request_id: Optional[str] = None,
        simulate_failure: bool = False,
        force_timeout: bool = False
    ) -> Dict:
        if request_id is None:
            request_id = str(uuid.uuid4())
        
        request = {
            'request_id': request_id,
            'method': method,
            'params': params,
            'timestamp': datetime.now().isoformat(),
            'client_id': self.client_id
        }
        
        port = self.server_port
        if simulate_failure:
            port = 9999
            logging.warning(f"[{request_id}] Using wrong port {port}")
        
        for attempt in range(self.max_retries):
            attempt_num = attempt + 1
            logging.info(f"[{request_id}] Attempt {attempt_num}/{self.max_retries}")
            start_time = time.time()
            
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(self.timeout)
                sock.connect((self.server_host, port))
                sock.send(json.dumps(request).encode('utf-8'))
                
                response_data = sock.recv(4096)
                latency = time.time() - start_time
                
                if not response_data:
                    raise ConnectionError("Empty response")
                
                response = json.loads(response_data.decode('utf-8'))
                response['latency'] = f"{latency:.3f}s"
                response['attempt'] = attempt_num
                response['client_timestamp'] = request['timestamp']
                sock.close()
                
                if response.get('status') == 'OK':
                    logging.info(f"[{request_id}] SUCCESS")
                    return response
                else:
                    return response
                    
            except socket.timeout:
                logging.warning(f"[{request_id}] TIMEOUT on attempt {attempt_num}")
                if attempt_num < self.max_retries:
                    time.sleep(self.retry_delay)
                else:
                    return {
                        'request_id': request_id,
                        'error': f'Max retries exceeded after {self.max_retries}',
                        'status': 'TIMEOUT_ERROR',
                        'attempts': attempt_num,
                        'client_timestamp': request['timestamp']
                    }
                    
            except ConnectionRefusedError:
                logging.error(f"[{request_id}] CONNECTION REFUSED")
                if attempt_num < self.max_retries:
                    time.sleep(self.retry_delay)
                else:
                    return {
                        'request_id': request_id,
                        'error': 'Connection refused',
                        'status': 'CONNECTION_ERROR',
                        'attempts': attempt_num,
                        'client_timestamp': request['timestamp']
                    }
                    
            except Exception as e:
                logging.error(f"[{request_id}] ERROR: {e}")
                if attempt_num < self.max_retries:
                    time.sleep(self.retry_delay)
                else:
                    return {
                        'request_id': request_id,
                        'error': str(e),
                        'status': 'UNKNOWN_ERROR',
                        'attempts': attempt_num,
                        'client_timestamp': request['timestamp']
                    }
        
        return {
            'request_id': request_id,
            'error': 'Fatal error',
            'status': 'FATAL_ERROR',
            'client_timestamp': request['timestamp']
        }

def demonstrate_all_scenarios(client: RPCClient, server_ip: str):
    print("\n" + "=" * 70)
    print("RPC LAB - COMPLETE DEMONSTRATION")
    print("=" * 70)
    
    print(client.call('add', {'a': 5, 'b': 7}))
    print(client.call('multiply', {'a': 3, 'b': 4}))
    print(client.call('reverse_string', {'s': 'hello world'}))
    print(client.call('get_time', {}))
    print(client.call('non_existent_method', {'x': 1}))
    print(client.call('simulate_delay', {'delay_seconds': 5}))
    print(client.call('add', {'a': 2, 'b': 3}, simulate_failure=True))
    print(client.call('simulate_delay', {'delay_seconds': 1}))

def interactive_mode(client: RPCClient):
    while True:
        try:
            cmd = input("RPC> ").strip().lower()
            if cmd == 'quit':
                break
            elif cmd.startswith('add '):
                a, b = map(float, cmd.split()[1:])
                print(client.call('add', {'a': a, 'b': b}))
            elif cmd.startswith('mul '):
                a, b = map(float, cmd.split()[1:])
                print(client.call('multiply', {'a': a, 'b': b}))
            elif cmd.startswith('reverse '):
                print(client.call('reverse_string', {'s': cmd[8:]}))
            elif cmd == 'time':
                print(client.call('get_time', {}))
            elif cmd.startswith('delay '):
                print(client.call('simulate_delay', {'delay_seconds': float(cmd.split()[1])}))
            elif cmd == 'fail':
                print(client.call('add', {'a': 1, 'b': 2}, simulate_failure=True))
            else:
                print("Unknown command")
        except KeyboardInterrupt:
            break

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        server_ip = sys.argv[1]
    else:
        server_ip = input("Enter Server IP: ").strip()
    
    client = RPCClient(server_ip)
    
    try:
        test_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        test_sock.settimeout(2)
        test_sock.connect((server_ip, 5000))
        test_sock.close()
    except:
        print("Cannot connect to server")
        sys.exit(1)
    
    demonstrate_all_scenarios(client, server_ip)
    
    if input("Interactive mode? (y/n): ").lower() == 'y':
        interactive_mode(client)
