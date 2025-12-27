#!/usr/bin/env python3
"""
RPC Client Implementation with timeout and retry logic
Demonstrates at-least-once semantics
"""
import socket
import json
import time
import uuid
import logging
from typing import Any, Optional, Dict
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class RPCClient:
    def __init__(self, server_host: str, server_port: int = 5000):
        """
        Initialize RPC Client
        
        Args:
            server_host: Server IP address
            server_port: Server port (default: 5000)
        """
        self.server_host = server_host
        self.server_port = server_port
        self.timeout = 2  # seconds (for single attempt)
        self.max_retries = 3  # maximum retry attempts
        self.retry_delay = 1  # seconds between retries
        self.client_id = str(uuid.uuid4())[:8]  # Short client ID for logging
        
        logging.info(f"RPC Client {self.client_id} initialized")
        logging.info(f"Server: {self.server_host}:{self.server_port}")
        logging.info(f"Timeout: {self.timeout}s, Max retries: {self.max_retries}")
    
    def call(self, method: str, params: Any, request_id: Optional[str] = None, 
             simulate_failure: bool = False, force_timeout: bool = False) -> Dict:
        """
        Make an RPC call with automatic retry logic
        
        Args:
            method: Name of the remote method
            params: Parameters for the method (dict or list)
            request_id: Optional request ID (auto-generated if None)
            simulate_failure: If True, use wrong port to simulate network failure
            force_timeout: If True, server will simulate delay > timeout
            
        Returns:
            Response dictionary with results or error
        """
        # Generate request ID if not provided
        if request_id is None:
            request_id = str(uuid.uuid4())
        
        # Prepare request with all required fields
        request = {
            'request_id': request_id,
            'method': method,
            'params': params,
            'timestamp': datetime.now().isoformat(),
            'client_id': self.client_id
        }
        
        # Modify for failure simulation
        port = self.server_port
        if simulate_failure:
            port = 9999  # Non-existent port
            logging.warning(f"[{request_id}] SIMULATION: Using wrong port {port} to cause connection failure")
        
        # If forcing timeout, use simulate_delay method with long delay
        if force_timeout and method != 'simulate_delay':
            logging.warning(f"[{request_id}] SIMULATION: Will force timeout scenario")
        
        # Attempt RPC call with retries
        for attempt in range(self.max_retries):
            attempt_num = attempt + 1
            logging.info(f"[{request_id}] Attempt {attempt_num}/{self.max_retries}")
            
            start_time = time.time()
            
            try:
                # Create socket
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(self.timeout)
                
                # Connect to server (or wrong port for simulation)
                sock.connect((self.server_host, port))
                
                # Send request
                request_json = json.dumps(request)
                sock.send(request_json.encode('utf-8'))
                logging.debug(f"[{request_id}] Sent: {request_json}")
                
                # Receive response
                response_data = sock.recv(4096)
                end_time = time.time()
                latency = end_time - start_time
                
                if not response_data:
                    raise ConnectionError("Empty response from server")
                
                # Parse response
                response = json.loads(response_data.decode('utf-8'))
                response['latency'] = f"{latency:.3f}s"
                response['attempt'] = attempt_num
                response['client_timestamp'] = request['timestamp']
                
                sock.close()
                
                # Check response status
                if response.get('status') == 'OK':
                    logging.info(f"[{request_id}] SUCCESS on attempt {attempt_num}")
                    logging.info(f"[{request_id}] Latency: {latency:.3f}s, Result: {response.get('result')}")
                    return response
                else:
                    # Server returned error (not network error)
                    logging.error(f"[{request_id}] Server error: {response.get('error')}")
                    return response
                    
            except socket.timeout:
                latency = time.time() - start_time
                logging.warning(f"[{request_id}] TIMEOUT after {latency:.2f}s on attempt {attempt_num}")
                
                if attempt_num < self.max_retries:
                    logging.info(f"[{request_id}] Waiting {self.retry_delay}s before retry...")
                    time.sleep(self.retry_delay)
                else:
                    logging.error(f"[{request_id}] MAX RETRIES EXCEEDED ({self.max_retries} attempts)")
                    return {
                        'request_id': request_id,
                        'error': f'Max retries exceeded after {self.max_retries} attempts',
                        'status': 'TIMEOUT_ERROR',
                        'attempts': attempt_num,
                        'client_timestamp': request['timestamp']
                    }
                    
            except ConnectionRefusedError:
                latency = time.time() - start_time
                logging.error(f"[{request_id}] CONNECTION REFUSED after {latency:.2f}s on attempt {attempt_num}")
                
                if attempt_num < self.max_retries:
                    logging.info(f"[{request_id}] Retrying in {self.retry_delay}s...")
                    time.sleep(self.retry_delay)
                else:
                    logging.error(f"[{request_id}] MAX RETRIES EXCEEDED")
                    return {
                        'request_id': request_id,
                        'error': 'Server not responding (connection refused)',
                        'status': 'CONNECTION_ERROR',
                        'attempts': attempt_num,
                        'client_timestamp': request['timestamp']
                    }
                    
            except Exception as e:
                latency = time.time() - start_time
                logging.error(f"[{request_id}] ERROR on attempt {attempt_num}: {str(e)}")
                
                if attempt_num < self.max_retries:
                    time.sleep(self.retry_delay)
                else:
                    return {
                        'request_id': request_id,
                        'error': f'Unexpected error: {str(e)}',
                        'status': 'UNKNOWN_ERROR',
                        'attempts': attempt_num,
                        'client_timestamp': request['timestamp']
                    }
        
        # Should not reach here
        return {
            'request_id': request_id,
            'error': 'Unknown error in RPC call',
            'status': 'FATAL_ERROR',
            'client_timestamp': request['timestamp']
        }

def demonstrate_all_scenarios(client: RPCClient, server_ip: str):
    """Demonstrate all required RPC scenarios"""
    print("\n" + "="*70)
    print("RPC LAB - COMPLETE DEMONSTRATION")
    print("="*70)
    
    # SCENARIO 1: Normal successful RPC calls
    print("\n1. NORMAL OPERATION - Successful RPC Calls")
    print("-"*40)
    
    print("a) add(5, 7):")
    response = client.call('add', {'a': 5, 'b': 7})
    print(f"   Result: {response.get('result')}, Status: {response.get('status')}")
    
    print("\nb) multiply(3, 4):")
    response = client.call('multiply', {'a': 3, 'b': 4})
    print(f"   Result: {response.get('result')}, Status: {response.get('status')}")
    
    print("\nc) reverse_string('hello world'):")
    response = client.call('reverse_string', {'s': 'hello world'})
    print(f"   Result: {response.get('result')}, Status: {response.get('status')}")
    
    print("\nd) get_time():")
    response = client.call('get_time', {})
    print(f"   Result: {response.get('result')}, Status: {response.get('status')}")
    
    # SCENARIO 2: Method not found error
    print("\n2. ERROR HANDLING - Method Not Found")
    print("-"*40)
    response = client.call('non_existent_method', {'x': 1})
    print(f"   Error: {response.get('error')}, Status: {response.get('status')}")
    
    # SCENARIO 3: Server delay (timeout triggers retry)
    print("\n3. FAILURE DEMONSTRATION - Server Delay (5 seconds)")
    print("-"*40)
    print("Server will sleep for 5 seconds, client timeout is 2 seconds")
    print("Expected: Client will timeout and retry 3 times")
    response = client.call('simulate_delay', {'delay_seconds': 5})
    print(f"   Final Status: {response.get('status')}")
    print(f"   Attempts: {response.get('attempt', 1)}")
    if response.get('status') == 'TIMEOUT_ERROR':
        print("   ✓ Demonstrated: At-least-once semantics with retries")
    
    # SCENARIO 4: Network failure simulation
    print("\n4. FAILURE DEMONSTRATION - Network Failure")
    print("-"*40)
    print("Client will try to connect to wrong port (9999)")
    response = client.call('add', {'a': 2, 'b': 3}, simulate_failure=True)
    print(f"   Status: {response.get('status')}")
    print(f"   Error: {response.get('error')}")
    print(f"   Attempts: {response.get('attempts', 1)}")
    
    # SCENARIO 5: Quick delay (within timeout)
    print("\n5. BOUNDARY TEST - Short Delay (1 second)")
    print("-"*40)
    print("Server delay is 1 second, client timeout is 2 seconds")
    print("Expected: Success on first attempt")
    response = client.call('simulate_delay', {'delay_seconds': 1})
    print(f"   Result: {response.get('result')}")
    print(f"   Latency: {response.get('latency')}")
    print(f"   Attempt: {response.get('attempt')}")

def interactive_mode(client: RPCClient):
    """Interactive mode for testing"""
    print("\n" + "="*70)
    print("INTERACTIVE RPC TESTING")
    print("="*70)
    print("Commands:")
    print("  add a b        - Add two numbers")
    print("  mul a b        - Multiply two numbers")
    print("  reverse text   - Reverse a string")
    print("  time           - Get server time")
    print("  delay seconds  - Simulate server delay")
    print("  fail           - Simulate network failure")
    print("  quit           - Exit")
    print("="*70)
    
    while True:
        try:
            cmd = input("\nRPC> ").strip().lower()
            
            if cmd == 'quit':
                break
            elif cmd.startswith('add '):
                parts = cmd.split()
                if len(parts) == 3:
                    a, b = float(parts[1]), float(parts[2])
                    response = client.call('add', {'a': a, 'b': b})
                    print(f"Response: {json.dumps(response, indent=2)}")
            elif cmd.startswith('mul '):
                parts = cmd.split()
                if len(parts) == 3:
                    a, b = float(parts[1]), float(parts[2])
                    response = client.call('multiply', {'a': a, 'b': b})
                    print(f"Response: {json.dumps(response, indent=2)}")
            elif cmd.startswith('reverse '):
                text = cmd[8:]
                response = client.call('reverse_string', {'s': text})
                print(f"Response: {json.dumps(response, indent=2)}")
            elif cmd == 'time':
                response = client.call('get_time', {})
                print(f"Response: {json.dumps(response, indent=2)}")
            elif cmd.startswith('delay '):
                parts = cmd.split()
                if len(parts) == 2:
                    delay = float(parts[1])
                    response = client.call('simulate_delay', {'delay_seconds': delay})
                    print(f"Response: {json.dumps(response, indent=2)}")
            elif cmd == 'fail':
                response = client.call('add', {'a': 1, 'b': 2}, simulate_failure=True)
                print(f"Response: {json.dumps(response, indent=2)}")
            else:
                print("Unknown command. Type 'quit' to exit.")
                
        except KeyboardInterrupt:
            print("\nExiting...")
            break
        except Exception as e:
            print(f"Error: {e}")

if __name__ == "__main__":
    import sys
    
    # Get server IP
    if len(sys.argv) > 1:
        server_ip = sys.argv[1]
    else:
        print("Enter Server IP address (from EC2 console):")
        server_ip = input("> ").strip()
    
    # Create client
    client = RPCClient(server_ip)
    
    # Test connection first
    print(f"\nTesting connection to server {server_ip}:5000...")
    try:
        test_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        test_sock.settimeout(2)
        test_sock.connect((server_ip, 5000))
        test_sock.close()
        print("✓ Connection successful")
    except:
        print("✗ Cannot connect to server. Make sure server.py is running on the server instance.")
        print(f"  Server IP: {server_ip}")
        print(f"  Port: 5000")
        sys.exit(1)
    
    # Run demonstrations
    demonstrate_all_scenarios(client, server_ip)
    
    # Optional: Interactive mode
    print("\n" + "="*70)
    response = input("Enter interactive mode? (y/n): ").strip().lower()
    if response == 'y':
        interactive_mode(client)
    
    print("\n" + "="*70)
    print("RPC DEMONSTRATION COMPLETE")
    print("="*70)
