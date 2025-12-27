#!/usr/bin/env python3
"""
RPC Server Implementation
Supports: add, multiply, get_time, reverse_string, simulate_delay
Deployed on AWS EC2
"""
import socket
import json
import time
import threading
import logging
from datetime import datetime
import uuid

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class RPCServer:
    def __init__(self, host='0.0.0.0', port=5000):
        self.host = host
        self.port = port
        self.server_socket = None
        self.running = False
        self.request_count = 0
        
        # Register available methods
        self.methods = {
            'add': self.add,
            'multiply': self.multiply,
            'get_time': self.get_time,
            'reverse_string': self.reverse_string,
            'simulate_delay': self.simulate_delay,
            'echo': self.echo
        }
    
    def add(self, a, b):
        """Add two numbers - RPC Method 1"""
        result = a + b
        logging.info(f"add({a}, {b}) = {result}")
        return result
    
    def multiply(self, a, b):
        """Multiply two numbers - RPC Method 2"""
        result = a * b
        logging.info(f"multiply({a}, {b}) = {result}")
        return result
    
    def get_time(self):
        """Return current server time - RPC Method 3"""
        result = datetime.now().isoformat()
        logging.info(f"get_time() = {result}")
        return result
    
    def reverse_string(self, s):
        """Reverse a string - RPC Method 4"""
        result = s[::-1]
        logging.info(f"reverse_string('{s}') = '{result}'")
        return result
    
    def simulate_delay(self, delay_seconds):
        """
        Simulate a slow operation - for testing timeouts
        RPC Method 5 - FAILURE DEMONSTRATION
        """
        logging.warning(f"Simulating delay of {delay_seconds} seconds...")
        time.sleep(delay_seconds)
        result = f"Slept for {delay_seconds} seconds"
        logging.info(f"Delay completed: {result}")
        return result
    
    def echo(self, message):
        """Echo back the message - RPC Method 6"""
        logging.info(f"echo('{message}')")
        return f"Echo: {message}"
    
    def handle_client(self, client_socket, address):
        """Handle a single client connection"""
        client_id = f"{address[0]}:{address[1]}"
        logging.info(f"New client connected: {client_id}")
        
        try:
            while True:
                # Receive request
                data = client_socket.recv(4096)
                if not data:
                    break
                
                self.request_count += 1
                logging.debug(f"[Req #{self.request_count}] Received {len(data)} bytes from {client_id}")
                
                try:
                    # Parse JSON request
                    request = json.loads(data.decode('utf-8'))
                    
                    # Validate required fields
                    request_id = request.get('request_id')
                    method_name = request.get('method')
                    params = request.get('params', {})
                    timestamp = request.get('timestamp')
                    
                    # Log request details
                    logging.info(f"[Req #{self.request_count}] ID: {request_id}, Method: {method_name}")
                    
                    # Validate request
                    if not request_id:
                        response = {
                            'request_id': 'unknown',
                            'error': 'request_id is required',
                            'status': 'ERROR',
                            'server_timestamp': datetime.now().isoformat()
                        }
                    elif not method_name:
                        response = {
                            'request_id': request_id,
                            'error': 'method is required',
                            'status': 'ERROR',
                            'server_timestamp': datetime.now().isoformat()
                        }
                    elif method_name not in self.methods:
                        response = {
                            'request_id': request_id,
                            'error': f'Method "{method_name}" not found. Available: {list(self.methods.keys())}',
                            'status': 'ERROR',
                            'server_timestamp': datetime.now().isoformat()
                        }
                    else:
                        try:
                            # Execute the requested method
                            method = self.methods[method_name]
                            
                            # Handle different parameter formats
                            if isinstance(params, dict):
                                result = method(**params)
                            elif isinstance(params, list):
                                result = method(*params)
                            else:
                                result = method(params)
                            
                            # Build success response
                            response = {
                                'request_id': request_id,
                                'result': result,
                                'status': 'OK',
                                'server_timestamp': datetime.now().isoformat(),
                                'request_timestamp': timestamp
                            }
                            
                            logging.info(f"[Req #{self.request_count}] Success: {method_name}")
                            
                        except TypeError as e:
                            response = {
                                'request_id': request_id,
                                'error': f'Invalid parameters: {str(e)}',
                                'status': 'ERROR',
                                'server_timestamp': datetime.now().isoformat()
                            }
                        except Exception as e:
                            response = {
                                'request_id': request_id,
                                'error': f'Server error: {str(e)}',
                                'status': 'ERROR',
                                'server_timestamp': datetime.now().isoformat()
                            }
                    
                    # Send response
                    response_json = json.dumps(response)
                    client_socket.send(response_json.encode('utf-8'))
                    logging.debug(f"[Req #{self.request_count}] Sent response: {len(response_json)} bytes")
                    
                except json.JSONDecodeError as e:
                    error_response = {
                        'request_id': 'unknown',
                        'error': f'Invalid JSON: {str(e)}',
                        'status': 'ERROR',
                        'server_timestamp': datetime.now().isoformat()
                    }
                    client_socket.send(json.dumps(error_response).encode('utf-8'))
                    logging.error(f"JSON decode error: {e}")
                    
        except ConnectionResetError:
            logging.warning(f"Client {client_id} disconnected unexpectedly")
        except Exception as e:
            logging.error(f"Error handling client {client_id}: {e}")
        finally:
            client_socket.close()
            logging.info(f"Client {client_id} disconnected")
    
    def start(self):
        """Start the RPC server"""
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(5)
        self.running = True
        
        logging.info("=" * 60)
        logging.info(f"RPC Server Started")
        logging.info(f"Host: {self.host}:{self.port}")
        logging.info(f"Available Methods: {list(self.methods.keys())}")
        logging.info("=" * 60)
        logging.info("Waiting for connections...")
        
        try:
            while self.running:
                client_socket, address = self.server_socket.accept()
                client_thread = threading.Thread(
                    target=self.handle_client,
                    args=(client_socket, address),
                    daemon=True
                )
                client_thread.start()
                
        except KeyboardInterrupt:
            logging.info("\nServer shutting down gracefully...")
        except Exception as e:
            logging.error(f"Server error: {e}")
        finally:
            self.stop()
    
    def stop(self):
        """Stop the server"""
        self.running = False
        if self.server_socket:
            self.server_socket.close()
        logging.info("Server stopped")

if __name__ == "__main__":
    # Create and start server
    server = RPCServer()
    server.start()
