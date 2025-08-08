#!/usr/bin/env python3
"""
Message Server Simulator for Flipped Load Balancer
Simulates multiple server instances generating messages for different customers
"""

import json
import socket
import threading
import time
import random
import sys
import uuid
from datetime import datetime
import argparse

class MessageServer:
    def __init__(self, server_id, port, customers, message_types):
        self.server_id = server_id
        self.port = port
        self.customers = customers
        self.message_types = message_types
        self.running = False
        self.server_socket = None
        self.client_connections = []
        self.message_counter = 0
        
    def start(self):
        """Start the server and begin accepting connections"""
        self.running = True
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        
        try:
            self.server_socket.bind(('localhost', self.port))
            self.server_socket.listen(5)
            print(f"[{self.server_id}] Server started on port {self.port}")
            
            # Start message generation thread
            message_thread = threading.Thread(target=self._generate_messages, daemon=True)
            message_thread.start()
            
            # Accept client connections
            while self.running:
                try:
                    client_socket, address = self.server_socket.accept()
                    print(f"[{self.server_id}] Client connected from {address}")
                    self.client_connections.append(client_socket)
                    
                    # Handle client in separate thread
                    client_thread = threading.Thread(
                        target=self._handle_client, 
                        args=(client_socket,), 
                        daemon=True
                    )
                    client_thread.start()
                    
                except socket.error as e:
                    if self.running:
                        print(f"[{self.server_id}] Accept error: {e}")
                        
        except Exception as e:
            print(f"[{self.server_id}] Server error: {e}")
        finally:
            self.stop()
    
    def _handle_client(self, client_socket):
        """Handle individual client connections"""
        try:
            while self.running:
                time.sleep(0.1)  # Keep connection alive
        except Exception as e:
            print(f"[{self.server_id}] Client handler error: {e}")
        finally:
            try:
                client_socket.close()
            except:
                pass
    
    def _generate_messages(self):
        """Generate messages at regular intervals"""
        print(f"[{self.server_id}] Message generation started")
        
        while self.running:
            try:
                # Generate 1-3 messages per cycle
                num_messages = random.randint(1, 3)
                
                for _ in range(num_messages):
                    message = self._create_message()
                    self._send_message(message)
                    time.sleep(random.uniform(0.1, 0.5))  # Slight delay between messages
                
                # Wait before next batch
                time.sleep(random.uniform(1.0, 3.0))
                
            except Exception as e:
                print(f"[{self.server_id}] Message generation error: {e}")
                time.sleep(1)
    
    def _create_message(self):
        """Create a message following the standard format"""
        self.message_counter += 1
        
        # Randomly select destination and message type
        destination = random.choice(self.customers)
        message_type = random.choice(self.message_types)
        
        # Set priority based on message type and some randomness
        priority_map = {"breakfast": 1, "lunch": 2, "dinner": 2}
        base_priority = priority_map.get(message_type, 2)
        
        # Add some randomness and VIP customers (table1 gets higher priority)
        if destination == "table1":
            priority = max(1, base_priority - 1)  # VIP gets higher priority
        else:
            priority = min(3, base_priority + random.randint(0, 1))
        
        message = {
            "messageId": f"{self.server_id}-{self.message_counter}-{uuid.uuid4().hex[:8]}",
            "sourceServerId": self.server_id,
            "destinationId": destination,
            "messageType": message_type,
            "priority": priority,
            "timestamp": int(time.time() * 1000),
            "payload": {
                "serverInfo": self.server_id,
                "customerTable": destination,
                "orderDetails": self._generate_order_details(message_type),
                "specialRequests": self._generate_special_requests() if random.random() > 0.7 else None
            }
        }
        
        return message
    
    def _generate_order_details(self, message_type):
        """Generate realistic order details based on meal type"""
        breakfast_items = ["pancakes", "eggs benedict", "oatmeal", "french toast", "breakfast burrito"]
        lunch_items = ["caesar salad", "club sandwich", "soup and sandwich", "burger", "pasta"]
        dinner_items = ["grilled salmon", "steak", "chicken parmesan", "vegetarian pasta", "lamb chops"]
        
        items_map = {
            "breakfast": breakfast_items,
            "lunch": lunch_items,
            "dinner": dinner_items
        }
        
        items = items_map.get(message_type, lunch_items)
        return {
            "item": random.choice(items),
            "quantity": random.randint(1, 2),
            "prepTime": random.randint(10, 30),
            "cost": round(random.uniform(12.99, 45.99), 2)
        }
    
    def _generate_special_requests(self):
        """Generate special customer requests"""
        requests = [
            "no onions", "extra sauce", "gluten free", "dairy free", 
            "extra spicy", "mild seasoning", "extra vegetables", "well done"
        ]
        return random.choice(requests)
    
    def _send_message(self, message):
        """Send message to all connected clients"""
        message_json = json.dumps(message) + '\n'
        
        # Remove disconnected clients
        active_connections = []
        
        for client_socket in self.client_connections:
            try:
                client_socket.send(message_json.encode('utf-8'))
                active_connections.append(client_socket)
                
            except socket.error:
                try:
                    client_socket.close()
                except:
                    pass
        
        self.client_connections = active_connections
        
        if active_connections:
            print(f"[{self.server_id}] Sent: {message['messageId']} -> {message['destinationId']} "
                  f"({message['messageType']}, priority {message['priority']})")
    
    def stop(self):
        """Stop the server"""
        print(f"[{self.server_id}] Stopping server...")
        self.running = False
        
        # Close all client connections
        for client_socket in self.client_connections:
            try:
                client_socket.close()
            except:
                pass
        
        # Close server socket
        if self.server_socket:
            try:
                self.server_socket.close()
            except:
                pass
        
        print(f"[{self.server_id}] Server stopped")

def run_server(server_id, port):
    """Run a single message server instance"""
    customers = ["table1", "table2", "table3", "table4"]
    message_types = ["breakfast", "lunch", "dinner"]
    
    server = MessageServer(server_id, port, customers, message_types)
    
    try:
        server.start()
    except KeyboardInterrupt:
        print(f"\n[{server_id}] Received interrupt signal")
    except Exception as e:
        print(f"[{server_id}] Unexpected error: {e}")
    finally:
        server.stop()

def run_all_servers():
    """Run all server instances in parallel"""
    servers_config = [
        ("server1", 8001),
        ("server2", 8002),
        ("server3", 8003),
        ("server4", 8004)
    ]
    
    threads = []
    
    print("Starting all server simulators...")
    print("=" * 50)
    
    for server_id, port in servers_config:
        thread = threading.Thread(target=run_server, args=(server_id, port))
        thread.daemon = True
        thread.start()
        threads.append(thread)
        time.sleep(0.5)  # Stagger startup
    
    print("\nAll servers started! Press Ctrl+C to stop.")
    print("=" * 50)
    
    try:
        # Keep main thread alive
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("\nShutting down all servers...")
    
    # Wait for threads to finish (they're daemon threads, so this is quick)
    for thread in threads:
        thread.join(timeout=1)
    
    print("All servers stopped.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Message Server Simulator")
    parser.add_argument("--server", type=str, help="Run single server (server1, server2, server3, server4)")
    parser.add_argument("--port", type=int, help="Port for single server")
    
    args = parser.parse_args()
    
    if args.server and args.port:
        run_server(args.server, args.port)
    else:
        run_all_servers()