"""
Module 5 - Smart Smoker Emitter

Modifications Summary:
----------------------
1. Updated the CSV file reference to point to 'smoker-temps.csv' instead of 'tasks.csv'.
2. Modified the main execution block to:
   a. Read temperature readings from the CSV file, specifically extracting smoker temperature, Food A temperature, and Food B temperature.
   b. Loop through these readings and send them to their respective RabbitMQ queues, namely "01-smoker", "02-food-A", and "03-food-B".
   c. Introduced a consistent delay of 30 seconds between reading and sending each set of temperatures to the queues, as specified in the assignment.
3. Removed redundant code sections and imports to streamline the file.

Author: Kim Leach
Date: 09/23/2023
Edited: 09/29/2023
"""

# Import necessary libraries
import pika
import sys
import webbrowser
import csv
import time

# Define the CSV file as a variable
csv_file = 'smoker-temps.csv'

def offer_rabbitmq_admin_site():
    """Offer to open the RabbitMQ Admin website"""
    ans = input("Would you like to monitor RabbitMQ queues? y or n ")
    print()
    if ans.lower() == "y":
        webbrowser.open_new("http://localhost:15672/#/queues")
        print()

def send_message(host: str, queue_name: str, message: str):
    """
    Creates and sends a message to the queue each execution.
    This process runs and finishes.

    Parameters:
        host (str): the host name or IP address of the RabbitMQ server
        queue_name (str): the name of the queue
        message (str): the message to be sent to the queue
    """
    # Check if the message is empty, if so return without sending
    if not message.strip():
        return

    try:
        # create a blocking connection to the RabbitMQ server
        conn = pika.BlockingConnection(pika.ConnectionParameters(host))
        # use the connection to create a communication channel
        ch = conn.channel()
        # use the channel to declare a durable queue
        ch.queue_declare(queue=queue_name, durable=True)
        # use the channel to publish a message to the queue
        ch.basic_publish(exchange="", routing_key=queue_name, body=message)
        # print a message to the console for the user
        print(f" [x] Sent {message} to {queue_name} queue")
    except pika.exceptions.AMQPConnectionError as e:
        print(f"Error: Connection to RabbitMQ server failed: {e}")
        sys.exit(1)
    finally:
        # close the connection to the server
        conn.close()

if __name__ == "__main__":
    # ask the user if they'd like to open the RabbitMQ Admin site
    offer_rabbitmq_admin_site()

    # Read temperature readings from the CSV file
    readings = []
    with open(csv_file, mode='r') as file:
        reader = csv.reader(file)
        next(reader)  # skip the header
        for row in reader:
            readings.append((row[1], row[2], row[3]))  # Extract smoker, Food A, and Food B temperatures

    # Loop through the readings and send them to the respective queues with a delay of 30 seconds
    for smoker_temp, food_a_temp, food_b_temp in readings:
        send_message("localhost", "01-smoker", smoker_temp)
        send_message("localhost", "02-food-A", food_a_temp)
        send_message("localhost", "03-food-B", food_b_temp)
        
        # Delay for 30 seconds between readings as specified in the assignment
        time.sleep(30)
        
    print("All temperature readings from the CSV have been sent to the RabbitMQ queues!")
