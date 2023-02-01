"""
    This program sends a message to a queue on the RabbitMQ server.
    It reads from a CSV file and writes messages to a new queue.

    Name: Deanna Clayton
    Date: 1-30-23

"""

# add imports at the beginning of the file
import pika
import sys
import csv
import time

def send_message(host: str, queue_name: str, message: str):
    """
    Creates and sends a message to the queue each execution.
    This process runs and finishes.

    Parameters:
        queue_name (str): the name of the queue
        message (str): the message to be sent to the queue

    """

    try:
        # create a blocking connection to the RabbitMQ server
        conn = pika.BlockingConnection(pika.ConnectionParameters(host))
        # use the connection to create a communication channel
        ch = conn.channel()
        # use the channel to declare a queue
        ch.queue_declare(queue=queue_name)
        # use the channel to publish a message to the queue
        ch.basic_publish(exchange="", routing_key=queue_name, body=message)
        # print a message to the console for the user
        print(f" [x] Sent {message}")
    except pika.exceptions.AMQPConnectionError as e:
        print(f"Error: Connection to RabbitMQ server failed: {e}")
        sys.exit(1)
    finally:
        # close the connection to the server
        conn.close()

# read from a file to get some data
input_file = open("TeamsFranchises.csv", "r")

# use the built-in sorted() function to get them in chronological order
reversed = sorted(input_file)

# create a csv reader for our comma delimited data
reader = csv.reader(reversed, delimiter=",")

for row in reader:
    # read a row from the file
    FranchiseID, FranchiseName, Status, Association = row

    # use an fstring to create a message from our data
    message = f"[{FranchiseID}, {FranchiseName}, {Status}, {Association}]"

    # If this is the program being run, then execute the code below
    if __name__ == "__main__":
        send_message("localhost","bonus",message)
    
    # sleep for a second
    time.sleep(1)

