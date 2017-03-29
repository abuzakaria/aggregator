#!/usr/bin/env python
import contextlib
import json

import pika
import time

from pika.exceptions import AMQPError

from aggregator import aggregator_config
from aggregator import amqp_config
from aggregator import constants


class Aggregator_service(object):
    responses = []

    def send_status_request(self, exchange_name=constants.UNITS_EXCHANGE_NAME):
        # establishing a connection with RabbitMQ server
        # running on the local machine, localhost
        with pika_connection() as connection:
            channel = connection.channel()

            # # create a queue where the message will be delivered, named first_queue
            # channel.queue_declare(queue='hello')
            channel.exchange_declare(exchange=exchange_name,
                                     type="topic",
                                     durable=True,
                                     auto_delete=False)

            # message to be sent
            message = {
                "message": "status",
                "response_queue": constants.UNITS_RESPONSE_Q_NAME
            }

            for unit_id in aggregator_config.UNIT_IDS:
                # sending the message
                channel.basic_publish(exchange=exchange_name,
                                      routing_key=unit_id,
                                      body=json.dumps(message))
            print(message)

    def publish_result(self, exchange_name=constants.RESULT_EXCHANGE_NAME):
        # establishing a connection with RabbitMQ server
        # running on the local machine, localhost
        with pika_connection() as connection:
            channel = connection.channel()

            # # create a queue where the message will be delivered, named first_queue
            # channel.queue_declare(queue='hello')
            channel.exchange_declare(exchange=exchange_name,
                                     type="topic",
                                     durable=True,
                                     auto_delete=False)

            # message to be sent
            message = {
                "message": self.calculate_result(),
            }

            for unit_id in aggregator_config.UNIT_IDS:
                # sending the message
                channel.basic_publish(exchange=exchange_name,
                                      routing_key=unit_id,
                                      body=json.dumps(message))
            print(message)

    # callback function to the queue
    def callback(self, ch, method, properties, message_received):
        response = json.loads(message_received)
        self.responses.append(response)

    def _get_number_of_active_units(self):
        # todo
        return 0

    def _get_average_SoC(self):
        # todo
        return 0

    def _get_remaining_capacity(self):
        # todo
        return 0

    def _get_number_of_units(self):
        # todo
        return 0

    def _get_units_out_of_boundaries(self):
        # todo
        return 0

    def calculate_result(self):
        return {
            "AverageSoC": self._get_average_SoC(),
            "RemainingCapacity": self._get_remaining_capacity(),
            "NumberOfActiveUnits": self._get_number_of_active_units(),
            "NumberOfUnits": self._get_number_of_units(),
            "UnitsOutOfBoundaries": self._get_units_out_of_boundaries(),

        }

    def receive(self, queue=constants.UNITS_RESPONSE_Q_NAME):
        # establishing a connection with RabbitMQ server
        # running on the local machine, localhost
        with pika_connection() as connection:
            channel = connection.channel()

            # declaring the queue where we will consume the message, named first_queue

            channel.queue_declare(queue=queue)

            # tell to RabbitMQ that this callback function should receive messages from first_queue
            channel.basic_consume(self.callback, queue=queue)

            print('Python Queue - Waiting for messages.')

            # running a loop that is listening for data and runs the callback functions whenever necessary
            channel.start_consuming()


@contextlib.contextmanager
def pika_connection():
    """
    Provide a transactional scope around a series of operations.
    :return:
    """
    # establishing a connection with RabbitMQ server
    # running on the local machine, localhost
    params = pika.ConnectionParameters(host=amqp_config.HOST,
                                       credentials=pika.PlainCredentials(amqp_config.USER, amqp_config.PASSWORD))

    connection = pika.BlockingConnection()
    try:
        yield connection
    except AMQPError as ex:
        connection.close()
        raise ex
    connection.close()


if __name__ == "__main__":
    send()
    time.sleep(1)
    receive()
