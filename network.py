# coding: utf8
import socket
import json
import threading
import logging
import uuid
from collections import OrderedDict

logger = logging.getLogger(str(__name__).upper())


class NetCommand:
    def __init__(self, adress, data):
        self.adress = adress
        self.data = data


class Network(object):
    def __init__(self, server, port, request_handler):
        self.client = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.addr = (server, port)
        self.alive = True
        self.commands = OrderedDict()
        self.lock = threading.Lock()
        self.request_handler = request_handler
        self.last_key = 1

    def serve_forever(self):
        self.alive = True
        self.client.settimeout(0.03)
        self.client.bind(self.addr)
        logger.info("Приложение использует порт {}".format(self.addr[1]))

        while self.alive:

            # обработка очереди команд
            self.__handle_commands()

            # прием сообщений
            try:
                data, addr = self.client.recvfrom(2048)
                logger.info('Получили сообщение от {}, содержимое {}'.format(addr, data))
            except socket.timeout:
                continue
            except socket.error:
                continue

            message = json.loads(data, encoding="utf-8")

            if message is None:
                logger.error("Ошибка разбора сообщения от {}".format(addr))
                continue

            # обрабатываем полученное сообщение
            self.request_handler(message, addr)

    def shutdown(self):
        self.alive = False
        if self.client:
            self.client.close()

    def __send_msg_socket(self, adress, data):
        try:
            packed_data = json.dumps(data).encode("utf-8")
            self.client.sendto(packed_data, adress)
            logger.info('Отправлен пакет с данными. Содержимое пакета {}'.format(packed_data))
        except socket.error as e:
            logger.info('Ошибка отправки пакета. Содержимое пакета {}, текст ошибки {}'.format(data, e))

    def send_command(self, adress, data):
        command_id = self.last_key
        new_command = NetCommand(
            adress=adress,
            data=data
        )
        with self.lock:
            self.commands[command_id] = new_command
            self.last_key + 1

    def __handle_commands(self):
        with self.lock:
            for key, command in self.commands.iteritems():
                try:
                    self.__send_msg_socket(command.adress, command.data)
                    del self.commands[key]
                except:
                    exception_text = "Ошибка отправки команды. Ключ команды {}, содержимое пакета {}"
                    logger.error(exception_text.format(key, command.data))