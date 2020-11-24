# coding: utf8
from network import Network
from statuses import CalculatorStatus
from time import sleep

import json
import logging
import random
import time
import threading


# Потребовалось для того что бы была возможность остановки
class CalculatorStarter(object):
    def __init__(self, dispatcher_address, port, **kwargs):
        self.port = port
        self.calc_kwargs = kwargs
        self.calculator_object = Calculator(dispatcher_address,  self.port, **self.calc_kwargs)
        self.dispatcher_address = dispatcher_address
        self.calculator_thread = threading.Thread(target=self.calculator_object.start)
        #TODO: можно заполнять лучше, подумать
        self.disability_chance = kwargs['disability_chance']
        self.disability_timer_interval = kwargs['disability_timer_interval']
        self.disability_check_time = kwargs['disability_check_time']
        self.task_time_interval = kwargs['task_time_interval']
        self.status_update_time = kwargs['status_update_time']

    def serve_forever(self):
        # TODO: здесь подумать, можно лучше
        self.calculator_thread.start()
        time.sleep(self.disability_check_time)
        while True:
            # реализация не работоспособности калюкулятора
            #TODO: подумать над тем как не создавать новый калькулятор после каждого отключения event() может подойти
            if self.calculator_object is None:
                self.__start_calculator()
                time.sleep(self.disability_check_time)
            else:
                if self.disability_chance >= random.randint(0, 100):
                    disability_timer = random.randrange(
                        self.disability_timer_interval[0],
                        self.disability_timer_interval[1]
                    )
                    self.__stop_calculator()
                    logger.info('Калькулятор останавливается на {} сек'.format(disability_timer))
                    time.sleep(disability_timer)
                else:
                    time.sleep(self.disability_check_time)

    def __start_calculator(self):
        self.calculator_object = Calculator(
            dispatcher_address,
            self.port,
            **self.calc_kwargs
        )
        self.calculator_thread = threading.Thread(target=self.calculator_object.start)
        self.calculator_thread.start()

    def __stop_calculator(self):
        self.calculator_object.stop()
        self.calculator_thread.join(None)
        self.calculator_object = None
        self.calculator_thread = None


class Calculator(object):
    def __init__(self, dispatcher_adress, port, **kwargs):
        self.client = Network(
            '',
            port,
            self.__calculator_request_handler
        )
        self.alive = False
        self.task = None
        self.status = CalculatorStatus.ready
        self.dispatcher_adress = dispatcher_adress
        self.task_time_interval = kwargs['task_time_interval']
        self.disability_event = threading.Event()
        self.send_status_thread = threading.Thread(
            target=self.__send_status_thread,
            kwargs={'delay': kwargs['status_update_time'], 'disability_event':  self.disability_event}
        )
        self.perform_task_thread = threading.Thread(
            target=self.__perform_task,
            kwargs={'disability_event': self.disability_event}
        )

    def start(self):
        self.alive = True
        self.send_status_thread.start()
        self.perform_task_thread.start()
        self.client.serve_forever()

    def stop(self):
        self.alive = False
        self.disability_event.set()
        self.client.shutdown()

    def __calculator_request_handler(self, message, addr):
        if message['method'] == 'perform_task':
            self.__perform_task_handler(message)
        elif message['method'] == 'get_status':
            self.__send_status()

    def __perform_task_handler(self, message):
        self.status = CalculatorStatus.busy
        self.task = message['parameters']['task_id']

    def __perform_task(self, disability_event):
        while not disability_event.isSet():
            if self.task is not None:
                task_time = random.randrange(self.task_time_interval[0], self.task_time_interval[1])
                sleep(task_time)

                # Возможно, калькулятор выключился пока мы выполняли задачу
                if disability_event.isSet():
                    self.task = None
                    continue

                logger.info('Задача выполнена калькулятором за {} сек.'.format(task_time))

                new_command = {}
                new_command['method'] = 'task_calculated'
                new_command['parameters'] = {'task_id': self.task}

                try:
                    self.client.send_command(adress=self.dispatcher_adress, data=new_command)
                    logger.info('Отправили ответ об успешном выполнении диспетчеру')
                except:
                    logger.info('Ошибка отправки ответа об успешном выолнении диспетчеру')

                self.status = CalculatorStatus.ready
                # TODO: Подумать, возможно лучше текущую задачу делать пустой строкой
                self.task = None

    def __send_status(self):
        try:
            new_command = {}
            new_command['method'] = 'calculator_status'
            new_command['parameters'] = {'status': self.status}

            self.client.send_command(adress=self.dispatcher_adress, data=new_command)
            logger.info('Отправили текущий статус диспетчеру')
        except:
            logger.error('Ошибка отправки статуса диспетчеру')

    def __send_status_thread(self, disability_event, delay=None):
        while not disability_event.isSet():
            if delay is not None:
                time.sleep(delay)
            self.__send_status()


if __name__ == '__main__':

    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s | %(name)s | %(message)s')
    logger = logging.getLogger()
    logger.name = 'CALCULATOR'

    with open('config/calculator.json') as f:
        config = json.load(f)

    dispatcher_address = (config['dispatcher'][0], config['dispatcher'][1])
    CalculatorStarter = CalculatorStarter(dispatcher_address, 3009, **config)
    CalculatorStarter.serve_forever()
