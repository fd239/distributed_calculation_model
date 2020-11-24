# coding: utf8
from network import Network
from statuses import CalculatorStatus, DispatcherTaskStatus

import json
import logging
import time
import threading
from collections import OrderedDict


class DispatcherTask(object):
    def __init__(self, addr, task_status):
        self.client_adress = addr
        self.calculator_adress = tuple
        self.status = task_status
        self.calculation_start_time = time.time()


class CalculatorInfo(object):
    def __init__(self, addr, status):
        self.adress = addr
        self.status = status


class Dispatcher(object):
    def __init__(self, network_class, server, port, **kwargs):
        self.client = network_class(
            server,
            port,
            self.__dispatcher_request_handler
        )
        self.addr = (server, port)
        self.lock = threading.Lock()
        self.alive = False
        self.tasks_dict = OrderedDict()
        self.calculators = {}
        self.task_max_calculate_time = kwargs['task_max_calculate_time']

    def start(self):
        self.alive = True
        try:
            threading.Thread(target=self.__handle_tasks_queue).start()
            self.client.serve_forever()
        except KeyboardInterrupt:
            logger.info("Обработка остановки диспетчера")

    def stop(self):
        self.alive = False
        self.client.shutdown()

    def __dispatcher_request_handler(self, message, addr):
        if message['method'] == 'add_task':
            self.__add_task(message, addr)
        elif message['method'] == 'task_calculated':
            self.__complete_task(message)
        elif message['method'] == 'calculator_status':
            self.set_calculator_status(message, addr)

    def __handle_tasks_queue(self):
            # TODO: Здесь подумать над реализацией, можно проще
            while self.alive:
                with self.lock:
                    #TODO: какой-то баг с task_key когда >5 клиентов, возможно поменять task_key
                    for task_key, task in self.tasks_dict.iteritems():
                        if task.status == DispatcherTaskStatus.new or \
                                task.status == DispatcherTaskStatus.calculation_timeout:
                            for calc_addr, calculator in self.calculators.iteritems():
                                if calculator.status == CalculatorStatus.ready:
                                    try:
                                        new_command = {}
                                        new_command['method'] = "perform_task"
                                        new_command['parameters'] = {"task_id": task_key}

                                        self.client.send_command(adress=calculator.adress, data=new_command)

                                        task.status = DispatcherTaskStatus.sent_to_calculator
                                        task.calculation_start_time = time.time()
                                        task.calculator_adress = calc_addr

                                        calculator.status = CalculatorStatus.busy

                                        logger.info("Отправили задачу калькулятору")

                                        break
                                    except Exception as e:
                                        logger.error("Не удалось отправить задачу калькулятору {}".format(e))

                        elif task.status == DispatcherTaskStatus.calculated:
                            try:
                                new_command = {}
                                new_command['method'] = "task_done"
                                new_command['parameters'] = {"task_id": task_key}

                                self.client.send_command(adress=task.client_adress, data=new_command)

                                task.status = DispatcherTaskStatus.sent_to_client

                                logger.info("Отправили завершенную задачу клиенту")
                            except:
                                logger.error("Не удалось отправить завершенную задачу клиенту")

                        elif task.status == DispatcherTaskStatus.sent_to_calculator:
                            if (time.time() - task.calculation_start_time) > self.task_max_calculate_time:
                                task.calculator_id = ''
                                task.calculator_adress = tuple
                                task.status == DispatcherTaskStatus.calculation_timeout

    def __add_task(self, message, addr):
        task_id = message['parameters']['task_id']

        new_task = DispatcherTask(addr, DispatcherTaskStatus.new)

        with self.lock:
            self.tasks_dict[task_id] = new_task

        logger.info('Диспетчер принял задачу')

    def __complete_task(self, message):
        task_id = message['parameters']['task_id']
        task = self.tasks_dict.get(task_id)

        if task is None:
            logger.error("Передана задача, которой не было в диспетчере. Ключ задачи".format(task_id))
        else:
            task.status = DispatcherTaskStatus.calculated
            task_calculator = self.calculators.get(task.calculator_adress)
            if task_calculator is None:
                logger.error("Ошибка установки свойства калькулятора")
            else:
                task_calculator.status = CalculatorStatus.ready
                logger.info("Обновили статус калькулятора на ready")

    def set_calculator_status(self, message, addr):
        calculator_address = addr
        new_status = message['parameters']['status']
        calculator = self.calculators.get(calculator_address)
        if calculator is None:
            new_calculator = CalculatorInfo(addr, new_status)
            self.calculators[calculator_address] = new_calculator
            logger.info("Добавлен новый вычислитель {}".format(calculator_address))
        else:
            calculator.status = new_status
            logger.info("Обновлен статус вычислителя {}. Новый статус {}".format(calculator_address, new_status))


if __name__ == '__main__':

    logging.basicConfig(level=logging.DEBUG, format="%(asctime)s | %(name)s | %(message)s")
    logger = logging.getLogger()
    logger.name = 'DISPATCHER'

    with open('config/dispatcher.json') as f:
        config = json.load(f)

    server = config['addr'][0]
    port = config['addr'][1]

    Dispatcher = Dispatcher(Network, server, port, **config)
    Dispatcher.start()

