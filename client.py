# coding: utf8
from network import Network
from statuses import ClientTaskStatus

import json
import threading
import time
import logging
import random
import uuid


class TaskInfo(object):
    def __init__(self):
        self.time_created = time.time()
        self.time_done = None
        self.status = ClientTaskStatus.new

# потребовалось для того что бы можно было выключить клиента
class ClientStarter(object):
    def __init__(self, Network, port, dispatcher_address, **kwargs):
        self.client_object = Client(Network, port, dispatcher_address, **kwargs)
        self.clinet_thread = threading.Thread(target=self.client_object.start)
        self.client_alive_time_limit = kwargs.get('client_time_limit', 0)

    def serve_forever(self):
        self.clinet_thread.start()
        if self.client_alive_time_limit > 0:
            time.sleep(self.client_alive_time_limit)
            self.client_object.stop()
            self.clinet_thread.join(None)


class Client(object):
    def __init__(self, Network, port, dispatcher_address, **kwargs):
        self.client = Network(
            '',
            port,
            self.__client_request_handler
        )
        self.dispatcher_address = dispatcher_address
        self.alive = False
        self.tasks_dict = {}
        self.create_task_interval = kwargs['create_task_interval']
        self.task_generate_thread = threading.Thread(target=self.__generate_task)

    def start(self):
        self.alive = True
        self.task_generate_thread.start()
        try:
            self.client.serve_forever()
        except KeyboardInterrupt:
            logger.info('Обработка остановки клиента')

    def stop(self):
        self.alive = False
        self.client.shutdown()
        self.task_generate_thread.join(None)
        self.__print_stats()

    def __print_stats(self):
        total_solved = 0
        total_unsolved = 0
        tasks_time = []
        for task in self.tasks_dict.values():
            if task.status == ClientTaskStatus.new:
                total_unsolved += 1
            else:
                total_solved += 1
                tasks_time.append(task.time_done - task.time_created)

        total_tasks = total_solved + total_unsolved
        logger.info('Запросов отправлено {}'.format(total_tasks))

        if total_solved > 0:
            avg_time = sum(tasks_time) / total_solved
            logger.info('Запросов решено {}'.format(total_solved))
            logger.info('Запросов не решено {}'.format(total_unsolved))
            logger.info('Минимальное время {}'.format(min(tasks_time)))
            logger.info('Среднее время {}'.format(avg_time))
            logger.info('Максимальное время {}'.format(max(tasks_time)))


    def __generate_task(self):
        while self.alive:
            task_time = random.randrange(self.create_task_interval[0], self.create_task_interval[1])
            time.sleep(task_time)

            try:
                new_task = TaskInfo()
                task_id = str(uuid.uuid4())

                new_command = {}
                new_command['method'] = 'add_task'
                new_command['parameters'] = {'task_id': task_id}

                self.client.send_command(adress=self.dispatcher_address, data=new_command)
                self.tasks_dict[task_id] = new_task
            except:
                logger.info('Ошибка при генерации задания')


    def __client_request_handler(self, message, addr):
        if message['method'] == 'task_done':
            task_id = message['parameters']['task_id']
            completed_task = self.tasks_dict.get(task_id)

            if completed_task is None:
                logger.error('Передали задачу клиенту, которую он не отправлял. ID задачи {}'.format(task_id))
            else:
                completed_task.time_done = time.time()
                completed_task.status = ClientTaskStatus.done
                logger.info('Получено подтверждение выполнения задачи')


if __name__ == '__main__':
    
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s | %(name)s | %(message)s')
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    logger.name = 'CLIENT'

    with open('config/client.json') as f:
        config = json.load(f)

    dispatcher_address = (config['dispatcher'][0], config['dispatcher'][1])

    Starter = ClientStarter(Network, 3003, dispatcher_address, **config)
    Starter.serve_forever()



