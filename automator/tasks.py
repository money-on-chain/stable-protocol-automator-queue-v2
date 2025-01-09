import decimal
from web3 import Web3
import datetime

from .contracts import Multicall2, MocQueue

from .base.main import ConnectionHelperBase
from .tasks_manager import PendingTransactionsTasksManager, on_pending_transactions
from .logger import log
from .utils import aws_put_metric_heart_beat


__VERSION__ = '1.0.3'


log.info("Starting Stable Protocol Queue Automator version {0}".format(__VERSION__))


class Automator(PendingTransactionsTasksManager):

    def __init__(self,
                 config,
                 connection_helper,
                 contracts_loaded
                 ):
        self.config = config
        self.connection_helper = connection_helper
        self.contracts_loaded = contracts_loaded

        # init PendingTransactionsTasksManager
        super().__init__(self.config,
                         self.connection_helper,
                         self.contracts_loaded)

    @on_pending_transactions
    def execute(self, task=None, global_manager=None, task_result=None):

        # If ready to execute the queue?
        ready_to_execute = self.contracts_loaded["MocQueue"].sc.functions.readyToExecute().call()
        if ready_to_execute:

            # return if there are pending transactions
            if task_result.get('pending_transactions', None):
                return task_result

            web3 = self.connection_helper.connection_manager.web3

            nonce = web3.eth.get_transaction_count(
                self.connection_helper.connection_manager.accounts[0].address, "pending")

            # get gas price from node
            node_gas_price = decimal.Decimal(Web3.from_wei(web3.eth.gas_price, 'ether'))

            try:
                tx_hash = self.contracts_loaded["MocQueue"].execute(
                    self.config['tasks']['execute']['fee_recipient'],
                    gas_limit=self.config['tasks']['execute']['gas_limit'],
                    max_fee_per_gas=self.config['max_fee_per_gas'],
                    max_priority_fee_per_gas=self.config['max_priority_fee_per_gas'],
                    nonce=nonce
                )
            except ValueError as err:
                log.error("Task :: {0} :: Error sending transaction! \n {1}".format(task.task_name, err))
                return task_result

            if tx_hash:
                new_tx = dict()
                new_tx['hash'] = tx_hash
                new_tx['timestamp'] = datetime.datetime.now()
                new_tx['gas_price'] = node_gas_price
                new_tx['nonce'] = nonce
                new_tx['timeout'] = self.config['tasks']['execute']['wait_timeout']
                task_result['pending_transactions'].append(new_tx)

                log.info("Task :: {0} :: Sending TX :: Hash: [{1}] Nonce: [{2}] Gas Price: [{3}]".format(
                    task.task_name, Web3.to_hex(new_tx['hash']), new_tx['nonce'], int(node_gas_price * 10 ** 18)))

        else:
            log.info("Task :: {0} :: No!".format(task.task_name))

        return task_result


class AutomatorTasks(Automator):

    def __init__(self, config):

        self.config = config
        self.connection_helper = ConnectionHelperBase(config)

        self.contracts_loaded = dict()
        self.contracts_addresses = dict()

        # contract addresses
        self.load_contracts()

        # init automator
        super().__init__(self.config,
                         self.connection_helper,
                         self.contracts_loaded)

        # Add tasks
        self.schedule_tasks()

    def load_contracts(self):
        """ Get contract address to use later """

        log.info("Getting addresses from Main Contract...")

        # Moc
        self.contracts_loaded["MocQueue"] = MocQueue(
            self.connection_helper.connection_manager,
            contract_address=self.config['addresses']['MocQueue'])
        self.contracts_addresses['MocQueue'] = self.contracts_loaded["MocQueue"].address().lower()

        # Multicall
        self.contracts_loaded["Multicall2"] = Multicall2(
            self.connection_helper.connection_manager,
            contract_address=self.config['addresses']['Multicall2'])

    def schedule_tasks(self):

        log.info("Starting adding tasks...")

        # set max workers
        self.max_workers = 1

        if 'execute' in self.config['tasks']:
            log.info("Jobs add: 1. Execute Queue")
            interval = self.config['tasks']['execute']['interval']
            self.add_task(self.execute,
                          args=[],
                          wait=interval,
                          timeout=180,
                          task_name='1. Execute Queue')

        # Set max workers
        self.max_tasks = len(self.tasks)
