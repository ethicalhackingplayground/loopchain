"""helper class for TxMessages"""

import json
import queue
import sys
from typing import List, Optional

from loopchain import configure
from loopchain.blockchain.transactions import Transaction, TransactionVersioner, TransactionSerializer
from loopchain.protos import loopchain_pb2


class TxItem:
    tx_serializers = {}

    def __init__(self, tx_json: str, channel: str):
        self.channel = channel
        self.__tx_json = tx_json
        self.__len = sys.getsizeof(tx_json) + sys.getsizeof(channel)

    def __len__(self):
        return self.__len

    def get_tx_message(self):
        message = loopchain_pb2.TxSend(
            tx_json=self.__tx_json,
            channel=self.channel)
        return message

    @classmethod
    def create_tx_item(cls, tx_param: tuple, channel: str):
        tx, tx_versioner = tx_param
        tx_serializer = cls.get_serializer(tx, tx_versioner)
        tx_item = TxItem(
            json.dumps(tx_serializer.to_raw_data(tx)),
            channel
        )
        return tx_item

    @classmethod
    def get_serializer(cls, tx: Transaction, tx_versioner: TransactionVersioner):
        if tx.version not in cls.tx_serializers:
            cls.tx_serializers[tx.version] = TransactionSerializer.new(tx.version, tx.type(), tx_versioner)
        return cls.tx_serializers[tx.version]


class TxMessages:
    def __init__(self):
        self.total_size = 0
        self._transactions: List[TxItem] = []

    def __len__(self):
        return len(self._transactions)

    def append(self, tx_item):
        self._transactions.append(tx_item)
        self.total_size += len(tx_item)

    def get_messages(self) -> List:
        tx_messages = [tx.get_tx_message() for tx in self._transactions]

        return tx_messages

    def reset(self):
        self.total_size = 0
        self._transactions.clear()


class TxMessagesQueue:
    def __init__(self, max_tx_size=None, max_tx_count=None):
        self.max_tx_size = max_tx_size or configure.MAX_TX_SIZE_IN_BLOCK
        self.max_tx_count = max_tx_count or configure.MAX_TX_COUNT_IN_ADDTX_LIST

        self._queue = queue.Queue()
        self._tx_messages = TxMessages()

    def get(self) -> Optional[TxMessages]:
        if self._queue.empty():
            tx_messages = self._tx_messages
            if len(tx_messages) == 0:
                return None
            self._tx_messages = TxMessages()
        else:
            tx_messages = self._queue.get()

        return tx_messages

    def append(self, tx_item: TxItem):
        tx_total_size = self._tx_messages.total_size + len(tx_item)
        tx_total_count = len(self._tx_messages) + 1

        if tx_total_size >= self.max_tx_size or tx_total_count >= self.max_tx_count:
            self._queue.put(self._tx_messages)
            self._tx_messages = TxMessages()

        self._tx_messages.append(tx_item)

    def remain(self) -> bool:
        return not self._queue.empty() or len(self._tx_messages) > 0

    def size(self) -> int:
        return self._queue.qsize()

    def __str__(self):
        return (f"{self.__class__.__name__}(queue={self._queue.qsize()}, "
                f"tx_total_count={len(self._tx_messages)})")
