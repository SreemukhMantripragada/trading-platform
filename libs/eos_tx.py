"""
libs/eos_tx.py
Minimal helper for Kafka transactional producer:
- begin()
- send_offsets_and_commit(offsets, group_id)
- abort()

Offsets dict: {TopicPartition(...): last_offset_plus_one}
"""
from __future__ import annotations
from typing import Dict
from aiokafka.structs import TopicPartition

class EOSTx:
    def __init__(self, producer):
        self.p = producer
        self._ready = False

    async def init(self):
        if not self._ready:
            await self.p.init_transactions()
            self._ready = True

    async def begin(self):
        await self.p.begin_transaction()

    async def send_offsets_and_commit(self, offsets: Dict[TopicPartition, int], group_id: str):
        await self.p.send_offsets_to_transaction(offsets, group_id)
        await self.p.commit_transaction()

    async def abort(self):
        try:
            await self.p.abort_transaction()
        except Exception:
            pass
