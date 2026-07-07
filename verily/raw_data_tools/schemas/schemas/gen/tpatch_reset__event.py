# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.tpatch.reset_event with Beam."""

import dataclasses
from typing import List, Optional

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.tpatch.reset_event')
@dataclasses.dataclass
class TpatchReset_Event(DataPoint):
    """Beam RowSchema for com.verily.tpatch.reset_event."""
    assert_info_program_counter: Optional[List[int]] = None
    hardfault_info_lr: Optional[int] = None
    hardfault_info_pc: Optional[int] = None
    hardfault_info_psr: Optional[int] = None
    hardfault_info_r0: Optional[int] = None
    hardfault_info_r1: Optional[int] = None
    hardfault_info_r12: Optional[int] = None
    hardfault_info_r2: Optional[int] = None
    hardfault_info_r3: Optional[int] = None
    memmanage_info_lr: Optional[int] = None
    memmanage_info_pc: Optional[int] = None
    memmanage_info_psr: Optional[int] = None
    memmanage_info_r0: Optional[int] = None
    memmanage_info_r1: Optional[int] = None
    memmanage_info_r12: Optional[int] = None
    memmanage_info_r2: Optional[int] = None
    memmanage_info_r3: Optional[int] = None
    nrf52_cpu_reset_reason_register: Optional[int] = None
    softdevice_info_id: Optional[int] = None
    softdevice_info_pc: Optional[int] = None
    sw_reset_reason: Optional[str] = None
