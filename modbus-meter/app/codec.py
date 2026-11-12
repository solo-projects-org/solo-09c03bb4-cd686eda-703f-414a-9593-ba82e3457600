"""寄存器字与无符号计数的编解码约定。

沿用 ``/read`` 的约定：双字按高字在前（big-endian / 高字在低地址）拼接。
"""

from __future__ import annotations


def decode_unsigned(words: list[int]) -> int:
    """把 16 位无符号寄存器字按高字在前拼成无符号整数。"""
    value = 0
    for word in words:
        value = (value << 16) | (word & 0xFFFF)
    return value


def decode_unsigned_pair(hi: int, lo: int) -> int:
    """两个 16 位无符号字：高字在前。"""
    return decode_unsigned([hi, lo])
