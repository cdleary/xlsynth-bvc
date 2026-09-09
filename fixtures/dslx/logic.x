// SPDX-License-Identifier: Apache-2.0

pub fn combine(a: u8, b: u8, select: bool) -> u8 {
    let sum = a + b;
    let mask = a ^ b;
    let mixed = (sum & mask) | (a & !b);
    if select { mixed } else { sum ^ mixed }
}

pub fn predicate(a: bool, b: bool, c: bool, d: bool) -> bool {
    let left = a & b;
    let right = c | d;
    (left ^ right) & !(a ^ d)
}
