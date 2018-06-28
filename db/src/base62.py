import string

_chars = string.digits + string.ascii_uppercase + string.ascii_lowercase
_values = ''.join([chr(i) for i in range(62)])
_c2v_trans_table = str.maketrans(_chars, _values)
_v2c_trans_table = str.maketrans(_values, _chars)
_identity_trans_table = str.maketrans(_chars, _chars)


def log_floor(n: int, b: int) -> int:
    """
    The largest integer k such that b^k <= n.
    """
    p = 1
    k = 0
    while p <= n:
        p *= b
        k += 1
    return k - 1


def encode(os: bytes) -> str:
    """
    @param os the data to be encoded (a string)

    @return the contents of os in base-62 encoded form
    """
    cs = b2a_l(os, len(os) * 8)
    if num_octets_that_encode_to_this_many_chars(len(cs)) != len(os):
        raise ValueError("{} != {}, numchars: {}".format(
            num_octets_that_encode_to_this_many_chars(len(cs)), len(os), len(cs)
        ))
    return cs


def b2a_l(os: bytes, lengthinbits: int) -> str:
    """
    @param os the data to be encoded (a string)
    @param lengthinbits the number of bits of data in os to be encoded
    b2a_l() will generate a base-62 encoded string big enough to encode
    lengthinbits bits.  So for example if os is 3 bytes long and lengthinbits is
    17, then b2a_l() will generate a 3-character- long base-62 encoded string
    (since 3 chars is sufficient to encode more than 2^17 values).  If os is 3
    bytes long and lengthinbits is 18 (or None), then b2a_l() will generate a
    4-character string (since 4 chars are required to hold 2^18 values).  Note
    that if os is 3 bytes long and lengthinbits is 17, the least significant 7
    bits of os are ignored.
    Warning: if you generate a base-62 encoded string with b2a_l(), and then someone else tries to
    decode it by calling decode() instead of  a2b_l(), then they will (potentially) get a different
    string than the one you encoded!  So use b2a_l() only when you are sure that the encoding and
    decoding sides know exactly which lengthinbits to use.  If you do not have a way for the
    encoder and the decoder to agree upon the lengthinbits, then it is best to use b2a() and
    decode().  The only drawback to using encode() over b2a_l() is that when you have a number of
    bits to encode that is not a multiple of 8, encode() can sometimes generate a base-62 encoded
    string that is one or two characters longer than necessary.
    @return the contents of os in base-62 encoded form
    """
    os = reversed(os)  # treat os as big-endian -- and we want to process the least-significant o first

    value = 0
    num_values = 1  # the number of possible values that value could be
    for o in os:
        o *= num_values
        value += o
        num_values *= 256

    chars = []
    while num_values > 0:
        chars.append(value % 62)
        value //= 62
        num_values //= 62

    return str.translate(''.join([chr(c) for c in reversed(chars)]), _v2c_trans_table)  # make it big-endian


def num_octets_that_encode_to_this_many_chars(numcs: int) -> int:
    return log_floor(62 ** numcs, 256)


# def decode(cs: str) -> bytes:
#     """
#     @param cs the base-62 encoded data (a string)
#     """
#     return a2b_l(cs, num_octets_that_encode_to_this_many_chars(len(cs)) * 8)
#
#
# def a2b_l(cs: str, lengthinbits: int) -> bytes:
#     """
#     a2b_l() will return a result just big enough to hold lengthinbits bits.  So
#     for example if cs is 2 characters long (encoding between 5 and 12 bits worth
#     of data) and lengthinbits is 8, then a2b_l() will return a string of length
#     1 (since 1 byte is sufficient to store 8 bits), but if lengthinbits is 9,
#     then a2b_l() will return a string of length 2.
#
#     Please see the warning in the docstring of b2a_l() regarding the use of
#     encode() versus b2a_l().
#
#     """
#     # treat cs as big-endian -- and we want to process the least-significant c first
#     cs = [ord(c) for c in reversed(str.translate(cs, _c2v_trans_table))]
#
#     value = 0
#     num_values = 1  # the number of possible values that value could be
#     for c in cs:
#         c *= num_values
#         value += c
#         num_values *= 62
#
#     num_values = 2 ** lengthinbits
#     bytes_ = []
#     while num_values > 1:
#         bytes_.append(value % 256)
#         value //= 256
#         num_values //= 256
#
#     return b''.join([b for b in reversed(bytes_)])  # make it big-endian