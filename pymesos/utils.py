from binascii import b2a_base64, a2b_base64

POSTFIX = {
    'ns': 1e-9,
    'us': 1e-6,
    'ms': 1e-3,
    'secs': 1,
    'mins': 60,
    'hrs': 60 * 60,
    'days': 24 * 60 * 60,
    'weeks': 7 * 24 * 60 * 60
}


def parse_duration(s):
    s = s.strip()
    unit = None
    postfix = None
    for n, u in POSTFIX.items():
        if s.endswith(n):
            unit = u
            postfix = n
            break

    assert unit is not None, \
        'Unknown duration \'%s\'; supported units are %s' % (
            s, ','.join('\'%s\'' % n for n in POSTFIX)
        )

    n = float(s[:-len(postfix)])
    return n * unit


def encode_data(data):
    return b2a_base64(data).strip()


def decode_data(data):
    return a2b_base64(data)
