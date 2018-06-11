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

DAY = 86400  # POSIX day


def parse_duration(s):
    s = s.strip()
    t = None
    unit = None
    for n, u in POSTFIX.items():
        if s.endswith(n):
            try:
                t = float(s[:-len(n)])
            except ValueError:
                continue

            unit = u
            break

    assert unit is not None, \
        'Unknown duration \'%s\'; supported units are %s' % (
            s, ','.join('\'%s\'' % n for n in POSTFIX)
        )

    return t * unit


def encode_data(data):
    return b2a_base64(data).strip().decode('ascii')


def decode_data(data):
    return a2b_base64(data)
