

def singleton(cls):
    instances = {}

    def get_instance(*args, **kwargs):
        if cls not in instances:
            instances[cls] = cls(*args, **kwargs)
        return instances[cls]

    return get_instance


def time_to_minutes(time):
    result = 0
    try:
        components = time.split(':')
        if len(components):
            result = int(components[0]) * 60 + int(components[1])
    except ValueError as e:
        result = 0
    return result


def minutes_to_time(minutes):
    h, m = divmod(minutes, 60)
    return '{:02d}:{:02d}'.format(abs(h), abs(m))