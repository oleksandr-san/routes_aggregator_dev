

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


def read_config_file(file_path):
    config = {}
    with open(file_path, 'r') as file:
        for line in file:
            if not line.startswith('#'):
                components = line.split('=', 1)
                if len(components) == 2:
                    config[components[0].strip()] = components[1].strip()
    return config


def calculate_raw_time_difference(first, second):
    first_minutes = time_to_minutes(first)
    second_minutes = time_to_minutes(second)

    if first_minutes <= second_minutes:
        return second_minutes - first_minutes
    else:
        return 1440 - first_minutes + second_minutes


def calculate_time_difference(first, second):
    return minutes_to_time(calculate_raw_time_difference(first, second))
