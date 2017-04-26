

class BaseException(Exception):
    pass


class DomainModelException(BaseException):
    pass


class AbsentRoutePointException(DomainModelException):
    def __init__(self, route_id, point_index):
        self.route_id = route_id
        self.point_index = point_index
        super().__init__(
            'absent route point #{} in {} route'.format(
                self.point_index,
                self.route_id
            )
        )


class AbsentPathItemException(DomainModelException):
    def __init__(self, item_index):
        self.item_index = item_index
        super().__init__(
            'absent path item #{}'.format(
                self.item_index
            )
        )


class ApplicationException(Exception):
    def __init__(self):
        super().__init__('application internal exception')