

class BaseException(Exception):
    pass


class DomainModelException(BaseException):
    pass


class AbsentRoutePointsException(DomainModelException):
    def __init__(self, route_id):
        self.route_id = route_id
        super().__init__('absent route points in {} route'.format(self.route_id))


class ApplicationException(Exception):
    pass