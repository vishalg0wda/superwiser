class GeneralOrcException(Exception):
    """ Exceptions thrown around the orc client
    """

    def __init__(self, message):
        self.message = message
        super(GeneralOrcException, self).__init__(message)
