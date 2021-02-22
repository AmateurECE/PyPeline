###############################################################################
# NAME:             Stage.py
#
# AUTHOR:           Ethan D. Twardy <edtwardy@mtu.edu>
#
# DESCRIPTION:      A stage in the pipeline
#
# CREATED:          02/21/2021
#
# LAST EDITED:      02/21/2021
###

class Stage:
    def __init__(self):
        self.configuration = {}

    def configure(self, configuration):
        self.configuration = configuration

    def consume(self, consumedType, *args, **kwargs):
        # pylint: disable=no-member
        if not hasattr(self, 'Consumers'):
            raise RuntimeError('This class has no consumers')
        if consumedType not in self.Consumers:
            raise RuntimeError(f'This class does not consume {consumedType}')
        return self.Consumers[consumedType](self, *args, **kwargs)

    def provide(self, providedType, *args, **kwargs):
        # pylint: disable=no-member
        if not hasattr(self, 'Providers'):
            raise RuntimeError('This class has no providers')
        if providedType not in self.Providers:
            raise RuntimeError(f'This class does not provide {providedType}')
        return self.Providers[providedType](self, *args, **kwargs)

###############################################################################
